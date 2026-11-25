"""DBaaS planning: the client picker, region choice, what is due and the forced path. Keeper is faked."""

from datetime import UTC, datetime, timedelta

import pytest
from fakes import Clock, FakeKube

from k8s_inspector import crd, dbaas_client, dbaas_keeper, dbaas_plan, planner
from k8s_inspector.model import (
    CapacityMode,
    Config,
    DbaasSettings,
    DbaasVendor,
    DbPhase,
    DbStatus,
    EnvConfig,
    RunPhase,
    RunSpec,
    Settings,
    VendorSettings,
)
from k8s_inspector.tasks import get_task, task_hash

TASK = "pgbench_postgres_dbaas_ro_durable"
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
NETWORKS = {"eu-1": {"vpc": "a"}, "eu-2": {"vpc": "b"}}
PRICES = {"eu-1": 0.40, "eu-2": 0.30, "ap-1": 0.10}  # ap-1 is the cheapest but has no network


@pytest.fixture(autouse=True)
def everything_offered(monkeypatch):
    """SCarpenter offers every SKU (the offered-types listing is tested in test_planner.py)."""
    monkeypatch.setattr(planner, "not_offered", lambda kube, cfg, vendor, skus, now=None: {})


def make_config(*, auto=True, networks=None, by_city=False, publish=True, **vendor) -> Config:
    vendors = {"oci": VendorSettings(enabled=True, dbaas_auto_plan=auto, publish=publish, **vendor)}
    dbaas = DbaasSettings(
        vendors={"oci": DbaasVendor(networks=NETWORKS if networks is None else networks, client_region_by_city=by_city)}
    )
    return Config(
        EnvConfig(bucket="b", bucket_region="r", dbaas_image="img@sha256:1"), Settings(vendors=vendors, dbaas=dbaas)
    )


def server(name, price, *, vcpus=2, memory=4096, allocation="Dedicated", arch="x86_64", gpu=0):
    return {
        "api_reference": name, "vcpus": vcpus, "memory_amount": memory, "cpu_architecture": arch,
        "cpu_allocation": allocation, "accelerator_count": gpu, "min_price_ondemand": price,
    }  # fmt: skip


def record(name, versions=("16", "17", "18"), vcpus=4, memory=32768):
    return {"api_reference": name, "engine": "postgresql", "engine_versions": list(versions), "vcpus": vcpus,
            "memory_amount": memory}  # fmt: skip


class World:
    """Keeper's answers and the git metas."""

    def __init__(self) -> None:
        self.records = {n: record(n) for n in ("db-a", "db-b", "db-c")}
        self.blocked: dict[str, dict] = {}  # database -> {task: [reasons]}
        self.prices = dict(PRICES)
        self.servers = [server("c-small", 0.05), server("c-cheap", 0.03), server("c-pricey", 0.09)]
        self.metas: dict[str, dict] = {}  # database -> {version: {task: meta}}
        self.keeper_down = False
        self.asked: list = []


@pytest.fixture
def world(monkeypatch) -> World:
    w = World()

    def bulk(env, task_names):
        if w.keeper_down:
            raise RuntimeError("keeper down")
        return {"oci": {name: w.blocked.get(name, {}) for name in w.records}}

    monkeypatch.setattr(dbaas_keeper, "block_reasons", bulk)
    monkeypatch.setattr(dbaas_keeper, "databases", lambda env, vendor: list(w.records.values()))
    monkeypatch.setattr(dbaas_keeper, "database", lambda env, vendor, name: w.records.get(name))
    monkeypatch.setattr(
        dbaas_keeper, "database_block_reasons", lambda env, vendor, name, tasks: w.blocked.get(name, {})
    )
    monkeypatch.setattr(dbaas_keeper, "standalone_prices", lambda env, vendor, name: dict(w.prices))
    monkeypatch.setattr(dbaas_keeper, "region_ids", lambda env, vendor: {r: f"id-{r}" for r in (*PRICES, "eu-3")})

    def client_servers(env, vendor, region_id):
        w.asked.append(region_id)
        return list(w.servers)

    monkeypatch.setattr(dbaas_keeper, "client_servers", client_servers)
    return w


@pytest.fixture
def kube() -> FakeKube:
    return FakeKube(Clock(NOW))


def reader(world):
    return lambda vendor: world.metas


def good_meta(**fields) -> dict:
    return {"task_hash": task_hash(get_task(TASK)), "exit_code": 0, "end": "2026-11-01T00:00:00", **fields}


def plan(kube, cfg, world, **kw):
    return dbaas_plan.plan_cycle(kube, cfg, reader(world), now=NOW, **kw)


def first_by_start_order(ids):
    return sorted(ids, key=lambda d: planner.start_order("oci", d))[0]


# --------------------------------------------------------------------------- the client picker


def test_the_client_requirements_are_scarpenter_labels_of_a_small_x86_type():
    assert dbaas_client.requirements() == [
        {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]},
        {"key": "scarpenter.sparecores.com/vcpus", "operator": "Gt", "values": ["1"]},
        {"key": "scarpenter.sparecores.com/vcpus", "operator": "Lt", "values": ["5"]},
        {"key": "scarpenter.sparecores.com/memory-mib", "operator": "Gt", "values": ["4095"]},
        {"key": "scarpenter.sparecores.com/memory-mib", "operator": "Lt", "values": ["8193"]},
        {"key": "scarpenter.sparecores.com/accelerator-count", "operator": "DoesNotExist"},
        {"key": "scarpenter.sparecores.com/cpu-allocation", "operator": "In", "values": ["Dedicated", "Shared"]},
    ]
    assert dbaas_client.requirements("c1") == [  # an explicit client replaces the band
        {"key": "scarpenter.sparecores.com/api-reference", "operator": "In", "values": ["c1"]}
    ]


def test_the_price_bound_is_the_dearest_server_the_requirements_accept():
    servers = [
        server("arm", 9.0, arch="arm64"), server("gpu", 9.0, gpu=1), server("tiny", 9.0, vcpus=1, memory=2048),
        server("big-ram", 9.0, memory=16384), server("burst", 9.0, allocation="Burstable"), server("none", None),
        server("shared", 0.02, allocation="Shared"), server("b", 0.05), server("edge", 0.07, vcpus=4, memory=8192),
    ]  # fmt: skip
    assert dbaas_client.most_expensive(servers) == 0.07
    assert dbaas_client.most_expensive([server("x", None)]) is None


def test_the_engine_version_is_the_preferred_one_when_offered_else_the_newest():
    assert dbaas_keeper.engine_version(record("d"), "17") == "17"
    assert dbaas_keeper.engine_version(record("d", ("9.6", "16", "17")), "18") == "17"
    assert dbaas_keeper.engine_version(record("d", ("9.6", "10", "9.10")), "18") == "10"
    assert dbaas_keeper.engine_version(record("d", ()), "18") is None


# --------------------------------------------------------------------------- regions


def test_regions_are_the_cheapest_first_among_those_with_a_network():
    assert dbaas_plan.region_order(PRICES, NETWORKS) == ["eu-2", "eu-1"]
    assert dbaas_plan.region_order(PRICES, {}) == []


def test_the_region_of_the_last_failed_attempt_goes_last():
    assert dbaas_plan.region_order(PRICES, NETWORKS, {"eu-2"}) == ["eu-1", "eu-2"]
    assert dbaas_plan.region_order({"eu-1": 1.0}, {"eu-1": {}}, {"eu-1"}) == ["eu-1"]  # nowhere else to go


# --------------------------------------------------------------------------- autoPlan


def test_a_vendor_without_the_flag_or_without_networks_is_not_planned(kube, world):
    assert plan(kube, make_config(auto=False), world) == []
    assert plan(kube, make_config(networks={}), world) == []
    assert plan(kube, make_config(publish=False), world) == []  # the result would never reach git
    assert kube.list("benchmarkrun") == []


def test_the_first_database_in_start_order_gets_a_run_in_the_cheapest_region_with_the_cheapest_client(kube, world):
    (planned,) = plan(kube, make_config(), world)
    assert planned.database_id == first_by_start_order(world.records) and planned.region == "eu-2"
    assert (
        planned.client is None  # any small x86_64 type: the cluster launches the cheapest one
        and planned.engine_version == "18"
        and planned.tasks == (TASK,)
    )
    assert world.asked == ["id-eu-2"]
    (run,) = crd.list_runs(kube)
    assert (run.spec.vendor, run.spec.sku, run.spec.tasks, run.spec.force) == (
        "oci",
        planned.database_id,
        [TASK],
        False,
    )
    assert run.spec.capacity == CapacityMode.ON_DEMAND  # a lost client would leave the database billing
    price = run.spec.database.pop("usdPerHour")  # the database in eu-2 and the dearest client, from Keeper
    assert price == pytest.approx(0.30 + 0.09)
    assert run.spec.database == {
        "vendor": "oci", "id": planned.database_id, "engineVersion": "18", "region": "eu-2", "vcpus": 4,
        "memoryGib": 32.0,
    }  # fmt: skip
    assert run.name == crd.run_name("oci", planned.database_id)  # not the client's: one run per database
    assert kube.get("benchmarkrun", run.name)["metadata"]["finalizers"] == ["inspector.sparecores.com/dbaas"]


def test_a_database_with_a_result_of_the_current_hash_is_not_due(kube, world):
    first = first_by_start_order(world.records)
    world.metas = {first: {"18": {TASK: good_meta()}}}
    (planned,) = plan(kube, make_config(), world)
    assert planned.database_id != first
    world.metas = {d: {"18": {TASK: good_meta()}} for d in world.records}
    assert plan(kube, make_config(), world, dry_run=True) == []


def test_a_changed_task_hash_makes_the_database_due_again(kube, world):
    world.metas = {d: {"18": {TASK: good_meta(task_hash="old")}} for d in world.records}
    assert [p.reason for p in plan(kube, make_config(), world, dry_run=True)] == [f"{TASK}: task hash changed"]


def test_failures_are_retried_with_the_usual_bounds(kube, world):
    only = {"db-a": world.records["db-a"]}
    world.records = only
    recent = (NOW - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    old = (NOW - timedelta(hours=3)).replace(tzinfo=None).isoformat()
    world.metas = {"db-a": {"18": {TASK: good_meta(exit_code=-1, end=recent)}}}
    assert plan(kube, make_config(), world, dry_run=True) == []  # -1: after 2 hours
    world.metas["db-a"]["18"][TASK] = good_meta(exit_code=-1, end=old)
    assert len(plan(kube, make_config(), world, dry_run=True)) == 1
    world.metas["db-a"]["18"][TASK] = good_meta(exit_code=-5, end=old)
    assert plan(kube, make_config(), world, dry_run=True) == []  # three failures: never again
    world.metas["db-a"]["18"][TASK] = None  # a damaged meta.json
    assert plan(kube, make_config(), world, dry_run=True) == []


def test_the_retry_goes_to_another_region_than_the_failed_one(kube, world):
    world.records = {"db-a": world.records["db-a"]}
    old = (NOW - timedelta(hours=3)).replace(tzinfo=None).isoformat()
    world.metas = {"db-a": {"18": {TASK: good_meta(exit_code=-1, end=old, region="eu-2")}}}
    assert plan(kube, make_config(), world, dry_run=True)[0].region == "eu-1"


def test_a_database_keeper_blocks_is_not_planned(kube, world):
    first = first_by_start_order(world.records)
    world.blocked = {first: {TASK: ["Cloud credit/budget exhausted."]}}
    (planned,) = plan(kube, make_config(), world, dry_run=True)
    assert planned.database_id != first


def test_one_dbaas_run_is_in_flight_per_vendor(kube, world):
    (planned,) = plan(kube, make_config(), world)
    assert plan(kube, make_config(), world) == []  # the first is Pending
    run = crd.list_runs(kube)[0]
    for phase in (RunPhase.PUBLISHING, RunPhase.FAILED):  # the node is gone but the database may not be
        crd.update_status(
            kube,
            run.name,
            lambda s, phase=phase: (
                setattr(s, "phase", phase),
                setattr(s, "database", DbStatus(DbPhase.READY, NOW, NOW)),
            ),
        )
        assert plan(kube, make_config(), world, dry_run=True) == [], phase
    crd.update_status(kube, run.name, lambda s: setattr(s.database, "phase", DbPhase.DESTROYED))
    (next_run,) = plan(kube, make_config(), world, dry_run=True)  # destroyed: the next one may start
    assert next_run.database_id != planned.database_id  # the first database has a run


def test_dbaas_runs_do_not_count_against_the_server_budget(kube, world):
    plan(kube, make_config(), world)
    runs = crd.list_runs(kube)
    assert planner.holding_runs(kube, runs, "oci") == []
    planner.check_vendor_budget(kube, make_config(max_in_flight=1), "oci", runs)


def test_a_database_with_a_run_is_not_planned_again_whatever_the_run_is_named(kube, world):
    old = RunSpec("oci", "c-cheap", "r0", database={"vendor": "oci", "id": "db-a", "engineVersion": "18"})
    crd.create_run(kube, old)  # made before runs were named after their database: named after its client
    assert "db-a" not in [p.database_id for p in plan(kube, make_config(dbaas_max_in_flight=5), world, dry_run=True)]
    with pytest.raises(crd.RunExists, match=crd.run_name("oci", "c-cheap")):
        force(kube, world, make_config(auto=False, dbaas_max_in_flight=5))


def test_a_database_without_a_usable_region_or_client_is_skipped(kube, world):
    world.servers = []
    assert plan(kube, make_config(), world, dry_run=True) == []
    world.servers = [server("c", 0.05)]
    world.prices = {"ap-1": 0.1}  # no network there
    assert plan(kube, make_config(), world, dry_run=True) == []


def test_a_keeper_that_cannot_be_reached_plans_nothing(kube, world):
    world.keeper_down = True
    assert plan(kube, make_config(), world) == []
    assert kube.list("benchmarkrun") == []


def test_dry_run_creates_nothing(kube, world):
    assert len(plan(kube, make_config(), world, dry_run=True)) == 1
    assert kube.list("benchmarkrun") == []


# --------------------------------------------------------------------------- the forced path


def force(kube, world, cfg=None, database="db-a", **kw):
    return dbaas_plan.force_run(kube, cfg or make_config(auto=False), "oci", database, **kw)


def test_a_forced_run_needs_no_autoplan_and_picks_region_and_client(kube, world):
    run = force(kube, world)
    assert run.spec.database["id"] == "db-a" and run.spec.database["region"] == "eu-2" and run.spec.sku == "db-a"
    assert "client" not in run.spec.database
    assert run.spec.force is True and run.spec.tasks == [TASK]


def test_a_forced_run_can_name_the_region_and_the_client(kube, world):
    run = force(kube, world, region="eu-1", client="my-client")
    assert (run.spec.database["region"], run.spec.database["client"]) == ("eu-1", "my-client")
    assert world.asked == ["id-eu-1"]
    assert "usdPerHour" not in run.spec.database  # Keeper does not list that client: its price is unknown


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [({"region": "ap-1"}, "no DBaaS network of oci in ap-1"), ({"force": False}, "Keeper blocks tasks")],
)
def test_a_forced_run_is_refused_when_it_cannot_work(kube, world, kwargs, message):
    world.blocked = {"db-a": {TASK: ["Cloud credit/budget exhausted."]}}
    with pytest.raises(ValueError, match=message):
        force(kube, world, **kwargs)


def test_force_skips_keepers_eligibility_but_not_its_record(kube, world):
    with pytest.raises(ValueError, match="does not know the database"):
        dbaas_plan.force_run(kube, make_config(), "oci", "nope")
    world.blocked = {"db-a": {TASK: ["blocked"]}}
    assert force(kube, world).spec.force is True


def test_a_forced_run_is_refused_without_networks_or_while_another_runs(kube, world):
    with pytest.raises(ValueError, match="no dbaas.vendors.oci.networks"):
        force(kube, world, make_config(networks={}))
    force(kube, world)
    with pytest.raises(ValueError, match="dbaasMaxInFlight limit"):
        dbaas_plan.force_run(kube, make_config(), "oci", "db-b")


def test_a_forced_run_of_a_disabled_vendor_is_refused(kube, world):
    cfg = Config(make_config().env, Settings(vendors={"oci": VendorSettings(enabled=False)}))
    with pytest.raises(ValueError, match="not enabled"):
        dbaas_plan.force_run(kube, cfg, "oci", "db-a")


# --------------------------------------------------------------------------- the DBaaS limits


def test_dbaas_max_in_flight_lets_more_runs_start_one_per_cycle(kube, world):
    cfg = make_config(dbaas_max_in_flight=2)
    assert len(plan(kube, cfg, world)) == 1
    assert len(plan(kube, cfg, world)) == 1  # the next cycle: the limit is not reached yet
    assert plan(kube, cfg, world) == []  # two DBaaS runs in flight
    assert len(crd.list_runs(kube)) == 2


def test_dbaas_max_in_flight_zero_pauses_the_vendor(kube, world):
    assert plan(kube, make_config(dbaas_max_in_flight=0), world) == []
    with pytest.raises(ValueError, match=r"dbaasMaxInFlight limit \(0 of 0"):
        force(kube, world, make_config(dbaas_max_in_flight=0))


def test_server_runs_do_not_use_up_the_dbaas_limits(kube, world):
    crd.create_run(kube, RunSpec("oci", "some-server", "r1", tasks=["x"]))
    cfg = make_config(max_in_flight=1, max_usd_per_hour=0.01, dbaas_max_usd_per_hour=1.0)
    assert len(plan(kube, cfg, world, dry_run=True)) == 1


def test_the_dbaas_cap_counts_database_and_client_price_of_the_runs_in_flight(kube, world):
    cfg = make_config(dbaas_max_in_flight=5, dbaas_max_usd_per_hour=0.50)
    (first,) = plan(kube, cfg, world)  # eu-2 database 0.30 + the dearest client 0.09
    assert first.region == "eu-2"
    assert plan(kube, cfg, world) == []  # 0.39 + 0.39 > 0.50, and eu-1 costs more
    name = crd.list_runs(kube)[0].name
    crd.update_status(  # the node is released, the database not yet destroyed
        kube,
        name,
        lambda s: (
            setattr(s, "phase", RunPhase.PUBLISHING),
            setattr(s, "database", DbStatus(DbPhase.DESTROYING, NOW, NOW)),
        ),
    )
    assert plan(kube, cfg, world) == []
    crd.update_status(kube, name, lambda s: setattr(s.database, "phase", DbPhase.DESTROYED))
    assert len(plan(kube, cfg, world)) == 1  # destroyed: the price is free again


def test_a_run_that_alone_passes_the_cap_is_never_planned_and_a_cheaper_database_is_tried(kube, world, monkeypatch):
    world.records = {"db-a": record("db-a"), "db-b": record("db-b")}
    prices = {"db-a": {"eu-2": 5.0}, "db-b": {"eu-2": 0.10}}
    monkeypatch.setattr(dbaas_keeper, "standalone_prices", lambda env, vendor, name: dict(prices[name]))
    (planned,) = plan(kube, make_config(dbaas_max_usd_per_hour=1.0), world)
    assert planned.database_id == "db-b"
    with pytest.raises(ValueError, match="would pass its dbaasMaxUsdPerHour cap"):
        dbaas_plan.force_run(
            kube, make_config(auto=False, dbaas_max_in_flight=3, dbaas_max_usd_per_hour=1.0), "oci", "db-a"
        )


def test_an_unknown_price_is_refused_when_a_cap_is_set(kube, world):
    cfg = make_config(auto=False, dbaas_max_usd_per_hour=1.0)
    with pytest.raises(ValueError, match="price of the database or client is unknown"):
        force(kube, world, cfg, region="eu-1", client="my-client")  # Keeper lists no such client
    assert force(kube, world, make_config(auto=False), region="eu-1", client="my-client")  # no cap: fine


# --------------------------------------------------------------------------- database and client regions differ (OVH)

CITIES = {
    "db-a": ("AA", "Alpha"),
    "db-b": ("BB", "Beta"),
    "srv-a": ("AA", "Alpha"),
    "srv-b": ("BB", "Beta"),
    "srv-a2": ("AA", "Alpha"),
    "srv-x": ("XX", "Alpha"),  # a city of the same name in another country
}
SPLIT_NETWORKS = {"srv-a": {"net": "a"}, "srv-b": {"net": "b"}}  # keyed by the client's region


def split_config(networks, by_city=True) -> Config:
    return make_config(auto=False, networks=networks, by_city=by_city, dbaas_max_in_flight=9)


@pytest.fixture
def split(world, monkeypatch):
    """Databases are priced in db-a and db-b, servers exist in srv-a, srv-a2 and srv-b (same cities)."""
    world.prices = {"db-a": 0.30, "db-b": 0.20}
    monkeypatch.setattr(dbaas_keeper, "region_cities", lambda env, vendor: dict(CITIES))
    monkeypatch.setattr(dbaas_keeper, "region_ids", lambda env, vendor: {r: f"id-{r}" for r in CITIES})
    return world


def test_the_client_goes_to_a_region_of_the_databases_city(kube, split):
    run = force(kube, split, split_config(SPLIT_NETWORKS))
    assert (run.spec.database["region"], run.spec.database["clientRegion"]) == ("db-b", "srv-b")  # cheapest database
    assert split.asked == ["id-srv-b"]  # the servers are those of the client's region, not the database's


def test_a_forced_database_region_finds_its_client_region_by_city(kube, split):
    run = force(kube, split, split_config(SPLIT_NETWORKS), region="db-a")
    assert (run.spec.database["region"], run.spec.database["clientRegion"]) == ("db-a", "srv-a")
    assert run.spec.database["usdPerHour"] == pytest.approx(
        0.30 + 0.09
    )  # database price of db-a plus the dearest client's


def test_the_explicit_client_region_overrides_the_city_but_must_have_a_network(kube, split):
    cfg = split_config(SPLIT_NETWORKS)
    run = force(kube, split, cfg, region="db-a", client_region="srv-b")
    assert (run.spec.database["region"], run.spec.database["clientRegion"]) == ("db-a", "srv-b")
    run = force(
        kube, split, cfg, "db-b", client_region="srv-a"
    )  # no --region: no city check either, the cheapest database
    assert (run.spec.database["region"], run.spec.database["clientRegion"]) == ("db-b", "srv-a")
    with pytest.raises(ValueError, match="no DBaaS network of oci in srv-a2"):
        force(kube, split, cfg, "db-c", client_region="srv-a2")


def test_a_database_region_without_a_network_in_its_city_is_not_used(kube, split):
    cfg = split_config({"srv-a": {}})
    assert force(kube, split, cfg).spec.database["region"] == "db-a"  # db-b is cheaper but its city has no network
    with pytest.raises(ValueError, match="no DBaaS network of oci in db-b or in a region of its city"):
        force(kube, split, cfg, "db-b", region="db-b")


def test_a_region_that_is_its_own_client_region_is_tried_first_and_leaves_no_client_region(kube, split):
    run = force(kube, split, split_config({"db-a": {}, "srv-a": {}}), region="db-a")
    assert run.spec.database["region"] == "db-a" and "clientRegion" not in run.spec.database


def test_a_database_region_with_a_network_but_no_client_does_not_spill_into_its_city(kube, split):
    split.servers = []
    with pytest.raises(ValueError, match="no client server"):
        force(kube, split, split_config({"db-a": {}, "srv-a": {}}), region="db-a")
    assert split.asked == ["id-db-a"]  # srv-a, of the same city, was not asked


def test_the_city_is_matched_with_its_country():
    networks = {"srv-a": {}, "srv-x": {}}
    by_city = {"by_city": True}
    assert dbaas_plan.client_regions("db-a", networks, CITIES, **by_city) == ["srv-a"]  # srv-x: another country
    assert dbaas_plan.client_regions("db-a", networks, {**CITIES, "db-a": ("XX", "Alpha")}, **by_city) == ["srv-x"]


def test_without_client_region_by_city_a_region_without_a_network_has_no_client_region(kube, split):
    assert dbaas_plan.client_regions("db-a", SPLIT_NETWORKS, CITIES) == []
    assert dbaas_plan.client_regions("srv-a", SPLIT_NETWORKS, CITIES) == ["srv-a"]
    cfg = split_config(SPLIT_NETWORKS, by_city=False)
    with pytest.raises(ValueError, match="no DBaaS network of oci in db-a in the ConfigMap"):
        force(kube, split, cfg, region="db-a")
    with pytest.raises(ValueError, match="no standalone price in a region that has a DBaaS network"):
        force(kube, split, cfg)


def test_autoplan_retries_in_another_database_region_after_a_failure_in_the_client_region(kube, split):
    split.records = {"db-a": split.records["db-a"]}
    split.metas = {"db-a": {"18": {TASK: good_meta(exit_code=-4, region="srv-b", end="2020-01-01T00:00:00")}}}
    (planned,) = plan(kube, make_config(networks=SPLIT_NETWORKS, by_city=True), split, dry_run=True)
    assert (planned.region, planned.client_region) == ("db-a", "srv-a")  # db-b (cheaper) failed in srv-b: last
