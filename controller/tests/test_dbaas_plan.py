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
    Settings,
    VendorSettings,
)
from k8s_inspector.tasks import get_task, task_hash

TASK = "pgbench_postgres_dbaas_ro_durable"
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
NETWORKS = {"eu-1": {"vpc": "a"}, "eu-2": {"vpc": "b"}}
PRICES = {"eu-1": 0.40, "eu-2": 0.30, "ap-1": 0.10}  # ap-1 is the cheapest but has no network


def make_config(*, auto=True, networks=None, publish=True, **vendor) -> Config:
    vendors = {"oci": VendorSettings(enabled=True, dbaas_auto_plan=auto, publish=publish, **vendor)}
    dbaas = DbaasSettings(vendors={"oci": DbaasVendor(networks=NETWORKS if networks is None else networks)})
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


def test_the_picker_takes_the_cheapest_dedicated_x86_client_in_the_size_band():
    servers = [
        server("arm", 0.01, arch="arm64"), server("gpu", 0.01, gpu=1), server("tiny", 0.01, vcpus=1, memory=2048),
        server("big-ram", 0.01, memory=16384), server("burst", 0.01, allocation="Burstable"),
        server("shared", 0.02, allocation="Shared"), server("b", 0.05), server("a", 0.05), server("c", 0.04),
    ]  # fmt: skip
    assert dbaas_client.rank(servers) == ["c", "a", "b"]  # dedicated only, cheapest first, ties by name


def test_the_picker_falls_back_to_shared_cpus_when_no_dedicated_one_is_in_the_band():
    servers = [server("s2", 0.02, allocation="Shared"), server("s1", 0.01, allocation="Shared"),
               server("burst", 0.001, allocation="Burstable")]  # fmt: skip
    assert dbaas_client.rank(servers) == ["s1", "s2"]


def test_the_picker_drops_price_outliers_with_the_tukey_fence():
    prices = [0.04, 0.045, 0.05, 0.05, 0.055, 0.06, 0.06, 0.065, 0.07, 0.9]  # the fence is about 0.092
    ranked = dbaas_client.rank([server(f"s{i}", p) for i, p in enumerate(prices)])
    assert "s9" not in ranked and len(ranked) == 9
    few = dbaas_client.rank([server("x", 0.04), server("y", 5.0)])  # fewer than four prices: no fence
    assert few == ["x", "y"]


def test_a_server_without_a_price_is_not_a_client():
    assert dbaas_client.rank([server("x", None)]) == []


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
    assert dbaas_plan.region_order(PRICES, NETWORKS, "eu-2") == ["eu-1", "eu-2"]
    assert dbaas_plan.region_order({"eu-1": 1.0}, {"eu-1": {}}, "eu-1") == ["eu-1"]  # nowhere else to go


# --------------------------------------------------------------------------- autoPlan


def test_a_vendor_without_the_flag_or_without_networks_is_not_planned(kube, world):
    assert plan(kube, make_config(auto=False), world) == []
    assert plan(kube, make_config(networks={}), world) == []
    assert plan(kube, make_config(publish=False), world) == []  # the result would never reach git
    assert kube.list("benchmarkrun") == []


def test_the_first_database_in_start_order_gets_a_run_in_the_cheapest_region_with_the_cheapest_client(kube, world):
    (planned,) = plan(kube, make_config(), world)
    assert planned.database_id == first_by_start_order(world.records) and planned.region == "eu-2"
    assert planned.client == "c-cheap" and planned.engine_version == "18" and planned.tasks == (TASK,)
    assert world.asked == ["id-eu-2"]
    (run,) = crd.list_runs(kube)
    assert (run.spec.vendor, run.spec.sku, run.spec.tasks, run.spec.force) == ("oci", "c-cheap", [TASK], False)
    assert run.spec.capacity == CapacityMode.ON_DEMAND  # a lost client would leave the database billing
    assert run.spec.database == {
        "vendor": "oci", "id": planned.database_id, "engineVersion": "18", "region": "eu-2", "vcpus": 4,
        "memoryGib": 32.0,
    }  # fmt: skip
    assert run.name == crd.run_name("oci", "c-cheap")
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
    (next_run,) = plan(kube, make_config(), world, dry_run=True)  # destroyed: the next one may start ...
    assert planned.client == "c-cheap" and next_run.client == "c-small"  # ... with a client that has no run


def test_dbaas_runs_do_not_count_against_the_server_budget(kube, world):
    plan(kube, make_config(), world)
    runs = crd.list_runs(kube)
    assert planner.holding_runs(kube, runs, "oci") == []
    planner.check_vendor_budget(kube, make_config(max_in_flight=1), "oci", runs)


def test_a_client_that_waits_for_a_launch_retry_is_skipped(kube, world, monkeypatch):
    monkeypatch.setattr(dbaas_plan.launch, "deferred", lambda kube, vendor, skus, now, prune: {"c-cheap": object()})
    assert plan(kube, make_config(), world, dry_run=True)[0].client == "c-small"


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


def force(kube, world, cfg=None, **kw):
    return dbaas_plan.force_run(kube, cfg or make_config(auto=False), "oci", "db-a", **kw)


def test_a_forced_run_needs_no_autoplan_and_picks_region_and_client(kube, world):
    run = force(kube, world)
    assert run.spec.database["id"] == "db-a" and run.spec.database["region"] == "eu-2" and run.spec.sku == "c-cheap"
    assert run.spec.force is True and run.spec.tasks == [TASK]


def test_a_forced_run_can_name_the_region_and_the_client(kube, world):
    run = force(kube, world, region="eu-1", client="my-client")
    assert (run.spec.database["region"], run.spec.sku) == ("eu-1", "my-client")
    assert world.asked == []  # nothing to pick


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
    with pytest.raises(ValueError, match="already has a DBaaS run in flight"):
        dbaas_plan.force_run(kube, make_config(), "oci", "db-b")


def test_a_forced_run_of_a_disabled_vendor_is_refused(kube, world):
    cfg = Config(make_config().env, Settings(vendors={"oci": VendorSettings(enabled=False)}))
    with pytest.raises(ValueError, match="not enabled"):
        dbaas_plan.force_run(kube, cfg, "oci", "db-a")
