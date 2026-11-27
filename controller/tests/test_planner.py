import base64
import copy
import gzip
import json
import threading
import zlib
from datetime import UTC, datetime, timedelta, timezone

import pytest

from k8s_inspector import crd, keeper, launch, planner, pods, render
from k8s_inspector.crd import RunExists, from_dict
from k8s_inspector.kube import AlreadyExists, NotFound
from k8s_inspector.model import (
    UNLIMITED,
    CapacityMode,
    Config,
    Defaults,
    EnvConfig,
    RunKey,
    RunPhase,
    RunSpec,
    Settings,
    SpotSettings,
    VendorSettings,
)
from k8s_inspector.tasks import all_tasks, get_task, task_hash


class FakeKube:
    """In-memory BenchmarkRun store: just what `crd.create_run`/`list_runs` use."""

    namespace = "inspector"

    def __init__(self) -> None:
        self.objects: dict[str, dict] = {}
        self.pods: list[dict] = []
        self.nodeclaims: list[dict] = []
        self.foreign_configmaps: dict[tuple[str, str], dict] = {}  # (namespace, name) -> ConfigMap
        self.configmaps: dict[str, dict] = {}  # own namespace

    def get_configmap_in(self, namespace, name):
        if (namespace, name) not in self.foreign_configmaps:
            raise NotFound(name)
        return copy.deepcopy(self.foreign_configmaps[namespace, name])

    def create(self, kind, manifest):
        name = manifest["metadata"]["name"]
        if kind == "configmap":
            self.configmaps[name] = copy.deepcopy(manifest) | {"metadata": {"name": name, "resourceVersion": "1"}}
            return copy.deepcopy(self.configmaps[name])
        if name in self.objects:
            raise AlreadyExists(name)
        obj = copy.deepcopy(manifest)
        obj["metadata"].update(uid=f"uid-{name}", resourceVersion="1", creationTimestamp="2026-11-20T09:30:00Z")
        self.objects[name] = obj
        return copy.deepcopy(obj)

    def get(self, kind, name):
        if kind == "configmap":
            if name not in self.configmaps:
                raise NotFound(name)
            return copy.deepcopy(self.configmaps[name])
        if name not in self.objects:
            raise NotFound(name)
        return copy.deepcopy(self.objects[name])

    def list(self, kind, labels=None):
        if kind == "nodeclaim":
            return [c for c in self.nodeclaims if labels.items() <= c["metadata"]["labels"].items()]
        if kind == "pod":
            return [p for p in self.pods if labels.items() <= p["metadata"]["labels"].items()]
        return [copy.deepcopy(o) for o in self.objects.values()]

    def merge_patch(self, kind, name, body, subresource_status=False):
        if kind == "configmap":
            self.configmaps[name]["data"].update(body["data"])
            return copy.deepcopy(self.configmaps[name])
        obj = self.objects[name]
        obj.setdefault("status", {}).update({k: v for k, v in body["status"].items() if v is not None})
        return copy.deepcopy(obj)

    def set_status(self, name, **status):
        self.objects[name].setdefault("status", {}).update(status)


def make_config(**vendor) -> Config:
    vendors = {"oci": VendorSettings(enabled=True, **vendor), "off": VendorSettings(enabled=False)}
    return Config(EnvConfig(bucket="b", bucket_region="r"), Settings(vendors=vendors))


@pytest.fixture
def kube():
    return FakeKube()


def test_force_run_defaults_to_all_server_tasks(kube):
    run = planner.force_run(kube, make_config(), "oci", "VM.Standard.E5.Flex.2-16")

    assert run.status.phase == RunPhase.PENDING
    assert run.spec.force is True
    assert run.spec.capacity == CapacityMode.SPOT_FIRST
    assert run.spec.hold_after == timedelta(0)
    assert run.spec.tasks == [t.name for t in all_tasks() if not t.dbaas and not t.keeper_gated]
    assert "stressnglongrun" not in run.spec.tasks
    assert run.spec.run_id.startswith("r")
    assert run.name in kube.objects


def test_force_run_runs_a_keeper_gated_task_when_named(kube):
    run = planner.force_run(kube, make_config(), "oci", "sku", tasks=["lscpu", "stressnglongrun"])
    assert run.spec.tasks == ["lscpu", "stressnglongrun"]


def test_run_without_force_leaves_keeper_gated_tasks_out_of_the_default(kube, monkeypatch):
    monkeypatch.setattr(keeper, "block_reasons", lambda *a: {"sku": {}})
    run = planner.force_run(kube, make_config(), "oci", "sku", force=False)
    assert run.spec.tasks == [t.name for t in all_tasks() if not t.dbaas and not t.keeper_gated]


def test_run_without_force_asks_keeper_about_a_named_keeper_gated_task(kube, monkeypatch):
    asked = []
    monkeypatch.setattr(keeper, "block_reasons", lambda cfg, vendor, sku, names: asked.append(names) or {"sku": {}})
    planner.force_run(kube, make_config(), "oci", "sku", tasks=["stressnglongrun"], force=False)
    assert asked == [["stressnglongrun"]]


def test_force_run_options(kube):
    run = planner.force_run(
        kube,
        make_config(capacity=CapacityMode.SPOT_FIRST),
        "oci",
        "sku",
        tasks=["lscpu"],
        capacity=CapacityMode.ON_DEMAND,
        hold_after=timedelta(hours=2),
    )
    assert run.spec.tasks == ["lscpu"]
    assert run.spec.capacity == CapacityMode.ON_DEMAND
    assert run.spec.hold_after == timedelta(hours=2)


def test_force_run_uses_vendor_capacity_setting(kube):
    run = planner.force_run(kube, make_config(capacity=CapacityMode.ON_DEMAND), "oci", "sku")
    assert run.spec.capacity == CapacityMode.ON_DEMAND


@pytest.mark.parametrize(
    ("vendor", "sku", "tasks", "message"),
    [
        ("off", "sku", None, "not enabled"),
        ("unknown", "sku", None, "not enabled"),
        ("oci", " ", None, "SKU"),
        ("oci", "sku", ["nope"], "unknown tasks"),
    ],
)
def test_force_run_validation(kube, vendor, sku, tasks, message):
    with pytest.raises(ValueError, match=message):
        planner.force_run(kube, make_config(), vendor, sku, tasks=tasks)
    assert kube.objects == {}


def test_force_run_refuses_dbaas_tasks(kube):
    dbaas = next((t.name for t in all_tasks() if t.dbaas), None)
    if dbaas is None:
        pytest.skip("catalogue has no DBaaS task")
    with pytest.raises(ValueError, match="DBaaS"):
        planner.force_run(kube, make_config(), "oci", "sku", tasks=[dbaas])


def test_force_run_existing_run_raises_run_exists(kube):
    planner.force_run(kube, make_config(), "oci", "sku")
    with pytest.raises(RunExists):
        planner.force_run(kube, make_config(), "oci", "sku")


@pytest.fixture
def keeper_allows(monkeypatch):
    monkeypatch.setattr(keeper, "block_reasons", lambda env, vendor, skus, names: {sku: {} for sku in skus})


@pytest.mark.usefixtures("keeper_allows")
def test_max_in_flight_counts_active_runs_only(kube):
    cfg = make_config(max_in_flight=2)
    first = planner.force_run(kube, cfg, "oci", "a", force=False)
    second = planner.force_run(kube, cfg, "oci", "b", force=False)
    with pytest.raises(ValueError, match="maxInFlight"):
        planner.force_run(kube, cfg, "oci", "c", force=False)

    kube.set_status(first.name, phase="Done")
    kube.set_status(second.name, phase="Cancelled")
    planner.force_run(kube, cfg, "oci", "c", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_max_in_flight_counts_failed_and_cancelled_runs_until_anchor_is_gone(kube):
    cfg = make_config(max_in_flight=2)
    first = planner.force_run(kube, cfg, "oci", "a", force=False)
    second = planner.force_run(kube, cfg, "oci", "b", force=False)
    kube.set_status(first.name, phase="Failed")
    kube.set_status(second.name, phase="Cancelled")
    kube.pods = [
        {"metadata": {"labels": {"inspector.sparecores.com/run": name, "inspector.sparecores.com/role": "anchor"}}}
        for name in (first.name, second.name)
    ]
    with pytest.raises(ValueError, match="2 of 2"):
        planner.force_run(kube, cfg, "oci", "c", force=False)

    kube.pods = kube.pods[:1]  # the runner deleted the second anchor
    planner.force_run(kube, cfg, "oci", "c", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_max_in_flight_zero_pauses_vendor(kube):
    with pytest.raises(ValueError, match="maxInFlight"):
        planner.force_run(kube, make_config(max_in_flight=0), "oci", "sku", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_max_in_flight_falls_back_to_defaults(kube):
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(vendors={"oci": VendorSettings(enabled=True)}, defaults=Defaults(max_in_flight=1)),
    )
    planner.force_run(kube, cfg, "oci", "a", force=False)
    with pytest.raises(ValueError, match="1 of 1"):
        planner.force_run(kube, cfg, "oci", "b", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_max_usd_per_hour(kube):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a", force=False)
    kube.set_status(first.name, phase="Running", pricePerHour="0.6")
    planner.force_run(kube, cfg, "oci", "b", force=False)  # 0.6 < 1.0

    kube.set_status(first.name, pricePerHour="1.2")
    with pytest.raises(ValueError, match="maxUsdPerHour"):
        planner.force_run(kube, cfg, "oci", "c", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_max_usd_per_hour_waits_for_runs_without_price(kube):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a", force=False)
    with pytest.raises(ValueError, match=f"wait until {first.name} has a price"):
        planner.force_run(kube, cfg, "oci", "b", force=False)

    no_cap = make_config(max_in_flight=5)
    planner.force_run(kube, no_cap, "oci", "b", force=False)  # no cap: prices do not matter


@pytest.mark.usefixtures("keeper_allows")
def test_max_usd_per_hour_refuses_a_running_run_without_price(kube, caplog):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a", force=False)
    kube.set_status(first.name, phase="Running", node="n1")
    with caplog.at_level("WARNING"), pytest.raises(ValueError, match=rf"node of {first.name} has no price annotation"):
        planner.force_run(kube, cfg, "oci", "b", force=False)
    assert [r.message for r in caplog.records if first.name in r.message]


@pytest.mark.usefixtures("keeper_allows")
def test_max_in_flight_counts_live_nodeclaims_beyond_runs(kube):
    cfg = make_config(max_in_flight=2)
    claim = lambda pool, vendor: {  # noqa: E731
        "metadata": {"labels": {"karpenter.sh/nodepool": pool, "scarpenter.sparecores.com/vendor": vendor}}
    }
    # Two OCI claims in the shared bench pool count; another vendor's claim and another pool's claim do not.
    kube.nodeclaims = [claim("bench", "oci"), claim("bench", "oci"), claim("bench", "aws"), claim("other-pool", "oci")]
    with pytest.raises(ValueError, match="2 of 2"):  # no run holds an anchor, two instances are still up
        planner.force_run(kube, cfg, "oci", "a", force=False)

    kube.nodeclaims = kube.nodeclaims[1:]
    planner.force_run(kube, cfg, "oci", "a", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_unlaunched_nodeclaims_count_by_spec_requirements(kube):
    cfg = make_config(max_in_flight=1)
    pool = {"karpenter.sh/nodepool": "bench"}
    requirements = [{"key": "scarpenter.sparecores.com/vendor", "operator": "In", "values": ["oci"]}]
    kube.nodeclaims = [{"metadata": {"labels": pool}, "spec": {"requirements": requirements}}]
    with pytest.raises(ValueError, match="1 of 1"):
        planner.force_run(kube, cfg, "oci", "a", force=False)
    kube.nodeclaims = [
        {"metadata": {"labels": pool}, "spec": {"requirements": [{**requirements[0], "values": ["aws"]}]}}
    ]
    planner.force_run(kube, cfg, "oci", "a", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_other_vendors_do_not_count(kube):
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(
            vendors={"oci": VendorSettings(enabled=True, max_in_flight=1), "aws": VendorSettings(enabled=True)},
        ),
    )
    planner.force_run(kube, cfg, "aws", "a", force=False)
    planner.force_run(kube, cfg, "oci", "a", force=False)


@pytest.mark.usefixtures("keeper_allows")
def test_force_skips_max_in_flight_but_still_counts_toward_it(kube):
    cfg = make_config(max_in_flight=1)
    first = planner.force_run(kube, cfg, "oci", "a", force=False)
    forced = planner.force_run(kube, cfg, "oci", "b", force=True)  # at the limit: forced anyway
    assert forced.spec.force is True
    kube.set_status(first.name, phase="Done")
    with pytest.raises(ValueError, match="1 of 1"):  # the forced run still holds the vendor's slot
        planner.force_run(kube, cfg, "oci", "c", force=False)


def test_force_overrides_a_paused_vendor(kube):
    planner.force_run(kube, make_config(max_in_flight=0), "oci", "sku", force=True)


def test_force_skips_max_usd_per_hour(kube):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a", force=True)
    kube.set_status(first.name, phase="Running", pricePerHour="1.2")
    planner.force_run(kube, cfg, "oci", "b", force=True)  # over the cap


def test_force_still_refuses_a_disabled_vendor(kube):
    cfg = Config(EnvConfig(bucket="b", bucket_region="r"), Settings(vendors={"oci": VendorSettings(enabled=False)}))
    with pytest.raises(ValueError, match="not enabled"):
        planner.force_run(kube, cfg, "oci", "sku", force=True)


def test_without_force_keeper_must_allow(kube, monkeypatch):
    calls = []

    def block_reasons(env, vendor, skus, names):
        calls.append((vendor, skus, names))
        return {"sku": {"lscpu": ["CPU_OLD"]}}

    monkeypatch.setattr(keeper, "block_reasons", block_reasons)
    with pytest.raises(ValueError, match=r"lscpu: CPU_OLD.*--force"):
        planner.force_run(kube, make_config(), "oci", "sku", tasks=["lscpu"], force=False)
    assert calls == [("oci", ["sku"], ["lscpu"])]
    assert kube.objects == {}


def test_without_force_keeper_ok_and_failing_closed(kube, monkeypatch):
    monkeypatch.setattr(keeper, "block_reasons", lambda *a: {"sku": {}})
    run = planner.force_run(kube, make_config(), "oci", "sku", force=False)
    assert run.spec.force is False

    def down(*a):
        raise RuntimeError("keeper HTTP 503")

    monkeypatch.setattr(keeper, "block_reasons", down)
    with pytest.raises(ValueError, match="Keeper.*503"):
        planner.force_run(kube, make_config(), "oci", "other", force=False)


def test_keeper_key_file_failure_is_a_clean_error(kube, monkeypatch):
    def broken(*a):
        raise PermissionError("key unreadable")

    monkeypatch.setattr(keeper, "block_reasons", broken)
    with pytest.raises(ValueError, match=r"key unreadable.*--force"):
        planner.force_run(kube, make_config(), "oci", "sku", force=False)


def test_force_skips_keeper(kube, monkeypatch):
    monkeypatch.setattr(keeper, "block_reasons", lambda *a: pytest.fail("keeper called"))
    planner.force_run(kube, make_config(), "oci", "sku", force=True)


# --------------------------------------------------------------------------- autoPlan

NOW = datetime.now(UTC).replace(tzinfo=None)  # for `due_reason`; metas are built relative to the real clock
TASK_NAMES = [t.name for t in all_tasks() if not t.dbaas]
DEFAULT_TASKS = [t.name for t in all_tasks() if not t.dbaas and not t.keeper_gated]


def auto_config(**vendor) -> Config:
    vendor = {"publish": True, "max_in_flight": 1, **vendor}
    return Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(auto_plan=True, vendors={"oci": VendorSettings(enabled=True, **vendor)}),
    )


def done_meta(name, *, code=0, ago=timedelta(days=1), **fields):
    """The schedule meta of a task run `ago` before now with the current hash (`code` is its exit code)."""
    end = datetime.now(UTC).replace(tzinfo=None) - ago
    return {
        "start": (end - timedelta(minutes=1)).isoformat(),
        "end": end.isoformat(),
        "exit_code": code,
        "task_hash": task_hash(get_task(name)),
        **fields,
    }


def all_done(**overrides) -> dict[str, dict]:
    """Every default task finished with the current hash; `overrides` replaces single metas (None: never run)."""
    metas = {name: done_meta(name) for name in DEFAULT_TASKS}
    for name, meta in overrides.items():
        if meta is None:
            del metas[name]
        else:
            metas[name] = meta
    return metas


@pytest.fixture(autouse=True)
def everything_offered(request, monkeypatch):
    """SCarpenter offers every SKU, unless the test is about the listing (`@pytest.mark.offered`)."""
    if not request.node.get_closest_marker("offered"):
        monkeypatch.setattr(planner, "not_offered", lambda kube, cfg, vendor, skus, now=None, offered=None: {})


@pytest.fixture
def keeper_skus(monkeypatch):
    """`keeper.active_servers_block_reasons` answers with this dict ({vendor: {sku: {task: reasons}}})."""
    answer = {"oci": {"s1": {}}}
    monkeypatch.setattr(keeper, "active_servers_block_reasons", lambda env, names: answer)
    return answer


def plan(kube, cfg, metas, **kw):
    return planner.plan_cycle(kube, cfg, lambda vendor: metas, **kw)


def test_plan_cycle_plans_all_tasks_of_a_never_run_sku_without_force(kube, keeper_skus):
    planned = plan(kube, auto_config(), {})
    assert [(p.vendor, p.sku, list(p.tasks)) for p in planned] == [("oci", "s1", TASK_NAMES)]
    (run,) = [from_dict(o) for o in kube.objects.values()]
    assert run.spec.force is False and run.spec.sku == "s1" and run.spec.tasks == list(planned[0].tasks)
    assert run.spec.capacity == CapacityMode.SPOT_FIRST


def test_current_hash_with_a_final_result_is_not_due(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["KEEPER_NOT_ALLOWED"]}
    stored = {"s1": all_done(lscpu=done_meta("lscpu", code=1))}  # a task's own failure is final until the hash changes
    assert plan(kube, auto_config(), stored) == []
    assert kube.objects == {}


def test_changed_hash_and_rerun_prefix_are_due(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["x"]}
    stored = {
        "s1": all_done(
            lscpu={**done_meta("lscpu"), "task_hash": "0" * 40},
            openssl={**done_meta("openssl"), "task_hash": "RERUN_" + task_hash(get_task("openssl"))},
        )
    }
    (planned,) = plan(kube, auto_config(), stored)
    assert set(planned.tasks) == {"lscpu", "openssl"}
    assert "task hash changed" in planned.reason


def test_exit_code_minus_5_is_final_until_the_hash_changes(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["x"]}
    meta = done_meta("lscpu", code=-5, ago=timedelta(days=30), start_retries=3)
    assert plan(kube, auto_config(), {"s1": all_done(lscpu=meta)}) == []
    assert plan(kube, auto_config(), {"s1": all_done(lscpu={**meta, "task_hash": "old"})})[0].tasks == ("lscpu",)


@pytest.mark.parametrize(
    ("meta_fields", "ago", "due"),
    [
        # a temporary reason is ignored: the launch ConfigMap paces the retries
        *[
            ({"code": -1, "error_msg": f"SCARPENTER_{reason}: x"}, timedelta(minutes=1), True)
            for reason in ("ICE", "QUOTA", "DAILY_LIMIT", "VENDOR", "NO_OFFERING", "QUOTA_ZERO", "LAUNCH_FAILED")
        ],
        ({"code": -1, "error_msg": "SCARPENTER_BOOT_FAILURE: no node after 60 minutes"}, timedelta(minutes=1), True),
        # permanent: final for 30 days
        ({"code": -1, "error_msg": "SCARPENTER_LAUNCH_REJECTED: Fatal (X): y"}, timedelta(days=29), False),
        ({"code": -1, "error_msg": "SCARPENTER_LAUNCH_REJECTED: Fatal (X): y"}, timedelta(days=31), True),
        ({"code": -1, "error_msg": "SCARPENTER_BOOT_FAILURE: z"}, timedelta(days=29), False),
        ({"code": -1, "error_msg": "SCARPENTER_BOOT_FAILURE: z"}, timedelta(days=31), True),
        ({"code": -1, "error_msg": "something else"}, timedelta(hours=1), False),
        ({"code": -1, "error_msg": "something else"}, timedelta(hours=3), True),
        ({"code": -4, "error_msg": "NODE_LOST", "start_retries": 1}, timedelta(hours=1), False),
        ({"code": -4, "error_msg": "NODE_LOST", "start_retries": 1}, timedelta(hours=3), True),
        ({"code": -4, "error_msg": "NODE_LOST", "start_retries": 2}, timedelta(hours=3), False),
        ({"code": -4, "error_msg": "NODE_LOST", "start_retries": 2}, timedelta(hours=7), True),
        ({"code": -3, "error_msg": "no GPU"}, timedelta(days=9), False),
        ({"code": -6}, timedelta(days=9), False),
        ({"code": 256}, timedelta(days=9), False),
    ],
)
def test_failed_tasks_wait_for_their_backoff(kube, keeper_skus, meta_fields, ago, due):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["x"]}
    meta = done_meta("lscpu", ago=ago, **meta_fields)
    planned = plan(kube, auto_config(), {"s1": all_done(lscpu=meta)})
    assert bool(planned) is due


def test_keeper_block_reasons_drop_tasks_and_skus(kube, keeper_skus):
    keeper_skus["oci"] = {"s1": {name: ["KEEPER_X"] for name in TASK_NAMES}, "s2": {"membench": ["KEEPER_X"]}}
    (planned,) = plan(kube, auto_config(), {})
    assert planned.sku == "s2" and "membench" not in planned.tasks and "lscpu" in planned.tasks


def test_keeper_gated_task_runs_only_when_keeper_allows_it(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["KEEPER_NOT_ALLOWED"]}
    (planned,) = plan(kube, auto_config(), {}, dry_run=True)
    assert "stressnglongrun" not in planned.tasks
    keeper_skus["oci"]["s1"] = {}
    (planned,) = plan(kube, auto_config(), {}, dry_run=True)
    assert "stressnglongrun" in planned.tasks


def test_sku_with_a_run_is_skipped(kube, keeper_skus):
    keeper_skus["oci"] = {"s1": {}, "s2": {}}
    first = order("s1", "s2")[0]
    kube.create("benchmarkrun", crd.to_manifest(RunSpec("oci", first, "r1", ["lscpu"]), "inspector"))
    kube.objects[crd.run_name("oci", first)]["status"] = {"phase": "Done"}  # finished, not yet published
    (planned,) = plan(kube, auto_config(), {})
    assert planned.sku == order("s1", "s2")[1]


def order(*skus):
    return sorted(skus, key=lambda sku: planner.start_order("oci", sku))


def test_order_is_crc32_of_vendor_and_sku_and_one_run_per_cycle(kube, keeper_skus):
    names = ["s1", "s2", "s3", "s4", "s5"]
    keeper_skus["oci"] = {name: {} for name in names}
    assert planner.start_order("oci", "s1")[0] == zlib.crc32(b"oci/s1")
    (planned,) = plan(kube, auto_config(), {})  # maxInFlight 1: the first run holds the vendor
    assert planned.sku == order(*names)[0]
    assert plan(kube, auto_config(), {}) == []  # next cycle: at the limit
    assert len(kube.objects) == 1


def test_max_in_flight_two_starts_two_in_order(kube, keeper_skus):
    names = ["s1", "s2", "s3"]
    keeper_skus["oci"] = {name: {} for name in names}
    planned = plan(kube, auto_config(max_in_flight=2), {})
    assert [p.sku for p in planned] == order(*names)[:2]


def test_dry_run_creates_nothing(kube, keeper_skus):
    assert len(plan(kube, auto_config(max_in_flight=2), {}, dry_run=True)) == 1
    assert kube.objects == {}


def test_vendor_at_its_cap_or_paused_skips_keeper_and_git(kube, monkeypatch):
    monkeypatch.setattr(keeper, "active_servers_block_reasons", lambda *a: pytest.fail("keeper called"))
    assert plan(kube, auto_config(max_in_flight=0), {}) == []


def test_keeper_unreachable_skips_the_cycle(kube, monkeypatch, caplog):
    def down(env, names):
        raise RuntimeError("keeper HTTP 503")

    monkeypatch.setattr(keeper, "active_servers_block_reasons", down)
    assert plan(kube, auto_config(), {}) == []
    assert kube.objects == {} and "Keeper is not reachable" in caplog.text


def test_unreadable_git_skips_the_vendor(kube, keeper_skus, caplog):
    def broken(vendor):
        raise RuntimeError("git fetch failed")

    assert planner.plan_cycle(kube, auto_config(), broken) == []
    assert kube.objects == {} and "cannot read" in caplog.text


def test_only_published_skus_of_enabled_auto_plan_vendors(kube, keeper_skus):
    assert plan(kube, auto_config(publish=False), {}) == []  # git would never learn about the results
    keeper_skus["oci"] = {"s1": {}, "s2": {}}
    assert [p.sku for p in plan(kube, auto_config(publish=("s2",)), {})] == ["s2"]
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"), Settings(vendors={"oci": VendorSettings(enabled=True, publish=True)})
    )
    assert plan(kube, cfg, {}) == []  # autoPlan off


def test_gpu_tasks_are_dropped_for_a_sku_without_a_gpu(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"nvidia_smi": ["KEEPER_NO_GPU"]}
    gpu = {t.name for t in all_tasks() if t.gpu}
    (planned,) = plan(kube, auto_config(), {}, dry_run=True)
    assert gpu and not gpu & set(planned.tasks) and "lscpu" in planned.tasks
    keeper_skus["oci"]["s1"] = {}  # a GPU SKU keeps them
    (planned,) = plan(kube, auto_config(), {}, dry_run=True)
    assert gpu <= set(planned.tasks)


def test_a_sku_with_only_gpu_tasks_due_is_not_planned(kube, keeper_skus):
    keeper_skus["oci"] = {"s1": {"nvidia_smi": ["KEEPER_NO_GPU"]}, "s2": {}}
    cpu_done = {name: done_meta(name) for name in DEFAULT_TASKS if not get_task(name).gpu}
    assert [p.sku for p in plan(kube, auto_config(), {"s1": cpu_done, "s2": cpu_done})] == ["s2"]


def test_sku_names_that_are_no_label_values_are_skipped_with_one_warning(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"bad sku": {}, "bad/two": {}, "good": {}}
    assert [p.sku for p in plan(kube, auto_config(), {})] == ["good"]
    warnings = [r.message for r in caplog.records if "not label values" in r.message]
    assert len(warnings) == 1 and "skips 2 SKUs of vendor oci" in warnings[0]


def test_unreadable_meta_skips_only_that_sku(kube, keeper_skus):
    keeper_skus["oci"] = {"s1": {}, "s2": {}}
    metas = {"s1": {"lscpu": None}, "s2": {}}
    assert [p.sku for p in plan(kube, auto_config(max_in_flight=2), metas)] == ["s2"]


def test_vendor_with_stalled_publishing_gets_no_new_run(kube, keeper_skus, caplog):
    run = RunSpec("oci", "old", "r1", ["lscpu"])
    kube.create("benchmarkrun", crd.to_manifest(run, "inspector"))
    ended = (datetime.now(UTC) - timedelta(minutes=11)).strftime("%Y-%m-%dT%H:%M:%SZ")
    kube.objects[crd.run_name("oci", "old")]["status"] = {
        "phase": "Publishing",
        "tasks": [{"name": "lscpu", "state": "Done", "attempt": 1, "end": ended, "published": False}],
    }
    assert plan(kube, auto_config(max_in_flight=2), {}) == []
    assert "publishing is failing" in caplog.text
    kube.objects[crd.run_name("oci", "old")]["status"]["tasks"][0]["published"] = True
    assert len(plan(kube, auto_config(max_in_flight=2), {})) == 1


def test_no_active_servers_for_an_enabled_vendor_is_logged(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {}
    caplog.set_level("INFO")
    assert plan(kube, auto_config(), {}) == []
    assert "no ACTIVE servers for vendor oci" in caplog.text


def test_keeper_answer_is_cached_for_30_minutes(kube, monkeypatch):
    calls = []
    monkeypatch.setattr(keeper, "active_servers_block_reasons", lambda env, names: calls.append(1) or {"oci": {}})
    clock = [1000.0]
    monkeypatch.setattr(planner.time, "monotonic", lambda: clock[0])
    cache = planner.KeeperCache()
    for _ in range(2):
        planner.plan_cycle(kube, auto_config(), lambda vendor: {}, keeper_cache=cache)
    assert len(calls) == 1
    clock[0] += 31 * 60
    planner.plan_cycle(kube, auto_config(), lambda vendor: {}, keeper_cache=cache)
    assert len(calls) == 2
    planner.plan_cycle(kube, auto_config(), lambda vendor: {})  # no cache object: asks every time
    assert len(calls) == 3


def test_due_reason_without_a_result_or_for_a_current_success():
    task = get_task("lscpu")
    assert planner.due_reason(task, None, NOW) == "never run"
    assert planner.due_reason(task, done_meta("lscpu"), NOW) is None
    assert planner.due_reason(task, {**done_meta("lscpu"), "end": None}, NOW) == "no result"


def test_run_forever_survives_errors_and_stops():
    stop = threading.Event()
    calls = []

    def provider():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("configmap unreadable")
        stop.set()
        return make_config()

    stop_wait = stop.wait
    stop.wait = lambda timeout=None: stop_wait(0)  # no real sleeping
    planner.run_forever(FakeKube(), provider, lambda vendor: {}, stop)
    assert len(calls) == 2


# --------------------------------------------------------------------------- SCarpenter offered types


def offered_configmap(types: dict, *, age=timedelta(minutes=1), truncated=False, naive_time=False, gz=False) -> dict:
    """ConfigMap `scarpenter-offered-bench-oci` as SCarpenter writes it; `types` is {sku: entry fields}. `gz`: the
    gzipped `binaryData` form (base64, as the API client returns it) instead of `data`."""
    generated = datetime.now(UTC) - age
    document = {
        "generatedAt": generated.replace(tzinfo=None).isoformat() if naive_time else generated.isoformat(),
        "class": "bench",
        "vendor": "oci",
        "truncated": truncated,
        "types": {
            f"oci.{sku}": {"apiReference": sku, "capacityTypes": [], "offerings": [], **fields}
            for sku, fields in types.items()
        },
    }
    if gz:
        return {
            "binaryData": {"types.json.gz": base64.b64encode(gzip.compress(json.dumps(document).encode())).decode()}
        }
    return {"data": {"types.json": json.dumps(document)}}


def publish_offered(kube, configmap: dict) -> None:
    kube.foreign_configmaps["scarpenter-system", "scarpenter-offered-bench-oci"] = configmap


OFFERED = {"available": True, "capacityTypes": ["on-demand", "spot"]}
QUOTA = {"available": False, "reason": "QuotaGate", "detail": "headroom 6, need 16"}


def not_offered_of(kube):
    return planner.not_offered(kube, auto_config(), "oci", ["s1"])


def planned_skus(kube, cfg=None) -> list[str]:
    return [p.sku for p in plan(kube, cfg or auto_config(), {}, dry_run=True)]


@pytest.mark.offered
def test_offered_types_skip_unavailable_skus(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"quota": {}, "ok": {}}
    publish_offered(kube, offered_configmap({"quota": QUOTA, "ok": OFFERED}))
    with caplog.at_level("INFO"):
        assert planned_skus(kube) == ["ok"]
    assert "autoPlan skips 1 SKUs of vendor oci not offered by SCarpenter, e.g. quota (QuotaGate)" in caplog.text


@pytest.mark.offered
def test_offered_types_skip_a_sku_without_an_entry(kube, keeper_skus):
    keeper_skus["oci"] = {"unknown": {}, "ok": {}}
    publish_offered(kube, offered_configmap({"ok": OFFERED}))
    assert planned_skus(kube) == ["ok"]


@pytest.mark.offered
def test_offered_types_on_demand_mode_needs_an_on_demand_offering(kube, keeper_skus):
    keeper_skus["oci"] = {"spot-only": {}}
    publish_offered(kube, offered_configmap({"spot-only": {"available": True, "capacityTypes": ["spot"]}}))
    assert planned_skus(kube, auto_config(capacity=CapacityMode.ON_DEMAND)) == []
    assert planned_skus(kube, auto_config(capacity=CapacityMode.SPOT_FIRST)) == ["spot-only"]


@pytest.mark.offered
@pytest.mark.parametrize(
    "configmap",
    [
        None,  # vendor not opted in
        offered_configmap({"s1": QUOTA}, age=timedelta(minutes=31)),  # stale
        offered_configmap({"s1": QUOTA}, age=timedelta(minutes=31), gz=True),
        {"data": {"types.json": "{not json"}},
        {"data": {}},
        {"binaryData": {"types.json.gz": base64.b64encode(b"not gzip").decode()}},
        {"data": {"types.json": json.dumps({"generatedAt": "now", "truncated": False, "types": {}})}},
        offered_configmap({"s1": QUOTA}, naive_time=True),
        offered_configmap({"s1": {"available": True, "capacityTypes": None}}),
        offered_configmap({"s1": {"available": True, "capacityTypes": ["spot"], "offerings": None}}),
    ],
    ids=[
        "absent",
        "stale",
        "stale-gzip",
        "bad-json",
        "no-key",
        "corrupt-gzip",
        "bad-time",
        "naive-time",
        "null-capacity-types",
        "null-offerings",
    ],
)
def test_an_unusable_offered_types_listing_skips_the_vendor_with_a_warning(kube, keeper_skus, caplog, configmap):
    if configmap:
        publish_offered(kube, configmap)
    assert planned_skus(kube) == []
    assert "offered types of vendor oci unusable" in caplog.text
    assert "autoPlan skips vendor oci: its SCarpenter offered-types listing is unusable" in caplog.text
    assert not_offered_of(kube) is None
    skipped = []
    plan(kube, auto_config(), {}, dry_run=True, on_vendor_skipped=lambda *args: skipped.append(args))
    assert skipped == [("oci", planner.OFFERED_UNUSABLE)]  # the metric's source
    publish_offered(kube, offered_configmap({"s1": OFFERED}))  # a usable listing plans again
    assert planned_skus(kube) == ["s1"]


@pytest.mark.offered
def test_offered_types_read_the_gzipped_listing(kube, keeper_skus):
    keeper_skus["oci"] = {"quota": {}, "ok": {}}
    configmap = offered_configmap({"quota": QUOTA, "ok": OFFERED}, gz=True)
    document = json.loads(gzip.decompress(base64.b64decode(configmap["binaryData"]["types.json.gz"])))
    document["zonesOmitted"] = True  # tolerated
    configmap["binaryData"]["types.json.gz"] = base64.b64encode(gzip.compress(json.dumps(document).encode())).decode()
    publish_offered(kube, configmap)
    assert planned_skus(kube) == ["ok"]


@pytest.mark.offered
def test_offered_types_prefer_the_gzipped_listing_over_data(kube, keeper_skus):
    keeper_skus["oci"] = {"s1": {}}
    gz = offered_configmap({"s1": OFFERED}, gz=True)
    plain = offered_configmap({"s1": QUOTA})
    publish_offered(kube, {**plain, **gz})
    assert planned_skus(kube) == ["s1"]
    publish_offered(kube, {**offered_configmap({"s1": QUOTA}, gz=True), **offered_configmap({"s1": OFFERED})})
    assert planned_skus(kube) == []


@pytest.mark.offered
def test_offered_types_without_truncated_field_are_complete(kube, keeper_skus):
    keeper_skus["oci"] = {"unknown": {}}
    configmap = offered_configmap({"ok": OFFERED})
    document = json.loads(configmap["data"]["types.json"])
    del document["truncated"]
    publish_offered(kube, {"data": {"types.json": json.dumps(document)}})
    assert planned_skus(kube) == []  # a missing entry counts: the list is complete


@pytest.mark.offered
def test_offered_types_truncated_list_skips_only_listed_unavailable(kube, keeper_skus):
    keeper_skus["oci"] = {"quota": {}, "unknown": {}}
    publish_offered(kube, offered_configmap({"quota": QUOTA}, truncated=True))
    assert planned_skus(kube) == ["unknown"]


@pytest.mark.offered
def test_force_run_ignores_offered_types(kube):
    publish_offered(kube, offered_configmap({"s1": QUOTA}))
    run = planner.force_run(kube, make_config(), "oci", "s1", force=True)
    assert run.spec.sku == "s1"


@pytest.mark.offered
def test_offered_types_accept_an_offset_time_and_ignore_entries_without_api_reference(kube, keeper_skus):
    keeper_skus["oci"] = {"quota": {}, "ok": {}}
    configmap = offered_configmap({"quota": QUOTA, "ok": OFFERED})
    document = json.loads(configmap["data"]["types.json"])
    document["generatedAt"] = datetime.now(timezone(timedelta(hours=1))).isoformat()
    document["types"]["oci.no-label-value"] = {"available": False, "capacityTypes": [], "reason": "NoPrice"}
    publish_offered(kube, {"data": {"types.json": json.dumps(document)}})
    assert planned_skus(kube) == ["ok"]


@pytest.mark.offered
def test_offered_types_duplicate_api_reference_prefers_the_available_entry(kube, keeper_skus):
    configmap = offered_configmap({"s1": OFFERED})
    document = json.loads(configmap["data"]["types.json"])
    document["types"]["oci.s1-twin"] = {"apiReference": "s1", **QUOTA, "capacityTypes": []}
    for types in (document["types"], dict(reversed(document["types"].items()))):
        publish_offered(kube, {"data": {"types.json": json.dumps({**document, "types": types})}})
        assert planned_skus(kube) == ["s1"]


@pytest.mark.offered
@pytest.mark.parametrize("capacity", [CapacityMode.SPOT_FIRST, CapacityMode.ON_DEMAND])
def test_offered_types_available_without_capacity_types_is_skipped(kube, keeper_skus, capacity):
    publish_offered(kube, offered_configmap({"s1": {"available": True, "capacityTypes": []}}))
    assert planned_skus(kube, auto_config(capacity=capacity)) == []


@pytest.mark.offered
def test_offered_types_truncated_list_with_on_demand_mode(kube, keeper_skus):
    keeper_skus["oci"] = {"spot-only": {}, "quota": {}, "unknown": {}, "ok": {}}
    publish_offered(
        kube,
        offered_configmap(
            {"spot-only": {"available": True, "capacityTypes": ["spot"]}, "quota": QUOTA, "ok": OFFERED}, truncated=True
        ),
    )
    cfg = auto_config(capacity=CapacityMode.ON_DEMAND)
    skipped = planner.not_offered(kube, cfg, "oci", list(keeper_skus["oci"]))
    assert skipped == {"spot-only": "no on-demand capacity", "quota": "QuotaGate"}


# --------------------------------------------------------------------------- launch retries


NOW = datetime.now(UTC)


def test_a_sku_waiting_for_its_launch_retry_is_skipped_until_next(kube, keeper_skus):
    launch.record_failure(kube, "oci", "s1", "ICE", NOW)
    assert planned_skus(kube) == []
    launch.record_failure(kube, "oci", "s1", "ICE", NOW - timedelta(hours=2))  # next was 2 h ago + 2 h
    entry = launch.read(kube, "oci")["s1"]
    assert entry["count"] == 2
    kube.configmaps["k8s-inspector-launch-oci"]["data"]["skus"] = json.dumps(
        {"s1": {**entry, "next": (NOW - timedelta(minutes=1)).isoformat()}}
    )
    assert planned_skus(kube) == ["s1"]


def test_a_stale_temporary_meta_does_not_block_and_a_current_permanent_one_does(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["x"]}
    old = done_meta("lscpu", ago=timedelta(minutes=5), code=-1, error_msg="SCARPENTER_ICE: x")
    assert plan(kube, auto_config(), {"s1": all_done(lscpu=old)})[0].tasks == ("lscpu",)
    rejected = {**old, "error_msg": "SCARPENTER_LAUNCH_REJECTED: Fatal (X): y"}
    assert plan(kube, auto_config(), {"s1": all_done(lscpu=rejected)}) == []


# An unavailable type as SCarpenter writes it when launch errors marked its offerings (scarpenter
# pkg/controllers/offered/listing_test.go): `until` is per offering, the entry itself has none.
MARKED = {
    "available": False,
    "capacityTypes": [],
    "reason": "UnavailableCache",
    "detail": "QuotaExceeded until 2026-11-21T10:05:00Z",
    "offerings": [
        {"capacityType": "on-demand", "zones": ["z3"], "available": False, "reason": "DailyLimit",
         "until": "2026-11-22T00:00:00Z"},
        {"capacityType": "on-demand", "zones": ["z1", "z2"], "available": False, "reason": "QuotaExceeded",
         "until": "2026-11-21T10:05:00Z"},
    ],
}  # fmt: skip


@pytest.mark.offered
def test_a_marked_sku_is_skipped_whatever_its_until(kube, keeper_skus, caplog):
    for until in ("2099-01-01T00:00:00Z", "2000-01-01T00:00:00Z", "tomorrow"):  # SCarpenter drops an expired mark
        offerings = [{**o, "until": until} if "until" in o else o for o in MARKED["offerings"]]
        publish_offered(kube, offered_configmap({"s1": {**MARKED, "offerings": offerings}}))
        with caplog.at_level("INFO"):
            assert planned_skus(kube) == []
        assert "e.g. s1 (UnavailableCache)" in caplog.text


@pytest.mark.offered
def test_offered_until_is_the_latest_until_of_the_unavailable_offerings(kube):
    cfg = make_config()
    publish_offered(kube, offered_configmap({"s1": MARKED}))
    assert planner.offered_until(kube, cfg, "oci", "s1") == datetime(2026, 11, 22, tzinfo=UTC)
    assert planner.offered_until(kube, cfg, "oci", "other") is None
    publish_offered(kube, offered_configmap({"s1": OFFERED}))
    assert planner.offered_until(kube, cfg, "oci", "s1") is None  # nothing marked
    # an until of an available offering, a damaged one and one without timezone are ignored
    offerings = [
        {"capacityType": "spot", "zones": ["z"], "available": True, "until": "2099-01-01T00:00:00Z"},
        {"capacityType": "spot", "zones": ["z"], "available": False, "until": "tomorrow"},
        {"capacityType": "spot", "zones": ["z"], "available": False, "until": "2099-01-01T00:00:00"},
        {"capacityType": "spot", "zones": ["z"], "available": False, "until": None},
        {"capacityType": "spot", "zones": ["z"], "available": False},  # an older SCarpenter: no `until` key
        {"capacityType": "spot", "zones": ["z"], "available": False, "until": "2026-11-21T10:05:00+01:00"},
    ]
    publish_offered(kube, offered_configmap({"s1": {**QUOTA, "offerings": offerings}}))
    assert planner.offered_until(kube, cfg, "oci", "s1") == datetime(2026, 11, 21, 9, 5, tzinfo=UTC)


@pytest.mark.offered
def test_offered_until_has_the_checks_of_not_offered(kube):
    cfg = make_config()
    for configmap in (
        None,
        offered_configmap({"s1": MARKED}, age=timedelta(minutes=31)),  # stale
        offered_configmap({"s1": MARKED}, naive_time=True),
        {"data": {"types.json": "{not json"}},
        offered_configmap({"s1": {**MARKED, "capacityTypes": None}}),
    ):
        if configmap:
            publish_offered(kube, configmap)
        assert planner.offered_until(kube, cfg, "oci", "s1") is None


def test_a_damaged_launch_configmap_stops_no_vendor_and_a_dry_run_prunes_nothing(kube, keeper_skus):
    launch.record_failure(kube, "oci", "gone", "ICE", NOW)
    assert planned_skus(kube) == ["s1"]
    assert set(launch.read(kube, "oci")) == {"gone"}  # a dry run leaves the entry of a SKU Keeper does not list
    for skus in (["s1"], {"s1": {"next": 5}, "x": None}):
        kube.configmaps["k8s-inspector-launch-oci"]["data"]["skus"] = json.dumps(skus)
        assert planned_skus(kube) == ["s1"]
    assert [p.sku for p in plan(kube, auto_config(), {})] == ["s1"]


def test_a_live_cycle_prunes_the_skus_keeper_no_longer_lists(kube, keeper_skus):
    launch.record_failure(kube, "oci", "gone", "ICE", NOW)
    launch.record_failure(kube, "oci", "s1", "ICE", NOW)
    plan(kube, auto_config(), {})
    assert set(launch.read(kube, "oci")) == {"s1"}


@pytest.mark.offered
def test_offered_until_keeps_the_latest_until_of_duplicate_api_references(kube):
    cfg = make_config()
    soon = {"capacityType": "on-demand", "zones": ["z"], "available": False, "until": "2026-11-21T10:05:00Z"}
    late = {**soon, "until": "2026-11-22T00:00:00Z"}
    for first, second in ((soon, late), (late, soon)):
        configmap = offered_configmap({"s1": {**QUOTA, "offerings": [first]}})
        document = json.loads(configmap["data"]["types.json"])
        document["types"]["oci.s1-twin"] = {"apiReference": "s1", **QUOTA, "capacityTypes": [], "offerings": [second]}
        publish_offered(kube, {"data": {"types.json": json.dumps(document)}})
        assert planner.offered_until(kube, cfg, "oci", "s1") == datetime(2026, 11, 22, tzinfo=UTC)


@pytest.mark.offered
def test_offered_until_judges_staleness_by_the_given_time(kube):
    cfg = make_config()
    publish_offered(kube, offered_configmap({"s1": MARKED}))
    later = datetime.now(UTC) + timedelta(hours=1)
    assert planner.offered_until(kube, cfg, "oci", "s1", later) is None
    assert planner.offered_until(kube, cfg, "oci", "s1") is not None


@pytest.mark.offered
def test_an_entry_without_offerings_is_usable_and_a_wrong_type_is_not(kube, keeper_skus):
    configmap = offered_configmap({"s1": OFFERED})
    document = json.loads(configmap["data"]["types.json"])
    del document["types"]["oci.s1"]["offerings"]
    publish_offered(kube, {"data": {"types.json": json.dumps(document)}})
    assert planner.read_offered(kube, make_config(), "oci") is not None
    document["types"]["oci.s1"]["offerings"] = {"a": 1}
    publish_offered(kube, {"data": {"types.json": json.dumps(document)}})
    assert planner.read_offered(kube, make_config(), "oci") is None


@pytest.mark.parametrize(
    "damage",
    [{"exit_code": "x"}, {"exit_code": [1]}, {"exit_code": -1, "end": "yesterday"}, {"exit_code": -1, "end": 5},
     {"exit_code": -4, "start_retries": "many"}, {"exit_code": -4, "start_retries": [1]}],
)  # fmt: skip
def test_a_damaged_meta_skips_the_sku_instead_of_raising(kube, keeper_skus, damage):
    keeper_skus["oci"] = {"s1": {}, "s2": {}}
    metas = {"s1": all_done(lscpu={**done_meta("lscpu", ago=timedelta(hours=9), code=-1), **damage})}
    planned = plan(kube, auto_config(), metas)
    assert [p.sku for p in planned] == ["s2"]


def boot_failed(kube, skus, ago=timedelta(0)):
    for sku in skus:
        launch.record_failure(kube, "oci", sku, "BOOT_FAILURE", datetime.now(UTC) - ago)


def test_a_vendor_with_boot_failures_on_three_skus_is_paused(kube, keeper_skus, caplog):
    boot_failed(kube, ["x", "y", "z"])
    assert plan(kube, auto_config(), {}) == []
    assert "autoPlan skips vendor oci" in caplog.text
    launch.node_ready(kube, "oci", "s1")  # a node of the vendor is ready: the pause ends
    assert [p.sku for p in plan(kube, auto_config(), {})] == ["s1"]


def test_two_boot_failures_or_old_ones_do_not_pause_the_vendor(kube, keeper_skus):
    boot_failed(kube, ["x", "y"])
    assert [p.sku for p in plan(kube, auto_config(), {}, dry_run=True)] == ["s1"]
    boot_failed(kube, ["z"], ago=timedelta(hours=25))
    assert [p.sku for p in plan(kube, auto_config(), {}, dry_run=True)] == ["s1"]


def test_a_forced_run_ignores_the_breaker(kube):
    boot_failed(kube, ["x", "y", "z"])
    assert launch.paused(kube, "oci", datetime.now(UTC))
    run = planner.force_run(kube, make_config(), "oci", "s1")
    assert run.spec.force is True


# --------------------------------------------------------------------------- a new hardware variant

VARIANT_CONFIGMAP = "k8s-inspector-launch-oci"
HOURS_12 = timedelta(hours=12)  # the default metas started a day ago: a variant that appeared 12 h ago is newer


def newest(n, missing=()):
    """What `Publisher.read_newest_variants` returns for s1: variant `n` has a meta.json for every default task
    except `missing`."""
    return {"s1": (n, frozenset(DEFAULT_TASKS) - set(missing))}


def plan_variant(kube, variants, metas=None, first_seen=-HOURS_12, **kw):
    """One cycle with the stressnglongrun task blocked (so only the default tasks are in play). `first_seen` is
    the age of the newest variant (a negative `first_seen` is `now - first_seen`; None: variant.json has none);
    the lease is freed afterwards, as if the run had ended."""
    metas = {"s1": all_done()} if metas is None else metas
    appeared = None if first_seen is None else datetime.now(UTC).replace(tzinfo=None) + first_seen
    try:
        return planner.plan_cycle(
            kube,
            auto_config(),
            lambda vendor: metas,
            variant_reader=lambda vendor: variants,
            first_seen_reader=lambda vendor, sku, n: appeared,
            **kw,
        )
    finally:
        kube.objects.clear()


@pytest.fixture
def quiet(keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["KEEPER_NOT_ALLOWED"]}
    return keeper_skus


def test_a_task_attempted_before_the_variant_appeared_is_due(kube, quiet):
    planned = plan_variant(kube, newest(1, missing=["lscpu", "membench"]))
    assert [(p.sku, set(p.tasks)) for p in planned] == [("s1", {"lscpu", "membench"})]
    assert "missing from the newest hardware variant" in planned[0].reason


def test_variant_0_is_checked_too(kube, quiet):
    assert [p.tasks for p in plan_variant(kube, newest(0, missing=["lscpu"]))] == [("lscpu",)]


def test_a_task_never_run_on_a_partial_newest_variant_is_due_for_its_own_reason(kube, quiet):
    metas = {"s1": all_done(lscpu=None)}
    planned = plan_variant(kube, newest(1, missing=["lscpu"]), metas)
    assert [(p.tasks, p.reason) for p in planned] == [(("lscpu",), "lscpu: never run")]
    # the same without a readable first_seen: the normal rules still apply
    assert [p.tasks for p in plan_variant(kube, newest(1, missing=["lscpu"]), metas, first_seen=None)] == [("lscpu",)]


def test_a_task_attempted_since_the_variant_appeared_is_not_due_again(kube, quiet):
    """The rerun landed on an older variant (or failed): its schedule meta is newer than first_seen, variant N
    still lacks the task, and nothing is due."""
    variants = newest(1, missing=["lscpu"])
    assert plan_variant(kube, variants, first_seen=-timedelta(days=2)) == []
    assert plan_variant(kube, variants, first_seen=-HOURS_12)  # the variant appeared after the last attempt


def test_a_variant_without_first_seen_is_not_filled(kube, quiet):
    assert plan_variant(kube, newest(1, missing=["lscpu"]), first_seen=None) == []


def test_an_unreadable_first_seen_or_variant_list_does_not_break_the_cycle(kube, quiet):
    def broken(*args):
        raise RuntimeError("git")

    variants = newest(1, missing=["lscpu"])
    metas = {"s1": all_done(lscpu=None)}
    assert planner.plan_cycle(
        kube, auto_config(), lambda v: metas, variant_reader=lambda v: variants, first_seen_reader=broken
    )
    kube.objects.clear()
    assert (
        planner.plan_cycle(
            kube, auto_config(), lambda v: {"s1": all_done()}, variant_reader=broken, first_seen_reader=broken
        )
        == []
    )
    kube.objects.clear()
    # an old-style `variants` key in the launch ConfigMap is ignored
    kube.configmaps[VARIANT_CONFIGMAP] = {"data": {"variants": "{damaged", "skus": "{}"}, "metadata": {}}
    assert plan_variant(kube, variants)


def test_first_seen_is_read_only_for_a_sku_with_a_candidate_task(kube, quiet):
    reads = []
    planner.plan_cycle(
        kube,
        auto_config(),
        lambda v: {"s1": all_done()},
        variant_reader=lambda v: newest(1),
        first_seen_reader=lambda *args: reads.append(args),
    )
    assert reads == []


# --- end to end: failed fill attempts are published by `render.render_task`, the planner reads the resulting metas


class FillScenario:
    """A SKU whose `lscpu` succeeded on variant 0 long ago, while variant 1 appeared later and lacks it. `attempt`
    publishes a result of a fill attempt like the publisher does; `plan` plans from the schedule metas it left."""

    FIRST_SEEN = -timedelta(hours=200)

    def __init__(self, tree):
        self.tree = tree
        self.task = get_task("lscpu")
        self.runs = 0
        self.publish(0, -timedelta(hours=300), 0)

    def publish(self, variant, start, code, error_msg=None):
        self.runs += 1
        begin = datetime.now(UTC).replace(tzinfo=None) + start
        status = {
            "start": begin.isoformat(),
            "end": (begin + timedelta(minutes=1)).isoformat(),
            "exit_code": code,
            "error_msg": error_msg,
            "task_hash": task_hash(self.task),
            "run_id": f"r{self.runs}",
        }
        inp = render.RenderInput(RunKey("oci", "s1", datetime(2026, 1, 1).date(), "r"), self.task, "lscpu", status)
        render.render_task(self.tree, "oci", "s1", variant, inp)

    def attempt(self, ago_hours, code, error_msg=None, variant=None):
        self.publish(variant, -timedelta(hours=ago_hours), code, error_msg)

    def schedule(self):
        return json.loads((self.tree / "data" / "oci" / "s1" / "schedule" / "lscpu" / "meta.json").read_text())

    def plan(self, kube):
        metas = {"s1": all_done(lscpu=self.schedule())}
        return plan_variant(kube, newest(1, missing=["lscpu"]), metas, first_seen=self.FIRST_SEEN)


@pytest.fixture
def fill(tmp_path):
    return FillScenario(tmp_path)


def test_the_fill_is_due_while_only_the_old_success_exists(kube, quiet, fill):
    assert [p.tasks for p in fill.plan(kube)] == [("lscpu",)]


@pytest.mark.parametrize("variant", [None, 0, 1])
@pytest.mark.parametrize("ago,due", [(1, False), (3, True)])
def test_the_first_lost_node_is_retried_after_two_hours(kube, quiet, fill, variant, ago, due):
    """-4 on no variant, on the old variant 0 or on the new variant 1: the schedule keeps the old success and
    `last_attempt` carries the failure, so the fill is not due at every cycle."""
    fill.attempt(ago, -4, "NODE_LOST", variant)
    assert fill.schedule()["exit_code"] == 0
    assert fill.schedule()["last_attempt"]["start_retries"] == 1
    assert bool(fill.plan(kube)) is due


@pytest.mark.parametrize("variant", [None, 0, 1])
def test_three_lost_nodes_in_a_row_end_with_minus_5(kube, quiet, fill, variant):
    fill.attempt(120, -4, "NODE_LOST", variant)
    fill.attempt(110, -4, "NODE_LOST", variant)
    assert fill.schedule()["last_attempt"]["start_retries"] == 2
    fill.attempt(100, -4, "NODE_LOST", variant)
    last = fill.schedule()["last_attempt"]
    assert (last["exit_code"], last["error_msg"], last["start_retries"]) == (-5, "exceeded 3 start retries", 3)
    assert fill.schedule()["exit_code"] == 0  # the old success stays
    assert fill.plan(kube) == []


@pytest.mark.parametrize("ago,due", [(1, False), (7, True)])
def test_the_second_lost_node_waits_longer(kube, quiet, fill, ago, due):
    fill.attempt(ago + 50, -4, "NODE_LOST")
    fill.attempt(ago, -4, "NODE_LOST")
    assert bool(fill.plan(kube)) is due


@pytest.mark.parametrize(
    "code,error_msg,variant",
    [
        (-1, "SCARPENTER_LAUNCH_REJECTED: not allowed", None),
        (-1, "SCARPENTER_BOOT_FAILURE: node never registered", None),
        (137, None, 0),
        (137, None, 1),
        (1, None, 0),
        (-6, None, 1),
    ],
)
def test_final_failures_of_a_fill_attempt_are_not_repeated(kube, quiet, fill, code, error_msg, variant):
    fill.attempt(5, code, error_msg, variant)
    assert fill.schedule()["exit_code"] == 0  # a failure never replaces the success
    assert fill.schedule()["last_attempt"]["exit_code"] == code
    assert fill.plan(kube) == []


def test_a_command_that_does_not_start_is_retried_three_times_then_given_up(kube, quiet, fill):
    for ago in (120, 110, 100):
        fill.attempt(ago, 256, "cannot start command: not found")
    assert fill.schedule()["last_attempt"]["exit_code"] == -5
    assert fill.plan(kube) == []


def test_a_temporary_launch_failure_leaves_nothing_but_the_launch_backoff(kube, quiet, fill):
    assert fill.plan(kube)  # nothing was published: still due, `launch.deferred` paces it
    assert fill.schedule()["exit_code"] == 0 and "last_attempt" not in fill.schedule()


def test_a_success_after_first_seen_ends_the_fill(kube, quiet, fill):
    fill.attempt(100, -4, "NODE_LOST")
    fill.attempt(50, 0, variant=0)  # landed on the old hardware: variant 1 still lacks the task
    assert "last_attempt" not in fill.schedule()
    assert fill.plan(kube) == []


def test_a_schedule_meta_without_start_blocks_only_its_own_task(kube, quiet):
    broken = {
        "end": datetime.now(UTC).replace(tzinfo=None).isoformat(),
        "exit_code": 0,
        "task_hash": task_hash(get_task("lscpu")),
    }
    metas = {"s1": all_done(lscpu=broken, membench=None)}
    planned = plan_variant(kube, newest(1, missing=["lscpu", "membench"]), metas)
    assert [p.tasks for p in planned] == [("membench",)]


def test_blocked_and_gpu_tasks_do_not_trigger(kube, keeper_skus):
    keeper_skus["oci"]["s1"] = {"stressnglongrun": ["x"], "membench": ["KEEPER_X"], "nvidia_smi": ["NO_GPU"]}
    assert plan_variant(kube, newest(1, missing=["membench", "gpu_facts", "nvbandwidth"])) == []


def test_a_sku_without_a_variant_or_without_a_reader_has_nothing_to_trigger(kube, quiet):
    assert plan_variant(kube, {}) == []
    assert plan(kube, auto_config(), {"s1": all_done()}) == []


def test_a_dry_run_plans_the_fill_without_creating_a_run(kube, quiet):
    assert plan_variant(kube, newest(1, missing=["lscpu"]), dry_run=True)
    assert kube.configmaps.get(VARIANT_CONFIGMAP) is None


def test_a_temporary_launch_failure_defers_the_fill_through_the_backoff(kube, quiet):
    variants = newest(1, missing=["lscpu"])
    launch.record_failure(kube, "oci", "s1", "ICE", datetime.now(UTC))
    assert plan_variant(kube, variants) == []  # the backoff holds
    launch.record_failure(kube, "oci", "s1", "ICE", datetime.now(UTC) - timedelta(days=1))  # it has elapsed
    assert [p.tasks for p in plan_variant(kube, variants)] == [("lscpu",)]


@pytest.mark.offered
def test_is_offered_needs_an_available_listing_of_the_sku(kube):
    cfg = make_config()
    assert not planner.is_offered(kube, cfg, "oci", "s1", False)  # no ConfigMap
    publish_offered(kube, offered_configmap({"s1": MARKED}))
    assert not planner.is_offered(kube, cfg, "oci", "s1", False)
    publish_offered(kube, offered_configmap({"s1": {**MARKED, "available": True}}))
    assert planner.is_offered(kube, cfg, "oci", "s1", False) and not planner.is_offered(
        kube, cfg, "oci", "other", False
    )


@pytest.mark.offered
def test_is_offered_of_an_on_demand_attempt_needs_on_demand_capacity(kube):
    cfg = make_config()
    spot_only = {**MARKED, "available": True, "capacityTypes": ["spot"]}
    publish_offered(kube, offered_configmap({"s1": spot_only}))
    assert planner.is_offered(kube, cfg, "oci", "s1", False)
    assert not planner.is_offered(kube, cfg, "oci", "s1", True)


# --------------------------------------------------------------------------- quota-aware planning


def quota(bucket="B", need=64, limit=384, headroom=128):
    return {"bucket": bucket, "unit": "vCPU", "need": need, "maxLimit": limit, "maxHeadroom": headroom}


def group(capacity_type, *quotas, available=True):
    """One `offerings[]` group; `quotas` (SCarpenter's field) is left out when there are none."""
    offering = {"capacityType": capacity_type, "zones": ["z"], "available": available}
    if quotas:
        offering["quotas"] = list(quotas)
    return offering


def spot_entry(*spot_quotas, on_demand_quotas=(), spot_available=True):
    """A SKU offered on both capacity types, but not on spot when `spot_available` is false."""
    capacity_types = ["on-demand", "spot"] if spot_available else ["on-demand"]
    offerings = [
        group("spot", *spot_quotas, available=spot_available),
        group("on-demand", *on_demand_quotas),
    ]
    return {"available": True, "capacityTypes": capacity_types, "offerings": offerings}


SPOT_TASK = "lscpu"  # a task that may run on spot; the catalogue also has `stressnglongrun`, which may not


def plan_wet(kube, keeper_skus, cfg=None, *, all_due=False, **kw) -> list[str]:
    """The SKUs one real cycle plans (creates runs for); only `SPOT_TASK` is due, or every task with `all_due`."""
    done = {SPOT_TASK: None, "stressnglongrun": done_meta("stressnglongrun")}
    metas = {} if all_due else {sku: all_done(**done) for sku in keeper_skus["oci"]}
    return [p.sku for p in plan(kube, cfg or auto_config(max_in_flight=5), metas, **kw)]


@pytest.mark.offered
def test_a_busy_spot_quota_defers_the_sku_instead_of_planning_it_on_demand(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"big": {}}
    publish_offered(kube, offered_configmap({"big": spot_entry(quota(need=384, headroom=128), spot_available=False)}))
    with caplog.at_level("INFO"):
        assert plan_wet(kube, keeper_skus) == []
    assert "autoPlan defers oci/big: spot quota busy (bucket B: need 384 vCPU, headroom 128)" in caplog.text


@pytest.mark.offered
def test_a_spot_quota_that_never_fits_plans_the_sku_as_before(kube, keeper_skus):
    keeper_skus["oci"] = {"big": {}}
    never = quota(need=384, limit=256, headroom=256)
    publish_offered(kube, offered_configmap({"big": spot_entry(never, spot_available=False)}))
    assert plan_wet(kube, keeper_skus) == ["big"]


@pytest.mark.offered
@pytest.mark.parametrize("entry", [spot_entry(spot_available=False), {**spot_entry(), "offerings": []}])
def test_without_quotas_the_listing_decides_alone(kube, keeper_skus, entry):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": entry}))
    assert plan_wet(kube, keeper_skus) == ["s"]


@pytest.mark.offered
def test_damaged_quotas_are_ignored(kube, keeper_skus):
    keeper_skus["oci"] = {"s": {}}
    entry = spot_entry(spot_available=False)
    entry["offerings"][0]["quotas"] = [{"bucket": "B", "need": "lots"}]
    publish_offered(kube, offered_configmap({"s": entry}))
    assert plan_wet(kube, keeper_skus) == ["s"]


@pytest.mark.offered
def test_a_fitting_spot_quota_is_planned(kube, keeper_skus):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": spot_entry(quota(need=64, headroom=64))}))
    assert plan_wet(kube, keeper_skus) == ["s"]


@pytest.mark.offered
def test_runs_of_one_cycle_share_the_headroom_of_a_bucket(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"a": {}, "b": {}}
    entry = spot_entry(quota(need=64, headroom=100))
    publish_offered(kube, offered_configmap({"a": entry, "b": entry}))
    with caplog.at_level("INFO"):
        assert len(plan_wet(kube, keeper_skus)) == 1
    assert "headroom 36" in caplog.text


@pytest.mark.offered
def test_different_buckets_do_not_share(kube, keeper_skus):
    keeper_skus["oci"] = {"a": {}, "b": {}}
    publish_offered(
        kube,
        offered_configmap(
            {"a": spot_entry(quota("B1", headroom=64)), "b": spot_entry(quota("B2", headroom=64))},
        ),
    )
    assert sorted(plan_wet(kube, keeper_skus)) == ["a", "b"]


@pytest.mark.offered
def test_every_bucket_of_an_entry_must_fit(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"m": {}, "n": {}}
    two = spot_entry(quota("family", need=64, headroom=128), quota("total", need=64, headroom=32))
    publish_offered(kube, offered_configmap({"m": two, "n": two}))
    with caplog.at_level("INFO"):
        assert plan_wet(kube, keeper_skus) == []
    assert "bucket total: need 64 vCPU, headroom 32" in caplog.text


@pytest.mark.offered
def test_one_entry_that_never_fits_makes_the_group_never_fit(kube, keeper_skus):
    keeper_skus["oci"] = {"m": {}}
    group_quotas = (quota("family", need=64, headroom=8), quota("total", need=64, limit=32, headroom=32))
    publish_offered(kube, offered_configmap({"m": spot_entry(*group_quotas, spot_available=False)}))
    assert plan_wet(kube, keeper_skus) == ["m"]


def pending_run(kube, sku, *, claim=False):
    run = planner._create_run(kube, auto_config(), "oci", sku, [SPOT_TASK])
    if claim:
        kube.nodeclaims.append(
            {
                "metadata": {"labels": {pods.NODEPOOL_LABEL: "bench"}},
                "spec": {"requirements": [{"key": pods.RUN_NODE_LABEL, "operator": "In", "values": [run.name]}]},
            }
        )
    return run


@pytest.mark.offered
def test_pending_runs_without_a_nodeclaim_use_the_headroom(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"old": {}, "new": {}}
    entry = spot_entry(quota(need=64, headroom=100))
    publish_offered(kube, offered_configmap({"old": entry, "new": entry}))
    pending_run(kube, "old")
    with caplog.at_level("INFO"):
        assert plan_wet(kube, keeper_skus) == []
    assert "autoPlan defers oci/new: spot quota busy (bucket B: need 64 vCPU, headroom 36)" in caplog.text


@pytest.mark.offered
def test_runs_with_a_nodeclaim_are_already_in_the_listing(kube, keeper_skus):
    keeper_skus["oci"] = {"old": {}, "new": {}}
    entry = spot_entry(quota(need=64, headroom=100))
    publish_offered(kube, offered_configmap({"old": entry, "new": entry}))
    pending_run(kube, "old", claim=True)
    assert plan_wet(kube, keeper_skus) == ["new"]


@pytest.mark.offered
def test_on_demand_mode_uses_the_on_demand_group(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"s": {}}
    entry = spot_entry(quota(headroom=1000), on_demand_quotas=(quota("OD", need=64, headroom=32),))
    publish_offered(kube, offered_configmap({"s": entry}))
    cfg = auto_config(capacity=CapacityMode.ON_DEMAND)
    with caplog.at_level("INFO"):
        assert plan_wet(kube, keeper_skus, cfg) == []
    assert "autoPlan defers oci/s: on-demand quota busy (bucket OD: need 64 vCPU, headroom 32)" in caplog.text
    assert plan_wet(kube, keeper_skus) == ["s"]  # SpotFirst: the spot group has room


@pytest.mark.offered
def test_a_quarantined_sku_ignores_the_busy_spot_group_and_plans_on_demand(kube, keeper_skus, monkeypatch):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": spot_entry(quota(headroom=0), spot_available=False)}))
    monkeypatch.setattr(planner.spot, "is_quarantined", lambda *args: True)
    assert plan_wet(kube, keeper_skus) == ["s"]  # on-demand only, and the on-demand group has no quotas


def reserving_listing(kube, keeper_skus, *, holds_bucket=True):
    """`big` (spot busy: it needs 384 of bucket B, 128 are free) has waited 2 hours; `small` and `other` could run.
    A run of `mid` holds bucket B when `holds_bucket`. Returns the waits of the vendor, as the controller keeps them."""
    keeper_skus["oci"] = {"big": {}, "small": {}, "other": {}}
    entries = {
        "big": spot_entry(quota(need=384, headroom=128), spot_available=False),
        "small": spot_entry(quota(need=8, headroom=128)),
        "other": spot_entry(quota("B2", need=8, headroom=128)),
        "mid": spot_entry(quota(need=64, headroom=128)),
    }
    publish_offered(kube, offered_configmap(entries))
    if holds_bucket:
        pending_run(kube, "mid")
    return {("oci", "big"): (datetime.now(UTC) - timedelta(hours=2), frozenset({("B", None)}))}


@pytest.mark.offered
def test_a_long_deferred_sku_reserves_its_bucket_while_own_runs_hold_it(kube, keeper_skus, caplog):
    waits = reserving_listing(kube, keeper_skus)
    since = waits["oci", "big"][0]
    with caplog.at_level("INFO"):
        assert plan_wet(kube, keeper_skus, quota_waits=waits) == ["other"]  # another bucket is not reserved
    assert "autoPlan defers oci/small: its quota is reserved for big" in caplog.text
    assert waits["oci", "big"][0] == since  # still busy: the timer goes on


@pytest.mark.offered
def test_there_is_no_reservation_when_own_runs_do_not_hold_the_bucket(kube, keeper_skus):
    waits = reserving_listing(kube, keeper_skus, holds_bucket=False)  # others hold it: blocking `small` frees nothing
    assert sorted(plan_wet(kube, keeper_skus, quota_waits=waits)) == ["other", "small"]


@pytest.mark.offered
def test_quota_reserve_after_minutes_zero_turns_the_reservation_off(kube, keeper_skus):
    waits = reserving_listing(kube, keeper_skus)
    settings = Settings(
        vendors={"oci": VendorSettings(enabled=True, publish=True, max_in_flight=5)},
        auto_plan=True,
        spot=SpotSettings(quota_reserve_after_minutes=0),
    )
    cfg = Config(EnvConfig(bucket="b", bucket_region="r"), settings)
    assert sorted(plan_wet(kube, keeper_skus, cfg, quota_waits=waits)) == ["other", "small"]


@pytest.mark.offered
def test_a_reservation_ends_when_its_holder_is_no_longer_a_candidate(kube, keeper_skus):
    waits = reserving_listing(kube, keeper_skus)
    keeper_skus["oci"] = {"small": {}}  # Keeper dropped `big`
    assert plan_wet(kube, keeper_skus, quota_waits=waits) == ["small"]
    assert waits == {}


@pytest.mark.offered
def test_waits_are_rebuilt_every_cycle(kube, keeper_skus):
    keeper_skus["oci"] = {"big": {}, "gone": {}}
    publish_offered(kube, offered_configmap({"big": spot_entry(quota(need=384, headroom=128))}))
    old = datetime.now(UTC) - timedelta(hours=1)
    waits = {("oci", "big"): (old, frozenset({("B", None)})), ("oci", "gone"): (old, frozenset({("B", None)}))}
    assert plan_wet(kube, keeper_skus, quota_waits=waits) == []
    assert waits == {("oci", "big"): (old, frozenset({("B", None)}))}


@pytest.mark.offered
def test_the_longest_waiter_wins_so_two_holders_do_not_block_each_other(kube, keeper_skus):
    waits = reserving_listing(kube, keeper_skus)
    publish_offered(
        kube, offered_configmap({"small": spot_entry(quota(need=8, headroom=128)), "mid": spot_entry(quota())})
    )
    waits["oci", "small"] = (datetime.now(UTC) - timedelta(hours=3), frozenset({("B", None)}))
    keeper_skus["oci"] = {"small": {}, "big": {}}
    assert plan_wet(kube, keeper_skus, quota_waits=waits) == ["small"]


@pytest.mark.offered
def test_each_sku_is_tested_against_its_own_max_headroom(kube, keeper_skus):
    keeper_skus["oci"] = {"a": {}, "b": {}, "mid": {}}
    publish_offered(
        kube,
        offered_configmap(
            {
                "mid": spot_entry(quota(need=64, headroom=100)),
                "a": spot_entry(quota(need=32, headroom=100)),  # 100 - 64 = 36 left: fits
                "b": spot_entry(quota(need=32, headroom=70)),  # 70 - 64 = 6 left: busy
            }
        ),
    )
    pending_run(kube, "mid")
    assert plan_wet(kube, keeper_skus) == ["a"]


@pytest.mark.offered
def test_a_truncated_listing_ignores_quotas(kube, keeper_skus):
    keeper_skus["oci"] = {"big": {}}
    entry = spot_entry(quota(need=384, headroom=128), spot_available=False)
    publish_offered(kube, offered_configmap({"big": entry}, truncated=True))
    assert plan_wet(kube, keeper_skus) == ["big"]


@pytest.mark.offered
def test_a_run_that_can_only_use_on_demand_counts_against_the_on_demand_group(kube, keeper_skus):
    keeper_skus["oci"] = {"new": {}}
    entries = {
        "old": spot_entry(quota("SPOT", headroom=100), on_demand_quotas=(quota("OD", need=64, headroom=100),)),
        "new": spot_entry(quota("OD", need=64, headroom=100)),
    }
    publish_offered(kube, offered_configmap(entries))
    run = pending_run(kube, "old")
    kube.objects[run.name]["status"] = {"attempts": [{"n": 1, "outcome": "LaunchFailed"}], "attempt": 1}
    assert plan_wet(kube, keeper_skus) == []  # `old` retries on-demand: bucket OD has 36 left, `new` needs 64


@pytest.mark.offered
def test_the_reservation_waits_for_quotaReserveAfterMinutes(kube, keeper_skus):
    keeper_skus["oci"] = {"big": {}, "small": {}}
    big = spot_entry(quota(need=384, headroom=128), spot_available=False)
    publish_offered(kube, offered_configmap({"big": big, "small": spot_entry(quota(need=8, headroom=128))}))
    waits = {("oci", "big"): (datetime.now(UTC) - timedelta(minutes=30), frozenset({("B", None)}))}
    assert plan_wet(kube, keeper_skus, quota_waits=waits) == ["small"]


@pytest.mark.offered
def test_a_sku_that_gets_planned_stops_waiting(kube, keeper_skus):
    keeper_skus["oci"] = {"big": {}}
    publish_offered(kube, offered_configmap({"big": spot_entry(quota(need=64, headroom=128))}))
    waits = {("oci", "big"): (datetime.now(UTC) - timedelta(hours=3), frozenset({("B", None)}))}
    assert plan_wet(kube, keeper_skus, quota_waits=waits) == ["big"]
    assert waits == {}


@pytest.mark.offered
def test_a_run_that_cannot_use_spot_ignores_the_spot_quota(kube, keeper_skus):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": spot_entry(quota(need=384, headroom=0), spot_available=False)}))
    assert plan_wet(kube, keeper_skus, all_due=True) == [
        "s"
    ]  # every task is due, `stressnglongrun` among them: on-demand only


@pytest.mark.offered
def test_runs_created_after_the_listing_count_even_with_a_nodeclaim(kube, keeper_skus):
    keeper_skus["oci"] = {"old": {}, "new": {}}
    entry = spot_entry(quota(need=64, headroom=100))
    publish_offered(kube, offered_configmap({"old": entry, "new": entry}, age=timedelta(minutes=8)))
    run = pending_run(kube, "old", claim=True)
    created = (datetime.now(UTC) - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    kube.objects[run.name]["metadata"]["creationTimestamp"] = created
    assert plan_wet(kube, keeper_skus) == []


@pytest.mark.offered
def test_waits_of_skus_after_the_vendor_cap_are_kept(kube, keeper_skus):
    first, second, third = order("x", "y", "z")
    keeper_skus["oci"] = {first: {}, second: {}, third: {}}
    fits = spot_entry(quota("B2", need=8, headroom=128))
    busy = spot_entry(quota("B", need=384, headroom=128))
    publish_offered(kube, offered_configmap({first: fits, second: fits, third: busy}))
    wait = (datetime.now(UTC) - timedelta(minutes=50), frozenset({("B", None)}))
    waits = {("oci", third): wait}
    cfg = auto_config(max_in_flight=1)
    assert plan_wet(kube, keeper_skus, cfg, quota_waits=waits) == [first]  # the cap stops the loop before `third`
    assert waits == {("oci", third): wait}


@pytest.mark.offered
def test_the_listing_is_read_once_per_cycle(kube, keeper_skus, monkeypatch):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": OFFERED}))
    reads = []
    original = planner.read_offered
    monkeypatch.setattr(planner, "read_offered", lambda *args, **kw: reads.append(1) or original(*args, **kw))
    plan_wet(kube, keeper_skus)
    assert len(reads) == 1
    kube.foreign_configmaps.clear()  # unusable listing: still one read
    reads.clear()
    plan_wet(kube, keeper_skus)
    assert len(reads) == 1


@pytest.mark.offered
def test_a_spot_run_without_spot_quotas_consumes_nothing_known(kube, keeper_skus):
    keeper_skus["oci"] = {"new": {}}
    od = (quota("OD", need=64, headroom=100),)
    publish_offered(
        kube, offered_configmap({"old": spot_entry(on_demand_quotas=od), "new": spot_entry(on_demand_quotas=od)})
    )
    pending_run(kube, "old")  # spot offered without quotas: counts nothing, like a new run of that kind
    assert plan_wet(kube, keeper_skus) == ["new"]


# --------------------------------------------------------------------------- packing: order, fill, regions


def market(series="s", price=1.0, regions=5, accelerators=0, spot=None, vcpus=4):
    """What `keeper.sku_market` returns: `regions` regions with an on-demand price (`spot`: {region: price})."""
    return {
        "series": series,
        "accelerators": accelerators,
        "memory_generation": "DDR4",
        "vcpus": vcpus,
        "memory_gib": 16,
        "ondemand": {f"r{i}": price for i in range(regions)},
        "spot": spot or {},
    }


class Markets(dict):
    calls: list


@pytest.fixture
def markets(monkeypatch):
    """`keeper.sku_market` answers with this dict ({sku: market}); the calls are kept in `markets.calls`."""
    data = Markets()
    data_calls = []
    monkeypatch.setattr(keeper, "region_ids", lambda env, vendor: {})
    monkeypatch.setattr(keeper, "sku_market", lambda env, vendor, sku, ids: data_calls.append(sku) or data.get(sku))
    data.calls = data_calls
    return data


def test_the_popularity_order_replaces_crc32(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"gpu": {}, "narrow": {}, "wide": {}}
    markets.update(
        gpu=market("g", regions=30, accelerators=1), narrow=market("n", regions=5), wide=market("w", regions=9)
    )
    cache = popularity_cache()
    assert plan_wet(kube, keeper_skus, popularity_cache=cache) == ["wide", "narrow", "gpu"]


def popularity_cache():
    from k8s_inspector import popularity

    return popularity.PopularityCache()


def test_without_popularity_data_the_order_is_crc32_and_planning_goes_on(kube, keeper_skus, markets, monkeypatch):
    keeper_skus["oci"] = {"a": {}, "b": {}, "c": {}}

    def down(*a):
        raise RuntimeError("keeper HTTP 503")

    monkeypatch.setattr(keeper, "sku_market", down)
    assert plan_wet(kube, keeper_skus, popularity_cache=popularity_cache()) == list(order("a", "b", "c"))


def test_the_cache_is_warmed_a_few_skus_per_cycle(kube, keeper_skus, markets):
    from k8s_inspector import popularity

    keeper_skus["oci"] = {f"s{i}": {} for i in range(popularity.LOOKUPS_PER_CYCLE + 5)}
    cache = popularity_cache()
    plan(kube, auto_config(max_in_flight=0), {}, popularity_cache=cache, dry_run=True)
    assert markets.calls == []  # a paused vendor is not even looked at
    plan(kube, auto_config(max_in_flight=UNLIMITED), {}, popularity_cache=cache, dry_run=True)
    assert len(markets.calls) == popularity.LOOKUPS_PER_CYCLE
    plan(kube, auto_config(max_in_flight=UNLIMITED), {}, popularity_cache=cache, dry_run=True)
    assert len(markets.calls) == popularity.LOOKUPS_PER_CYCLE + 5  # nothing is asked twice within a day


def test_a_sku_that_does_not_fit_the_usd_budget_is_skipped_and_cheaper_ones_fill_it(kube, keeper_skus, markets, caplog):
    keeper_skus["oci"] = {"dear": {}, "cheap": {}, "mid": {}}
    markets.update(dear=market("w", price=5, regions=9), cheap=market("n", price=0.3), mid=market("m", price=0.5))
    cfg = auto_config(max_in_flight=5, max_usd_per_hour=1.0)
    with caplog.at_level("INFO"):
        planned = plan_wet(kube, keeper_skus, cfg, popularity_cache=popularity_cache())
    assert planned == ["cheap", "mid"]  # as wide as each other: crc32 of the series; "dear" fits neither time
    assert "autoPlan skips oci/dear: its node costs about 5 USD/hour, 1 are left under maxUsdPerHour" in caplog.text


def test_max_in_flight_stops_the_cycle_when_the_slots_are_full(kube, keeper_skus):
    keeper_skus["oci"] = {name: {} for name in "abcd"}
    assert len(plan_wet(kube, keeper_skus, auto_config(max_in_flight=3))) == 3


def test_an_unset_max_in_flight_is_unbounded_up_to_the_burst_cap(kube, keeper_skus):
    keeper_skus["oci"] = {f"s{i}": {} for i in range(12)}
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(
            auto_plan=True,
            vendors={"oci": VendorSettings(enabled=True, publish=True)},
            defaults=Defaults(max_in_flight=UNLIMITED),
        ),
    )
    assert len(plan_wet(kube, keeper_skus, cfg)) == 10  # maxNewRunsPerCycle defaults to 10
    assert len(plan_wet(kube, keeper_skus, cfg)) == 2  # the other two; the first ten hold their runs


def test_max_new_runs_per_cycle_is_a_burst_cap_per_vendor(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {f"s{i}": {} for i in range(6)}
    with caplog.at_level("INFO"):
        assert len(plan_wet(kube, keeper_skus, auto_config(max_in_flight=UNLIMITED, max_new_runs_per_cycle=2))) == 2
    assert "its maxNewRunsPerCycle" in caplog.text


def regional(bucket="B", need=64, limit=256, **regions):
    """A quota entry with `regions` ({region: headroom}); the entry's own headroom is the best region's."""
    return {
        "bucket": bucket,
        "unit": "vCPU",
        "need": need,
        "maxLimit": limit,
        "maxHeadroom": max(regions.values()),
        "regions": [{"region": name, "limit": limit, "headroom": room} for name, room in regions.items()],
    }


def planned_regions(kube):
    return {from_dict(o).spec.sku: tuple(from_dict(o).spec.regions) for o in kube.objects.values()}


@pytest.mark.offered
def test_a_run_is_confined_to_the_regions_where_it_fits(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"s": {}}
    markets["s"] = market(spot={"cheap": 0.1, "mid": 0.2, "dear": 0.3})
    entry = spot_entry(regional(cheap=32, mid=128, dear=512))  # `cheap` has no room for 64 vCPU
    publish_offered(kube, offered_configmap({"s": entry}))
    assert plan_wet(kube, keeper_skus, popularity_cache=popularity_cache()) == ["s"]
    assert planned_regions(kube) == {"s": ("dear", "mid")}  # the set where it fits; Karpenter picks within it


@pytest.mark.offered
def test_regional_consumption_is_counted_per_region_within_a_cycle(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"a": {}, "b": {}, "c": {}, "d": {}}
    for sku in "abcd":
        markets[sku] = market(series=sku, spot={"x": 0.1, "y": 0.2})
    entry = spot_entry(regional(x=128, y=64))
    publish_offered(kube, offered_configmap({sku: entry for sku in "abcd"}))
    assert len(plan_wet(kube, keeper_skus, popularity_cache=popularity_cache())) == 3  # x: 2 runs, y: 1 run
    regions = sorted(planned_regions(kube).values())
    assert regions == [("x", "y"), ("x", "y"), ("y",)]  # both runs count against x, the cheaper: then only y is left


@pytest.mark.offered
def test_a_global_bucket_is_checked_account_wide_next_to_a_regional_one(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"a": {}, "b": {}}
    markets.update(a=market("a", spot={"x": 0.1}), b=market("b", spot={"x": 0.1}))
    global_bucket = {"bucket": "TOTAL", "unit": "vCPU", "need": 64, "maxLimit": 128, "maxHeadroom": 100}
    entry = spot_entry(regional(x=512), global_bucket)  # the account has room for one run only
    publish_offered(kube, offered_configmap({"a": entry, "b": entry}))
    assert len(plan_wet(kube, keeper_skus, popularity_cache=popularity_cache())) == 1


@pytest.mark.offered
def test_without_regions_the_max_headroom_decides_and_nothing_is_pinned(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"s": {}}
    markets["s"] = market(spot={"cheap": 0.1})
    publish_offered(kube, offered_configmap({"s": spot_entry(quota(need=64, headroom=128))}))
    assert plan_wet(kube, keeper_skus, popularity_cache=popularity_cache()) == ["s"]
    assert planned_regions(kube) == {"s": ()}


@pytest.mark.offered
def test_a_sku_that_fits_no_region_is_deferred_as_busy(kube, keeper_skus, markets, caplog):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": spot_entry(regional(x=32, y=16), spot_available=False)}))
    with caplog.at_level("INFO"):
        assert plan_wet(kube, keeper_skus) == []
    assert "autoPlan defers oci/s: spot quota busy (bucket B: need 64 vCPU, headroom 32 in x (and 1 more regions))" in (
        caplog.text
    )


def drain_listing(kube, keeper_skus, markets):
    """`big` needs 200 of 256 vCPU in each region; a run of `mid` holds 64 of region `b`, where spot is cheaper."""
    keeper_skus["oci"] = {"big": {}, "small": {}}
    markets["small"] = market(spot={"a": 0.5, "b": 0.1})
    entries = {
        "big": spot_entry(regional(need=200, a=128, b=128), spot_available=False),
        "small": spot_entry(regional(need=8, a=128, b=128)),
        "mid": spot_entry(regional(need=64, a=128, b=128)),
    }
    publish_offered(kube, offered_configmap(entries))
    planner._create_run(kube, auto_config(), "oci", "mid", [SPOT_TASK], regions=("b",))


@pytest.mark.offered
def test_a_big_sku_waiting_for_quota_asks_to_drain_the_region_its_own_runs_hold(kube, keeper_skus, markets):
    drain_listing(kube, keeper_skus, markets)
    waits = {}
    assert plan_wet(kube, keeper_skus, quota_waits=waits, popularity_cache=popularity_cache()) == ["small"]
    ((_, (since, keys)),) = waits.items()
    assert keys == frozenset({("B", "b")})  # `small` is not big: it never waits


@pytest.mark.offered
def test_a_drained_region_takes_no_other_run_while_the_others_keep_filling(kube, keeper_skus, markets, caplog):
    drain_listing(kube, keeper_skus, markets)
    waits = {("oci", "big"): (datetime.now(UTC) - timedelta(hours=2), frozenset({("B", "b")}))}
    assert plan_wet(kube, keeper_skus, quota_waits=waits, popularity_cache=popularity_cache()) == ["small"]
    assert planned_regions(kube)["small"] == ("a",)  # `b` is cheaper, but reserved for `big`


@pytest.mark.offered
def test_the_drain_ends_when_own_runs_no_longer_hold_the_region(kube, keeper_skus, markets):
    drain_listing(kube, keeper_skus, markets)
    kube.set_status(next(iter(kube.objects)), phase="Done")
    waits = {("oci", "big"): (datetime.now(UTC) - timedelta(hours=2), frozenset({("B", "b")}))}
    plan_wet(kube, keeper_skus, quota_waits=waits, popularity_cache=popularity_cache())
    assert planned_regions(kube)["small"] == ("a", "b")


@pytest.mark.offered
def test_quota_reserve_after_minutes_zero_turns_the_drain_off(kube, keeper_skus, markets):
    drain_listing(kube, keeper_skus, markets)
    waits = {("oci", "big"): (datetime.now(UTC) - timedelta(hours=2), frozenset({("B", "b")}))}
    settings = Settings(
        vendors={"oci": VendorSettings(enabled=True, publish=True, max_in_flight=5)},
        auto_plan=True,
        spot=SpotSettings(quota_reserve_after_minutes=0),
    )
    cfg = Config(EnvConfig(bucket="b", bucket_region="r"), settings)
    plan_wet(kube, keeper_skus, cfg, quota_waits=waits, popularity_cache=popularity_cache())
    assert planned_regions(kube)["small"] == ("a", "b")


def test_unknown_prices_under_a_usd_cap_stop_the_cycle_after_one_run(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"a": {}, "b": {}, "c": {}}
    with caplog.at_level("INFO"):
        assert len(plan_wet(kube, keeper_skus, auto_config(max_in_flight=5, max_usd_per_hour=100.0))) == 1
    assert "no price estimate for" in caplog.text
    assert len(plan_wet(kube, keeper_skus, auto_config(max_in_flight=5))) == 2  # no cap: prices do not matter


@pytest.mark.offered
def test_a_spot_run_is_budgeted_at_the_dearer_of_spot_and_on_demand(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"s": {}}
    markets["s"] = {**market(), "ondemand": {"x": 2.0}, "spot": {"x": 0.1}}
    publish_offered(kube, offered_configmap({"s": spot_entry(regional(x=512))}))
    cfg = auto_config(max_in_flight=5, max_usd_per_hour=1.0)
    assert plan_wet(kube, keeper_skus, cfg, popularity_cache=popularity_cache()) == []  # spot may fall back: 2 > 1


@pytest.mark.offered
def test_without_prices_the_run_is_confined_to_every_fitting_region(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"s": {}}
    publish_offered(kube, offered_configmap({"s": spot_entry(regional(b=128, a=128, c=8))}))
    assert plan_wet(kube, keeper_skus, popularity_cache=popularity_cache()) == ["s"]
    assert planned_regions(kube) == {"s": ("a", "b")}  # not the first region alone


@pytest.mark.offered
def test_a_region_whose_offering_is_unavailable_cannot_starve_the_sku(kube, keeper_skus, markets):
    keeper_skus["oci"] = {"s": {}}
    markets["s"] = market(spot={"ice": 0.1, "ok": 0.2})
    publish_offered(kube, offered_configmap({"s": spot_entry(regional(ice=128, ok=128))}))
    plan_wet(kube, keeper_skus, popularity_cache=popularity_cache())
    assert planned_regions(kube) == {"s": ("ice", "ok")}  # the cheapest region is not the only one the anchor accepts


@pytest.mark.offered
def test_zones_that_name_regions_narrow_the_set(kube, keeper_skus):
    keeper_skus["oci"] = {"s": {}}
    entry = spot_entry(regional(eu_1=128, eu_2=128))
    entry["offerings"][0]["zones"] = ["eu_2-a"]  # only eu_2 has a zone
    publish_offered(kube, offered_configmap({"s": entry}))
    plan_wet(kube, keeper_skus)
    assert planned_regions(kube) == {"s": ("eu_2",)}


@pytest.mark.offered
def test_a_big_sku_keeps_its_wait_timer_when_its_region_changes(kube, keeper_skus):
    keeper_skus["oci"] = {"big": {}}
    since = datetime.now(UTC) - timedelta(hours=2)
    waits = {("oci", "big"): (since, frozenset({("B", "gone")}))}  # it waited for a region that left the listing
    publish_offered(
        kube, offered_configmap({"big": spot_entry(regional(need=200, a=128, b=100), spot_available=False)})
    )
    assert plan_wet(kube, keeper_skus, quota_waits=waits) == []
    ((_, (kept, keys)),) = waits.items()
    assert kept == since and keys == frozenset({("B", "a")})
    publish_offered(kube, offered_configmap({"big": spot_entry(regional(need=100, a=64, b=50), spot_available=False)}))
    plan_wet(kube, keeper_skus, quota_waits=waits)  # not big any more: no drain, the timer still goes on
    assert waits[("oci", "big")][0] == since


@pytest.mark.offered
def test_zones_of_all_available_groups_count_not_the_last_group(kube, keeper_skus):
    keeper_skus["oci"] = {"s": {}}
    entry = spot_entry(regional(eu_1=128, eu_2=128, eu_3=128))
    spot = entry["offerings"][0]
    spot["zones"] = ["eu_1a", "eu_2b"]  # the available group
    unavailable = {"capacityType": "spot", "zones": ["eu_3c"], "available": False}
    entry["offerings"].append(unavailable)  # listed last, as SCarpenter lists the unavailable groups
    publish_offered(kube, offered_configmap({"s": entry}))
    plan_wet(kube, keeper_skus)
    assert planned_regions(kube) == {"s": ("eu_1", "eu_2")}


@pytest.mark.parametrize(
    ("zone", "region", "belongs"),
    [
        ("us-east-1a", "us-east-1", True),
        ("europe-west1-b", "europe-west1", True),
        ("fi-hel1", "fi-hel1", True),
        ("europe-west10-a", "europe-west1", False),
        ("us-east-1-bos-1a", "us-east-1", False),
        ("1", "eastus", False),
    ],
)
def test_a_zone_belongs_to_a_region_by_name(zone, region, belongs):
    assert planner._in_region(zone, region) is belongs
