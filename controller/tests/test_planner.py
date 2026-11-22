import copy
import json
import threading
import zlib
from datetime import UTC, datetime, timedelta, timezone

import pytest

from k8s_inspector import crd, keeper, launch, planner
from k8s_inspector.crd import RunExists, from_dict
from k8s_inspector.kube import AlreadyExists, NotFound
from k8s_inspector.model import (
    CapacityMode,
    Config,
    Defaults,
    EnvConfig,
    RunPhase,
    RunSpec,
    Settings,
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


def test_max_in_flight_counts_active_runs_only(kube):
    cfg = make_config(max_in_flight=2)
    first = planner.force_run(kube, cfg, "oci", "a")
    second = planner.force_run(kube, cfg, "oci", "b")
    with pytest.raises(ValueError, match="maxInFlight"):
        planner.force_run(kube, cfg, "oci", "c")

    kube.set_status(first.name, phase="Done")
    kube.set_status(second.name, phase="Cancelled")
    planner.force_run(kube, cfg, "oci", "c")


def test_max_in_flight_counts_failed_and_cancelled_runs_until_anchor_is_gone(kube):
    cfg = make_config(max_in_flight=2)
    first = planner.force_run(kube, cfg, "oci", "a")
    second = planner.force_run(kube, cfg, "oci", "b")
    kube.set_status(first.name, phase="Failed")
    kube.set_status(second.name, phase="Cancelled")
    kube.pods = [
        {"metadata": {"labels": {"inspector.sparecores.com/run": name, "inspector.sparecores.com/role": "anchor"}}}
        for name in (first.name, second.name)
    ]
    with pytest.raises(ValueError, match="2 of 2"):
        planner.force_run(kube, cfg, "oci", "c")

    kube.pods = kube.pods[:1]  # the runner deleted the second anchor
    planner.force_run(kube, cfg, "oci", "c")


def test_max_in_flight_zero_pauses_vendor(kube):
    with pytest.raises(ValueError, match="maxInFlight"):
        planner.force_run(kube, make_config(max_in_flight=0), "oci", "sku")


def test_max_in_flight_falls_back_to_defaults(kube):
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(vendors={"oci": VendorSettings(enabled=True)}, defaults=Defaults(max_in_flight=1)),
    )
    planner.force_run(kube, cfg, "oci", "a")
    with pytest.raises(ValueError, match="1 of 1"):
        planner.force_run(kube, cfg, "oci", "b")


def test_max_usd_per_hour(kube):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a")
    kube.set_status(first.name, phase="Running", pricePerHour="0.6")
    planner.force_run(kube, cfg, "oci", "b")  # 0.6 < 1.0

    kube.set_status(first.name, pricePerHour="1.2")
    with pytest.raises(ValueError, match="maxUsdPerHour"):
        planner.force_run(kube, cfg, "oci", "c")


def test_max_usd_per_hour_waits_for_runs_without_price(kube):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a")
    with pytest.raises(ValueError, match=f"wait until {first.name} has a price"):
        planner.force_run(kube, cfg, "oci", "b")

    no_cap = make_config(max_in_flight=5)
    planner.force_run(kube, no_cap, "oci", "b")  # no cap: prices do not matter


def test_max_usd_per_hour_refuses_a_running_run_without_price(kube, caplog):
    cfg = make_config(max_in_flight=5, max_usd_per_hour=1.0)
    first = planner.force_run(kube, cfg, "oci", "a")
    kube.set_status(first.name, phase="Running", node="n1")
    with caplog.at_level("WARNING"), pytest.raises(ValueError, match=rf"node of {first.name} has no price annotation"):
        planner.force_run(kube, cfg, "oci", "b")
    assert [r.message for r in caplog.records if first.name in r.message]


def test_max_in_flight_counts_live_nodeclaims_beyond_runs(kube):
    cfg = make_config(max_in_flight=2)
    claim = lambda pool, vendor: {  # noqa: E731
        "metadata": {"labels": {"karpenter.sh/nodepool": pool, "scarpenter.sparecores.com/vendor": vendor}}
    }
    # Two OCI claims in the shared bench pool count; another vendor's claim and another pool's claim do not.
    kube.nodeclaims = [claim("bench", "oci"), claim("bench", "oci"), claim("bench", "aws"), claim("other-pool", "oci")]
    with pytest.raises(ValueError, match="2 of 2"):  # no run holds an anchor, two instances are still up
        planner.force_run(kube, cfg, "oci", "a")

    kube.nodeclaims = kube.nodeclaims[1:]
    planner.force_run(kube, cfg, "oci", "a")


def test_unlaunched_nodeclaims_count_by_spec_requirements(kube):
    cfg = make_config(max_in_flight=1)
    pool = {"karpenter.sh/nodepool": "bench"}
    requirements = [{"key": "scarpenter.sparecores.com/vendor", "operator": "In", "values": ["oci"]}]
    kube.nodeclaims = [{"metadata": {"labels": pool}, "spec": {"requirements": requirements}}]
    with pytest.raises(ValueError, match="1 of 1"):
        planner.force_run(kube, cfg, "oci", "a")
    kube.nodeclaims = [
        {"metadata": {"labels": pool}, "spec": {"requirements": [{**requirements[0], "values": ["aws"]}]}}
    ]
    planner.force_run(kube, cfg, "oci", "a")


def test_other_vendors_do_not_count(kube):
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(
            vendors={"oci": VendorSettings(enabled=True, max_in_flight=1), "aws": VendorSettings(enabled=True)},
        ),
    )
    planner.force_run(kube, cfg, "aws", "a")
    planner.force_run(kube, cfg, "oci", "a")


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


def offered_configmap(types: dict, *, age=timedelta(minutes=1), truncated=False, naive_time=False) -> dict:
    """ConfigMap `scarpenter-offered-bench-oci` as SCarpenter writes it; `types` is {sku: entry fields}."""
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
    return {"data": {"types.json": json.dumps(document)}}


def publish_offered(kube, configmap: dict) -> None:
    kube.foreign_configmaps["scarpenter-system", "scarpenter-offered-bench-oci"] = configmap


OFFERED = {"available": True, "capacityTypes": ["on-demand", "spot"]}
QUOTA = {"available": False, "reason": "QuotaGate", "detail": "headroom 6, need 16"}


def planned_skus(kube, cfg=None) -> list[str]:
    return [p.sku for p in plan(kube, cfg or auto_config(), {}, dry_run=True)]


def test_offered_types_skip_unavailable_skus(kube, keeper_skus, caplog):
    keeper_skus["oci"] = {"quota": {}, "ok": {}}
    publish_offered(kube, offered_configmap({"quota": QUOTA, "ok": OFFERED}))
    with caplog.at_level("INFO"):
        assert planned_skus(kube) == ["ok"]
    assert "autoPlan skips 1 SKUs of vendor oci not offered by SCarpenter, e.g. quota (QuotaGate)" in caplog.text


def test_offered_types_skip_a_sku_without_an_entry(kube, keeper_skus):
    keeper_skus["oci"] = {"unknown": {}, "ok": {}}
    publish_offered(kube, offered_configmap({"ok": OFFERED}))
    assert planned_skus(kube) == ["ok"]


def test_offered_types_on_demand_mode_needs_an_on_demand_offering(kube, keeper_skus):
    keeper_skus["oci"] = {"spot-only": {}}
    publish_offered(kube, offered_configmap({"spot-only": {"available": True, "capacityTypes": ["spot"]}}))
    assert planned_skus(kube, auto_config(capacity=CapacityMode.ON_DEMAND)) == []
    assert planned_skus(kube, auto_config(capacity=CapacityMode.SPOT_FIRST)) == ["spot-only"]


@pytest.mark.parametrize(
    "configmap",
    [
        None,  # vendor not opted in
        offered_configmap({"s1": QUOTA}, age=timedelta(minutes=31)),  # stale
        {"data": {"types.json": "{not json"}},
        {"data": {}},
        {"data": {"types.json": json.dumps({"generatedAt": "now", "truncated": False, "types": {}})}},
        offered_configmap({"s1": QUOTA}, naive_time=True),
        offered_configmap({"s1": {"available": True, "capacityTypes": None}}),
        offered_configmap({"s1": {"available": True, "capacityTypes": ["spot"], "offerings": None}}),
    ],
    ids=["absent", "stale", "bad-json", "no-key", "bad-time", "naive-time", "null-capacity-types", "null-offerings"],
)
def test_offered_types_unusable_configmap_filters_nothing(kube, keeper_skus, caplog, configmap):
    if configmap:
        publish_offered(kube, configmap)
    with caplog.at_level("DEBUG"):
        assert planned_skus(kube) == ["s1"]
    assert "offered types of vendor oci unusable" in caplog.text


def test_offered_types_truncated_list_skips_only_listed_unavailable(kube, keeper_skus):
    keeper_skus["oci"] = {"quota": {}, "unknown": {}}
    publish_offered(kube, offered_configmap({"quota": QUOTA}, truncated=True))
    assert planned_skus(kube) == ["unknown"]


def test_force_run_ignores_offered_types(kube):
    publish_offered(kube, offered_configmap({"s1": QUOTA}))
    run = planner.force_run(kube, make_config(), "oci", "s1", force=True)
    assert run.spec.sku == "s1"


def test_offered_types_accept_an_offset_time_and_ignore_entries_without_api_reference(kube, keeper_skus):
    keeper_skus["oci"] = {"quota": {}, "ok": {}}
    configmap = offered_configmap({"quota": QUOTA, "ok": OFFERED})
    document = json.loads(configmap["data"]["types.json"])
    document["generatedAt"] = datetime.now(timezone(timedelta(hours=1))).isoformat()
    document["types"]["oci.no-label-value"] = {"available": False, "capacityTypes": [], "reason": "NoPrice"}
    publish_offered(kube, {"data": {"types.json": json.dumps(document)}})
    assert planned_skus(kube) == ["ok"]


def test_offered_types_duplicate_api_reference_prefers_the_available_entry(kube, keeper_skus):
    configmap = offered_configmap({"s1": OFFERED})
    document = json.loads(configmap["data"]["types.json"])
    document["types"]["oci.s1-twin"] = {"apiReference": "s1", **QUOTA, "capacityTypes": []}
    for types in (document["types"], dict(reversed(document["types"].items()))):
        publish_offered(kube, {"data": {"types.json": json.dumps({**document, "types": types})}})
        assert planned_skus(kube) == ["s1"]


@pytest.mark.parametrize("capacity", [CapacityMode.SPOT_FIRST, CapacityMode.ON_DEMAND])
def test_offered_types_available_without_capacity_types_is_skipped(kube, keeper_skus, capacity):
    publish_offered(kube, offered_configmap({"s1": {"available": True, "capacityTypes": []}}))
    assert planned_skus(kube, auto_config(capacity=capacity)) == []


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


def test_a_marked_sku_is_skipped_whatever_its_until(kube, keeper_skus, caplog):
    for until in ("2099-01-01T00:00:00Z", "2000-01-01T00:00:00Z", "tomorrow"):  # SCarpenter drops an expired mark
        offerings = [{**o, "until": until} if "until" in o else o for o in MARKED["offerings"]]
        publish_offered(kube, offered_configmap({"s1": {**MARKED, "offerings": offerings}}))
        with caplog.at_level("INFO"):
            assert planned_skus(kube) == []
        assert "e.g. s1 (UnavailableCache)" in caplog.text


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


def test_offered_until_judges_staleness_by_the_given_time(kube):
    cfg = make_config()
    publish_offered(kube, offered_configmap({"s1": MARKED}))
    later = datetime.now(UTC) + timedelta(hours=1)
    assert planner.offered_until(kube, cfg, "oci", "s1", later) is None
    assert planner.offered_until(kube, cfg, "oci", "s1") is not None


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
