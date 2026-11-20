import copy
import threading
from datetime import timedelta

import pytest

from k8s_inspector import keeper, planner
from k8s_inspector.crd import RunExists
from k8s_inspector.kube import AlreadyExists, NotFound
from k8s_inspector.model import (
    CapacityMode,
    Config,
    Defaults,
    EnvConfig,
    NodePools,
    RunPhase,
    Settings,
    VendorSettings,
)
from k8s_inspector.tasks import all_tasks


class FakeKube:
    """In-memory BenchmarkRun store: just what `crd.create_run`/`list_runs` use."""

    namespace = "inspector"

    def __init__(self) -> None:
        self.objects: dict[str, dict] = {}
        self.pods: list[dict] = []
        self.nodeclaims: list[dict] = []

    def create(self, kind, manifest):
        name = manifest["metadata"]["name"]
        if name in self.objects:
            raise AlreadyExists(name)
        obj = copy.deepcopy(manifest)
        obj["metadata"].update(uid=f"uid-{name}", resourceVersion="1", creationTimestamp="2026-11-20T09:30:00Z")
        self.objects[name] = obj
        return copy.deepcopy(obj)

    def get(self, kind, name):
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
    assert run.spec.tasks == [t.name for t in all_tasks() if not t.dbaas]
    assert run.spec.run_id.startswith("r")
    assert run.name in kube.objects


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
    cfg = make_config(max_in_flight=2, node_pools=NodePools(cpu="bench-oci", lowmem="bench-oci-lowmem"))
    claim = lambda pool: {"metadata": {"labels": {"karpenter.sh/nodepool": pool}}}  # noqa: E731
    kube.nodeclaims = [claim("bench-oci"), claim("bench-oci-lowmem"), claim("other-pool")]
    with pytest.raises(ValueError, match="2 of 2"):  # no run holds an anchor, two instances are still up
        planner.force_run(kube, cfg, "oci", "a")

    kube.nodeclaims = kube.nodeclaims[1:]
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


def test_plan_cycle_is_a_stub_that_warns_about_auto_plan(kube, caplog):
    cfg = Config(
        EnvConfig(bucket="b", bucket_region="r"),
        Settings(auto_plan=True, vendors={"oci": VendorSettings(enabled=True)}),
    )
    assert planner.plan_cycle(kube, cfg) == []
    assert "autoPlan is not implemented" in caplog.text
    assert planner.plan_cycle(kube, make_config(), dry_run=True) == []


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
    planner.run_forever(FakeKube(), provider, stop)
    assert len(calls) == 2
