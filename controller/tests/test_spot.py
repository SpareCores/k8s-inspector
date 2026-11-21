import json
from datetime import UTC, datetime, timedelta

import pytest

from k8s_inspector import spot
from k8s_inspector.kube import Conflict, NotFound
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    BenchmarkRun,
    CapacityMode,
    CapacityType,
    NodeFacts,
    RunSpec,
    RunStatus,
    Settings,
    TaskDef,
    TaskState,
    TaskStatus,
)

NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
SPOT_OD = [CapacityType.SPOT, CapacityType.ON_DEMAND]
OD = [CapacityType.ON_DEMAND]


@pytest.fixture(autouse=True)
def fake_allows_spot(monkeypatch):
    monkeypatch.setattr(
        spot, "allows_spot", lambda tasks, limit: all(t.spot and t.timeout <= timedelta(minutes=limit) for t in tasks)
    )


def task(**kw) -> TaskDef:
    return TaskDef(**({"name": "t", "image": "i", "command": None, "timeout": timedelta(minutes=30)} | kw))


def run(attempts=(), tasks=(), capacity=CapacityMode.SPOT_FIRST) -> BenchmarkRun:
    return BenchmarkRun(
        name="r",
        namespace="inspector",
        uid="u",
        resource_version="1",
        created=NOW,
        spec=RunSpec(vendor="oci", sku="VM.X", run_id="r1", capacity=capacity),
        status=RunStatus(attempts=list(attempts), tasks=list(tasks)),
    )


def attempt(n, start_min, end_min, outcome=AttemptOutcome.SPOT_INTERRUPTED) -> AttemptStatus:
    base = NOW - timedelta(hours=10)
    return AttemptStatus(
        n,
        CapacityType.SPOT,
        start=base + timedelta(minutes=start_min),
        end=base + timedelta(minutes=end_min),
        outcome=outcome,
    )


def capacity(r, tasks=None, quarantined=False, settings=None):
    return spot.capacity_for_next_attempt(r, tasks or [task()], settings or Settings(), quarantined)


def test_spot_first_by_default():
    assert capacity(run()) == SPOT_OD


def test_on_demand_rules():
    assert capacity(run(capacity=CapacityMode.ON_DEMAND)) == OD
    assert capacity(run(), quarantined=True) == OD
    assert capacity(run(), tasks=[task(spot=False)]) == OD
    assert capacity(run(), tasks=[task(timeout=timedelta(hours=26))]) == OD


def test_attempt_budget():
    one = run([attempt(1, 0, 10)])
    assert capacity(one) == SPOT_OD
    two = run([attempt(1, 0, 10), attempt(2, 20, 30)])
    assert capacity(two) == OD
    # Other outcomes do not count.
    assert (
        capacity(run([attempt(1, 0, 10, AttemptOutcome.NODE_LOST), attempt(2, 0, 10, AttemptOutcome.COMPLETED)]))
        == SPOT_OD
    )


def test_lost_minutes_budget():
    # one attempt, no finished task: 61 min lost > 60
    assert capacity(run([attempt(1, 0, 61)])) == OD
    assert capacity(run([attempt(1, 0, 60)])) == SPOT_OD
    # a task finished at minute 100 of a 120 min attempt: 20 min lost
    done = TaskStatus("a", TaskState.DONE, attempt=1, end=NOW - timedelta(hours=10) + timedelta(minutes=100))
    assert capacity(run([attempt(1, 0, 120)], [done])) == SPOT_OD
    assert capacity(run([attempt(1, 0, 120)])) == OD


def test_classify_node_loss():
    r = run()
    claim = {"metadata": {"annotations": {"scarpenter.sparecores.com/interruption": "SpotInterrupted"}}}
    assert spot.classify_node_loss(r, None, claim) == AttemptOutcome.SPOT_INTERRUPTED
    cond = {"status": {"conditions": [{"type": "Interrupted", "status": "True"}]}}
    assert spot.classify_node_loss(r, None, cond) == AttemptOutcome.SPOT_INTERRUPTED
    off = {"status": {"conditions": [{"type": "Interrupted", "status": "False"}]}}
    assert spot.classify_node_loss(r, None, off) == AttemptOutcome.NODE_LOST
    facts = NodeFacts("n", "oci.x", "amd64", NOW, 1, capacity_type=CapacityType.SPOT)
    assert spot.classify_node_loss(r, facts, None) == AttemptOutcome.SPOT_INTERRUPTED
    facts.capacity_type = CapacityType.ON_DEMAND
    assert spot.classify_node_loss(r, facts, {}) == AttemptOutcome.NODE_LOST
    r.status.capacity_type = CapacityType.SPOT
    assert spot.classify_node_loss(r, None, None) == AttemptOutcome.SPOT_INTERRUPTED
    assert spot.classify_node_loss(run(), None, None) == AttemptOutcome.NODE_LOST


class FakeKube:
    def __init__(self, data=None, conflicts=0, fail=False):
        self.cm = None if data is None else {"metadata": {"resourceVersion": "5"}, "data": data}
        self.conflicts, self.fail, self.patches = conflicts, fail, []

    def get(self, kind, name):
        if self.fail:
            raise RuntimeError("api down")
        if self.cm is None:
            raise NotFound(name)
        return self.cm

    def create(self, kind, manifest):
        self.cm = {"metadata": {"resourceVersion": "1"}, "data": manifest["data"]}

    def merge_patch(self, kind, name, body, subresource_status=False):
        if self.conflicts:
            self.conflicts -= 1
            raise Conflict("stale")
        self.patches.append(body)
        self.cm["data"].update(body["data"])


KEY = "oci.VM.X"


def times(*days_ago):
    return json.dumps([(NOW - timedelta(days=d)).isoformat() for d in days_ago])


def test_is_quarantined():
    s = Settings()  # 2 interruptions within 14 days
    assert not spot.is_quarantined(FakeKube(), "oci", "VM.X", s, NOW)
    assert not spot.is_quarantined(FakeKube({KEY: times(1)}), "oci", "VM.X", s, NOW)
    assert spot.is_quarantined(FakeKube({KEY: times(1, 13)}), "oci", "VM.X", s, NOW)
    assert not spot.is_quarantined(FakeKube({KEY: times(1, 15)}), "oci", "VM.X", s, NOW)
    assert not spot.is_quarantined(FakeKube({"oci.other": times(1, 2)}), "oci", "VM.X", s, NOW)
    assert not spot.is_quarantined(FakeKube({KEY: "garbage"}), "oci", "VM.X", s, NOW)
    assert not spot.is_quarantined(FakeKube(fail=True), "oci", "VM.X", s, NOW)


def test_record_interruption_creates_and_appends():
    kube = FakeKube()
    spot.record_interruption(kube, "oci", "VM.X", NOW)
    assert json.loads(kube.cm["data"][KEY]) == [NOW.isoformat()]
    spot.record_interruption(kube, "oci", "VM.X", NOW + timedelta(days=1))
    assert len(json.loads(kube.cm["data"][KEY])) == 2
    assert kube.patches[0]["metadata"]["resourceVersion"] == "1"


def test_record_interruption_prunes_old_and_retries_conflicts():
    kube = FakeKube({KEY: times(200, 3)}, conflicts=2)
    spot.record_interruption(kube, "oci", "VM.X", NOW)
    assert len(json.loads(kube.cm["data"][KEY])) == 2  # the 200 day old entry is dropped


def test_record_interruption_never_raises():
    spot.record_interruption(FakeKube(fail=True), "oci", "VM.X", NOW)
    spot.record_interruption(FakeKube({KEY: "[]"}, conflicts=10), "oci", "VM.X", NOW)


def test_key_is_configmap_safe():
    assert spot._key("aws", "Standard D2/v3") == "aws.Standard_D2_v3"


def test_a_failed_provisioning_attempt_makes_the_next_one_on_demand_only():
    failed = attempt(1, 0, 5, outcome=AttemptOutcome.PROVISIONING_FAILED)
    assert spot.capacity_for_next_attempt(run([failed]), [task(spot=True)], Settings(), False) == OD
    assert spot.capacity_for_next_attempt(run([]), [task(spot=True)], Settings(), False) == SPOT_OD
