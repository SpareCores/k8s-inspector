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
    SpotSettings,
    TaskDef,
    TaskState,
    TaskStatus,
    VendorSettings,
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
        for key, value in body["data"].items():
            if value is None:  # a merge patch removes the key
                self.cm["data"].pop(key, None)
            else:
                self.cm["data"][key] = value


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


def test_a_launch_failed_attempt_makes_the_next_one_on_demand_only():
    failed = attempt(1, 0, 5, outcome=AttemptOutcome.LAUNCH_FAILED)
    assert spot.capacity_for_next_attempt(run([failed]), [task(spot=True)], Settings(), False) == OD
    assert spot.capacity_for_next_attempt(run([]), [task(spot=True)], Settings(), False) == SPOT_OD


def test_other_lost_provisioning_attempts_keep_spot():
    lost = [attempt(n, 0, 5, outcome=AttemptOutcome.PROVISIONING_FAILED) for n in (1, 2)]
    assert spot.capacity_for_next_attempt(run(lost), [task(spot=True)], Settings(), False) == SPOT_OD


# --------------------------------------------------------------------------- SpotOnly and SpotUntilExhausted


def test_spot_only_never_allows_on_demand():
    r = run(capacity=CapacityMode.SPOT_ONLY)
    assert capacity(r) == [CapacityType.SPOT]
    assert capacity(r, quarantined=True) == [CapacityType.SPOT]
    assert capacity(run([attempt(1, 0, 10), attempt(2, 20, 30)], capacity=CapacityMode.SPOT_ONLY)) == [
        CapacityType.SPOT
    ]
    launch_failed = run([attempt(1, 0, 10, AttemptOutcome.LAUNCH_FAILED)], capacity=CapacityMode.SPOT_ONLY)
    assert capacity(launch_failed) == [CapacityType.SPOT]


def test_spot_until_exhausted_is_spot_only_until_released():
    r = run(capacity=CapacityMode.SPOT_UNTIL_EXHAUSTED)
    spot_only = [CapacityType.SPOT]
    assert capacity(r) == spot_only
    assert spot.capacity_for_next_attempt(r, [task()], Settings(), False, spot_released=True) == SPOT_OD


def test_spot_until_exhausted_goes_on_demand_by_the_spot_first_rules():
    r = capacity(run(capacity=CapacityMode.SPOT_UNTIL_EXHAUSTED, attempts=[attempt(1, 0, 10), attempt(2, 20, 30)]))
    assert r == OD  # maxSpotAttempts interruptions
    assert capacity(run(capacity=CapacityMode.SPOT_UNTIL_EXHAUSTED), quarantined=True) == OD
    assert capacity(run(capacity=CapacityMode.SPOT_UNTIL_EXHAUSTED), tasks=[task(spot=False)]) == OD


def entry(*, offered=("on-demand", "spot"), spot_quota=True, never_fits=False):
    quota = {"bucket": "B", "unit": "vCPU", "need": 64, "maxLimit": 32 if never_fits else 384, "maxHeadroom": 128}
    quotas = {"spot": [quota]} if spot_quota and "spot" in offered else {}
    return {"available": True, "capacityTypes": list(offered), "offered_types": set(offered), "quotas": quotas}


def test_spot_impossible_comes_from_the_offerings_not_the_quota_groups():
    assert spot.spot_impossible(entry()) is None
    assert spot.spot_impossible(entry(spot_quota=False)) is None  # an available spot offering without quotas
    assert spot.spot_impossible(entry(offered=("on-demand",))) == "no spot offering"
    assert spot.spot_impossible(entry(never_fits=True)) == "spot quota can never fit"
    assert spot.spot_impossible(None) is None  # nothing known
    assert spot.spot_impossible(entry(offered=())) is None  # an entry without offering groups says nothing


def test_quota_busy_uses_the_regions_the_run_is_confined_to():
    entry_ = {
        "need": 64,
        "maxHeadroom": 128,
        "regions": {"a": {"limit": 100, "headroom": 10}, "b": {"limit": 100, "headroom": 80}},
    }
    assert not spot.quota_busy([{"need": 64, "maxHeadroom": 64}])
    assert spot.quota_busy([{"need": 64, "maxHeadroom": 63}])
    assert not spot.quota_busy([])
    assert not spot.quota_busy([entry_])  # region b has room
    assert spot.quota_busy([entry_], ("a",))  # the run may only land in a
    assert not spot.quota_busy([entry_], ("a", "b"))
    assert not spot.quota_busy([entry_], ("elsewhere",))  # no regional fact for it: maxHeadroom decides


S = Settings()  # 3 hour slots: 56 per week, 14 days patience, coverage 100 %
MONDAY = datetime(2026, 11, 23, 0, 30, tzinfo=UTC)  # a Monday, slot (0, 0)
TRIES = "tries--oci.VM.X"


def tries_of(*times: datetime, settings=S) -> spot.Tries:
    return spot.Tries(min(times), max(times), len(times), frozenset(spot._slot(t, settings) for t in times))


def week_of_tries(start: datetime, slots: int = 56) -> spot.Tries:
    return tries_of(*(start + timedelta(hours=3 * i) for i in range(slots)))


def test_slot_arithmetic():
    assert spot.slots_per_week(S) == 56
    assert spot.slots_per_week(Settings(spot=SpotSettings(slot_hours=5))) == 7 * 5
    assert len(tries_of(MONDAY, MONDAY + timedelta(hours=2, minutes=29), MONDAY + timedelta(hours=3)).slots) == 2
    assert len(tries_of(MONDAY, MONDAY + timedelta(days=7)).slots) == 1  # the same weekday and hour a week later


def test_unavailable_bound_and_the_log_text():
    assert spot.unavailable_bound(41) == pytest.approx(0.0705, abs=1e-3)
    tries = tries_of(*(MONDAY + timedelta(hours=3 * i) for i in range(41)))
    assert spot.describe_tries("x8i.96xlarge", tries, S) == "x8i.96xlarge: 41/56 slots tried, spot available <7% (95%)"


def test_exhausted_needs_patience_and_coverage():
    start = MONDAY
    full = week_of_tries(start)
    assert not spot.is_exhausted(None, S, NOW)
    # every slot covered but the first try is 8 days old: patience (14 days) is not over
    assert not spot.is_exhausted(full, S, start + timedelta(days=8))
    assert spot.is_exhausted(full, S, start + timedelta(days=14))
    # patience is over but 55 of 56 slots are covered
    assert not spot.is_exhausted(week_of_tries(start, 55), S, start + timedelta(days=20))
    relaxed = Settings(spot=SpotSettings(coverage=95))
    assert spot.is_exhausted(week_of_tries(start, 55), relaxed, start + timedelta(days=20))
    shorter = Settings(spot=SpotSettings(patience_days=7))
    assert spot.is_exhausted(full, shorter, start + timedelta(days=7))


def test_try_due_targets_slots_not_tried_yet():
    assert spot.try_due(None, S, MONDAY)
    tries = tries_of(MONDAY)
    assert not spot.try_due(tries, S, MONDAY + timedelta(hours=2))  # the same slot
    assert spot.try_due(tries, S, MONDAY + timedelta(hours=3))  # the next one
    assert not spot.try_due(tries, S, MONDAY + timedelta(days=7, hours=1))  # a week later: the slot was tried
    assert spot.try_due(tries, S, MONDAY + timedelta(days=1))


def test_try_due_once_every_slot_has_a_try_is_once_a_day():
    tries = week_of_tries(MONDAY)  # the last try is at the start of Sunday's last slot, 21:30 + 0:30 offset
    assert not spot.try_due(tries, S, tries.last + timedelta(hours=23))
    assert spot.try_due(tries, S, tries.last + timedelta(days=1))


def test_record_try_keeps_a_compact_value_and_clear_removes_the_keys():
    kube = FakeKube()
    first = spot.record_try(kube, "oci", "VM.X", MONDAY, S)
    assert (first.count, first.first, first.last) == (1, MONDAY, MONDAY)
    later = MONDAY + timedelta(days=1, hours=7)
    second = spot.record_try(kube, "oci", "VM.X", later, S)
    assert (second.count, second.first, second.last, len(second.slots)) == (2, MONDAY, later, 2)
    value = json.loads(kube.cm["data"][TRIES])  # next to the interruptions, in the same ConfigMap
    assert set(value) == {"first", "last", "count", "slots", "slot_hours"} and len(value["slots"]) == 2
    spot.record_rejected(kube, "oci", "VM.X", NOW)
    spot.clear_tries(kube, "oci", "VM.X")
    assert kube.cm["data"] == {}  # the keys are gone, not emptied
    spot.clear_tries(FakeKube(), "oci", "VM.X")  # no ConfigMap: nothing created
    spot.clear_tries(FakeKube(fail=True), "oci", "VM.X")  # never raises


def test_a_stale_or_damaged_value_counts_as_no_tries_and_the_next_try_replaces_it():
    old = NOW - timedelta(days=2 * S.spot.patience_days + 1)
    kube = FakeKube()
    spot.record_try(kube, "oci", "VM.X", old, S)
    assert spot.State.load(kube).tries("oci", "VM.X", S, NOW) is None  # pruned on read
    assert spot.record_try(kube, "oci", "VM.X", NOW, S).count == 1
    for raw in ("garbage", "{}", '{"first": 1}'):
        assert spot.State({TRIES: raw}).tries("oci", "VM.X", S, NOW) is None
    other_slots = Settings(spot=SpotSettings(slot_hours=6))
    assert spot.State(kube.cm["data"]).tries("oci", "VM.X", other_slots, NOW) is None  # made with other slots


def test_a_try_that_cannot_be_written_is_not_counted():
    assert spot.record_try(FakeKube(fail=True), "oci", "VM.X", NOW, S) is None
    assert spot.record_try(FakeKube({}, conflicts=10), "oci", "VM.X", NOW, S) is None


def test_tries_do_not_count_as_interruptions():
    kube = FakeKube(
        {
            TRIES: json.dumps(
                {"first": NOW.isoformat(), "last": NOW.isoformat(), "count": 9, "slots": [], "slot_hours": 3}
            )
        }
    )
    assert not spot.is_quarantined(kube, "oci", "VM.X", S, NOW)


def test_state_tries_of_vendor():
    kube = FakeKube()
    spot.record_try(kube, "oci", "VM.X", NOW, S)
    spot.record_try(kube, "oci", "VM.Y", NOW, S)
    spot.record_try(kube, "aws", "z", NOW, S)
    found = spot.State.load(kube).tries_of_vendor("oci", S, NOW)
    assert sorted(found) == ["VM.X", "VM.Y"]


def test_rejected_marker_expires_after_its_ttl():
    kube = FakeKube()
    assert spot.record_rejected(kube, "oci", "VM.X", NOW)
    state = spot.State.load(kube)
    assert state.rejected("oci", "VM.X", NOW + timedelta(days=29)) and not state.rejected("oci", "VM.Y", NOW)
    assert not state.rejected("oci", "VM.X", NOW + timedelta(days=31))


def vendors(mode):
    return Settings(vendors={"oci": VendorSettings(enabled=True, capacity=mode)})


def skip(mode, state, entry_=None, tasks=("lscpu",), now=NOW):
    return spot.planning_skip(state, vendors(mode), "oci", "VM.X", list(tasks), entry_ or entry(), now)


def test_planning_skip_of_the_other_modes_is_none():
    assert skip(CapacityMode.SPOT_FIRST, None) is None
    assert skip(CapacityMode.ON_DEMAND, spot.State({})) is None


def test_planning_skip_fails_closed_when_the_state_is_unreadable():
    for mode in (CapacityMode.SPOT_ONLY, CapacityMode.SPOT_UNTIL_EXHAUSTED):
        assert "unreadable" in skip(mode, spot.State.load(FakeKube(fail=True)))


def test_planning_skip_of_spot_only():
    mode = CapacityMode.SPOT_ONLY
    assert skip(mode, spot.State({})) is None
    assert skip(mode, spot.State({}), entry(offered=("on-demand",))) == "no spot offering"
    assert skip(mode, spot.State({}), entry(spot_quota=False)) is None  # a spot offering without quotas is possible
    assert skip(mode, spot.State({}), tasks=("stressnglongrun",)) == "its tasks do not allow spot"
    quarantined = spot.State({KEY: times(1, 2)})
    assert skip(mode, quarantined) == "quarantined"
    kube = FakeKube()
    spot.record_rejected(kube, "oci", "VM.X", NOW - timedelta(days=1))
    assert skip(mode, spot.State.load(kube)) == "its spot launch was rejected"


def test_planning_skip_of_spot_until_exhausted():
    mode = CapacityMode.SPOT_UNTIL_EXHAUSTED
    kube = FakeKube()
    assert skip(mode, spot.State({})) is None  # never tried
    spot.record_try(kube, "oci", "VM.X", NOW, S)
    assert skip(mode, spot.State.load(kube)) == "spot was tried in this slot"
    assert skip(mode, spot.State.load(kube), now=NOW + timedelta(hours=3)) is None  # the next slot
    # on-demand is allowed at once, so the SKU is planned (like SpotFirst)
    state = spot.State.load(kube)
    assert skip(mode, state, entry(offered=("on-demand",))) is None
    assert skip(mode, state, tasks=("stressnglongrun",)) is None
    assert skip(mode, spot.State({**kube.cm["data"], KEY: times(1, 2)})) is None
    # exhausted
    exhausted = spot.State({TRIES: spot._dump_tries(week_of_tries(NOW - timedelta(days=14)), S)})
    assert skip(mode, exhausted) is None


def test_the_exhausted_line_is_logged_once_per_state(caplog):
    spot._announced.clear()
    state = spot.State({TRIES: spot._dump_tries(week_of_tries(NOW - timedelta(days=14)), S)})
    with caplog.at_level("INFO"):
        for _ in range(3):
            assert skip(CapacityMode.SPOT_UNTIL_EXHAUSTED, state) is None
    assert caplog.text.count("exhausted, on-demand allowed") == 1
