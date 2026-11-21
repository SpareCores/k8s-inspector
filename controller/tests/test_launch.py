import json
from datetime import UTC, datetime, timedelta

import pytest
from fakes import Clock, FakeKube

from k8s_inspector import launch

NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("message", "default", "reason"),
    [
        ("InsufficientCapacity (InsufficientInstanceCapacity): no capacity", "BOOT_FAILURE", "ICE"),
        ("ZoneUnusable (Unsupported): zone", "BOOT_FAILURE", "ICE"),
        ("QuotaExceeded (VcpuLimit): vCPU quota", "ICE", "QUOTA"),
        ("DailyLimit (LimitExceeded): daily resource creation limit", "ICE", "DAILY_LIMIT"),
        ("Retryable (RequestLimitExceeded): slow down", "ICE", "VENDOR"),
        ("Unsupported (InvalidParameterValue): shape", "ICE", "LAUNCH_REJECTED"),
        ("InvalidOffering (X): y", "ICE", "LAUNCH_REJECTED"),
        ("Fatal (InvalidParameter): 400", "ICE", "LAUNCH_REJECTED"),
        # tolerant: no class (older SCarpenter), an unknown class, an empty code, text that merely mentions one
        ("InsufficientInstanceCapacity in zone", "ICE", "ICE"),
        ("no node after 60 minutes", "BOOT_FAILURE", "BOOT_FAILURE"),
        ("SomethingNew (X): y", "ICE", "ICE"),
        ("Fatal (): y", "ICE", "LAUNCH_REJECTED"),
        ("launching X: Retryable (RequestLimitExceeded): slow", "ICE", "VENDOR"),  # wrapped
        ("QuotaExceededX (X): y", "ICE", "ICE"),
        # SCarpenter's contract: the code is "Unknown" when the vendor gave none; core wraps the message in an event
        ("InsufficientCapacity (Unknown): no capacity", "BOOT_FAILURE", "ICE"),
        ("NodeClaim bench-x event: insufficient capacity, QuotaExceeded (Unknown): vCPU", "ICE", "QUOTA"),
        ("NodeClaim bench-x event: insufficient capacity, DailyLimit (LimitExceeded): daily", "ICE", "DAILY_LIMIT"),
        ("NodeClaim bench-x event: insufficient capacity, ZoneUnusable (Unknown): z", "BOOT_FAILURE", "ICE"),
        ("", "ICE", "ICE"),
    ],
)
def test_the_class_in_the_message_gives_the_reason(message, default, reason):
    assert launch.reason_of_class(message, default) == reason


def test_error_message_round_trip_and_temporary_reasons():
    error = launch.error_message("QUOTA", "QuotaExceeded (X): y")
    assert error == "SCARPENTER_QUOTA: QuotaExceeded (X): y"
    assert launch.reason_of_error(error) == "QUOTA" and launch.reason_of_error("NODE_LOST") is None
    assert launch.reason_of_error(None) is None
    temporary = [
        r
        for r in ("ICE", "QUOTA", "DAILY_LIMIT", "VENDOR", "NO_OFFERING", "QUOTA_ZERO", "LAUNCH_FAILED")
        if launch.is_temporary(f"SCARPENTER_{r}: x")
    ]
    assert len(temporary) == 7
    for reason in ("LAUNCH_REJECTED", "BOOT_FAILURE"):
        assert not launch.is_temporary(f"SCARPENTER_{reason}: x")
    assert launch.is_temporary("SCARPENTER_BOOT_FAILURE: no node after 60 minutes")  # older runs that only waited
    assert not launch.is_temporary(None)
    for odd in (7, ["SCARPENTER_ICE: x"], {"a": 1}):  # a damaged meta.json: not an error message, never raises
        assert launch.reason_of_error(odd) is None and not launch.is_temporary(odd)


@pytest.mark.parametrize(
    ("reason", "waits"),
    [
        ("ICE", [1, 2, 4, 8, 16, 24, 24, 24]),  # never gives up
        ("QUOTA", [1, 2, 4, 6, 6]),
        ("VENDOR", [1, 2, 4, 6]),
        ("NO_OFFERING", [24, 24, 24]),  # flat: the offered filter may fail open
        ("DAILY_LIMIT", [1, 2, 4, 6, 6]),
    ],
)
def test_backoff_doubles_to_the_cap_of_the_reason(reason, waits):
    assert [launch.wait_after(reason, n) for n in range(1, len(waits) + 1)] == [timedelta(hours=h) for h in waits]


def test_backoff_does_not_overflow_for_a_huge_count():
    assert launch.wait_after("ICE", 100) == timedelta(hours=24)
    assert launch.wait_after("QUOTA", 10**6) == timedelta(hours=6)


def test_failures_are_counted_per_sku_and_set_next():
    kube = FakeKube(Clock(NOW))
    assert launch.read(kube, "oci") == {}
    assert launch.record_failure(kube, "oci", "a", "ICE", NOW) == 1
    assert launch.record_failure(kube, "oci", "a", "ICE", NOW + timedelta(hours=1)) == 2
    launch.record_failure(kube, "oci", "b", "QUOTA", NOW)
    entries = launch.read(kube, "oci")
    assert entries["a"] == {
        "reason": "ICE",
        "since": NOW.isoformat(),
        "last": (NOW + timedelta(hours=1)).isoformat(),
        "count": 2,
        "next": (NOW + timedelta(hours=3)).isoformat(),
    }
    assert entries["b"]["next"] == (NOW + timedelta(hours=1)).isoformat()
    assert launch.read(kube, "aws") == {}  # per vendor


def test_deferred_lists_waiting_skus_and_prunes_the_ones_not_active():
    kube = FakeKube(Clock(NOW))
    for sku in ("a", "b", "gone"):
        launch.record_failure(kube, "oci", sku, "ICE", NOW)
    launch.record_failure(kube, "oci", "b", "ICE", NOW - timedelta(days=1))  # next is long past
    soon = NOW + timedelta(minutes=30)
    assert launch.deferred(kube, "oci", ["a", "b"], soon) == {"a"}  # b's retry is due, gone is pruned
    assert set(launch.read(kube, "oci")) == {"a", "b"}
    assert launch.deferred(kube, "oci", [], soon) == {"a"}  # Keeper listed nothing: nothing is pruned
    assert set(launch.read(kube, "oci")) == {"a", "b"}


def test_clear_removes_entries_and_never_creates_an_empty_configmap():
    kube = FakeKube(Clock(NOW))
    launch.clear(kube, "oci", ["a"])
    assert kube.list("configmap") == []
    launch.record_failure(kube, "oci", "a", "ICE", NOW)
    launch.clear(kube, "oci", ["a"])
    assert launch.read(kube, "oci") == {}


def test_a_damaged_configmap_means_no_entries_and_is_repaired():
    kube = FakeKube(Clock(NOW))
    launch.record_failure(kube, "oci", "a", "ICE", NOW)
    kube.merge_patch("configmap", "k8s-inspector-launch-oci", {"data": {"skus": "{broken"}})
    assert launch.read(kube, "oci") == {}
    assert launch.deferred(kube, "oci", ["a"], NOW) == set()
    assert launch.record_failure(kube, "oci", "a", "ICE", NOW) == 1


def test_a_different_reason_restarts_the_count_and_until_sets_next():
    kube = FakeKube(Clock(NOW))
    assert launch.record_failure(kube, "oci", "a", "ICE", NOW) == 1
    assert launch.record_failure(kube, "oci", "a", "ICE", NOW) == 2
    assert launch.record_failure(kube, "oci", "a", "BOOT_FAILURE", NOW) == 1
    until = NOW + timedelta(hours=9)
    launch.record_failure(kube, "oci", "b", "DAILY_LIMIT", NOW, until)
    assert launch.read(kube, "oci")["b"]["next"] == until.isoformat()
    launch.record_failure(kube, "oci", "b", "DAILY_LIMIT", NOW, NOW - timedelta(hours=1))  # a past `until` is ignored
    assert launch.read(kube, "oci")["b"]["next"] == (NOW + timedelta(hours=2)).isoformat()


def test_deferred_in_a_dry_run_prunes_nothing():
    kube = FakeKube(Clock(NOW))
    for sku in ("a", "gone"):
        launch.record_failure(kube, "oci", sku, "ICE", NOW)
    assert launch.deferred(kube, "oci", ["a"], NOW, prune=False) == {"a", "gone"}
    assert set(launch.read(kube, "oci")) == {"a", "gone"}


@pytest.mark.parametrize(
    "skus",
    [
        '["a", "b"]',  # not an object
        '"text"',
        "null",
        '{"a": "ICE", "b": 3, "c": null, "d": ["ICE"]}',  # no entry is an object
    ],
)
def test_a_launch_configmap_of_the_wrong_shape_has_no_entries_and_is_repaired(skus):
    kube = FakeKube(Clock(NOW))
    launch.record_failure(kube, "oci", "x", "ICE", NOW)
    kube.merge_patch("configmap", "k8s-inspector-launch-oci", {"data": {"skus": skus}})
    assert launch.read(kube, "oci") == {}
    assert launch.deferred(kube, "oci", ["a"], NOW) == set()
    assert launch.record_failure(kube, "oci", "a", "ICE", NOW) == 1
    assert set(launch.read(kube, "oci")) == {"a"}


def test_damaged_entries_are_dropped_and_damaged_times_block_nothing():
    kube = FakeKube(Clock(NOW))
    good = {"reason": "ICE", "since": NOW.isoformat(), "count": 1, "next": (NOW + timedelta(hours=1)).isoformat()}
    skus = {
        "good": good,
        "list": ["ICE"],
        "naive": {**good, "next": (NOW + timedelta(hours=1)).replace(tzinfo=None).isoformat()},
        "number": {**good, "next": 5},
        "null": {**good, "next": None},
        "text": {**good, "next": "soon"},
        "missing": {"reason": "ICE"},
        "count": {**good, "count": "many"},
    }
    kube.create("configmap", {"metadata": {"name": "k8s-inspector-launch-oci"}, "data": {"skus": json.dumps(skus)}})
    assert set(launch.read(kube, "oci")) == set(skus) - {"list"}
    assert launch.deferred(kube, "oci", list(skus), NOW) == {"good", "count"}
    assert launch.record_failure(kube, "oci", "count", "ICE", NOW) == 1  # an unusable count starts again


def test_no_lscpu_waits_a_flat_24_hours_like_no_offering():
    for reason in ("NO_LSCPU", "NO_OFFERING"):
        assert [launch.wait_after(reason, n) for n in (1, 2, 9)] == [timedelta(hours=24)] * 3
    assert "NO_LSCPU" in launch.REASONS


def test_boot_failure_waits_a_flat_day_and_is_permanent_at_the_second():
    assert launch.wait_after("BOOT_FAILURE", 1) == timedelta(hours=24)
    assert launch.wait_after("BOOT_FAILURE", 2) == timedelta(hours=24)
    assert launch.BOOT_FAILURE_LIMIT == 2


def fail_boot(kube, vendor, skus, when):
    for sku in skus:
        launch.record_failure(kube, vendor, sku, "BOOT_FAILURE", when)


def test_the_breaker_trips_at_three_distinct_skus_and_a_day_ends_it():
    kube = FakeKube(Clock(NOW))
    fail_boot(kube, "oci", ["a", "b"], NOW)
    fail_boot(kube, "oci", ["a"], NOW)  # the same SKU twice is still one SKU
    assert not launch.paused(kube, "oci", NOW)
    fail_boot(kube, "oci", ["c"], NOW)
    assert launch.paused(kube, "oci", NOW + timedelta(hours=23))
    assert not launch.paused(kube, "oci", NOW + timedelta(hours=25))
    assert not launch.paused(kube, "aws", NOW)  # per vendor


def test_another_reason_does_not_count_for_the_breaker():
    kube = FakeKube(Clock(NOW))
    fail_boot(kube, "oci", ["a", "b"], NOW)
    launch.record_failure(kube, "oci", "c", "ICE", NOW)
    assert not launch.paused(kube, "oci", NOW)


def test_a_ready_node_of_the_vendor_clears_the_breaker_and_keeps_the_waits():
    kube = FakeKube(Clock(NOW))
    fail_boot(kube, "oci", ["a", "b", "c"], NOW)
    launch.node_ready(kube, "oci", "other")
    assert not launch.paused(kube, "oci", NOW)
    assert launch.read(kube, "oci")["a"]["count"] == 1 and launch.deferred(kube, "oci", ["a", "b", "c"], NOW) == {
        "a",
        "b",
        "c",
    }
    fail_boot(kube, "oci", ["a"], NOW)  # counts again from the failure after the node
    fail_boot(kube, "oci", ["b", "c"], NOW)
    assert launch.paused(kube, "oci", NOW)
    launch.node_ready(kube, "oci", "a")  # the SKU's own entry goes too
    assert "a" not in launch.read(kube, "oci")
