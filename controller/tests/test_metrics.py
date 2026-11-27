import json
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta

import pytest

from k8s_inspector import crd
from k8s_inspector.metrics import Metrics, make_server, task_result
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    BenchmarkRun,
    CapacityMode,
    CapacityType,
    RunPhase,
    RunSpec,
    RunStatus,
    Settings,
    TaskState,
    TaskStatus,
    VendorSettings,
)

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
SETTINGS = Settings(vendors={"oci": VendorSettings(enabled=True, publish=True), "aws": VendorSettings(enabled=True)})


def done(name, exit_code, minutes_ago=30, published=False):
    return TaskStatus(
        name,
        TaskState.DONE,
        1,
        NOW - timedelta(minutes=minutes_ago + 5),
        NOW - timedelta(minutes=minutes_ago),
        exit_code,
        published=published,
    )


def make_run(sku, phase, tasks=(), vendor="oci", **status):
    return BenchmarkRun(
        f"{vendor}-{sku}",
        "inspector",
        f"uid-{sku}",
        "1",
        NOW - timedelta(hours=1),
        RunSpec(vendor, sku, "r1"),
        RunStatus(phase=phase, tasks=list(tasks), **status),
    )


@pytest.fixture
def runs(monkeypatch):
    store: list[BenchmarkRun] = []
    monkeypatch.setattr(crd, "list_runs", lambda kube: list(store))
    return store


@pytest.fixture
def metrics(runs):
    return Metrics(None, lambda: SETTINGS, clock=lambda: NOW)


def test_task_result_mapping():
    assert [task_result(c) for c in (0, 1, 137, 256, -1, -3, -4, -5, -6)] == [
        "ok",
        "failed",
        "failed",
        "failed",
        "lost",
        "failed",
        "lost",
        "lost",
        "timeout",
    ]


def test_run_phases_listed_for_every_configured_vendor(runs, metrics):
    runs.append(make_run("a", RunPhase.RUNNING))
    runs.append(make_run("b", RunPhase.RUNNING))
    text = metrics.render()

    assert 'k8s_inspector_runs{phase="Running",vendor="oci"} 2' in text
    assert 'k8s_inspector_runs{phase="Pending",vendor="oci"} 0' in text
    assert 'k8s_inspector_runs{phase="Running",vendor="aws"} 0' in text
    assert "# TYPE k8s_inspector_runs gauge" in text


def test_task_counters_count_each_task_once(runs, metrics):
    runs.append(make_run("a", RunPhase.PUBLISHING, [done("lscpu", 0), done("geekbench", -6), done("stress", 1)]))
    metrics.render()
    text = metrics.render()  # a second scrape must not count again

    assert 'k8s_inspector_tasks_total{result="ok",task="lscpu",vendor="oci"} 1' in text
    assert 'k8s_inspector_tasks_total{result="timeout",task="geekbench",vendor="oci"} 1' in text
    assert 'k8s_inspector_tasks_total{result="failed",task="stress",vendor="oci"} 1' in text
    assert 'k8s_inspector_task_duration_seconds_sum{task="lscpu",vendor="oci"} 300' in text
    assert 'k8s_inspector_task_duration_seconds_count{task="lscpu",vendor="oci"} 1' in text

    runs[0].status.tasks.append(done("openssl", 0))
    assert 'k8s_inspector_tasks_total{result="ok",task="openssl",vendor="oci"} 1' in metrics.render()


def test_attempt_counters(runs, metrics):
    attempts = [
        AttemptStatus(1, CapacityType.SPOT, outcome=AttemptOutcome.SPOT_INTERRUPTED),
        AttemptStatus(2, CapacityType.ON_DEMAND, outcome=AttemptOutcome.COMPLETED),
        AttemptStatus(3, CapacityType.ON_DEMAND, outcome=AttemptOutcome.HARNESS_ERROR),
        AttemptStatus(4, CapacityType.ON_DEMAND),  # open: not counted
        AttemptStatus(5, None, outcome=AttemptOutcome.PROVISIONING_FAILED),  # never got a node
    ]
    runs.append(make_run("a", RunPhase.RUNNING, attempts=attempts))
    text = metrics.render()

    assert 'k8s_inspector_run_attempts_total{capacity_type="spot",outcome="interrupted",vendor="oci"} 1' in text
    assert 'k8s_inspector_run_attempts_total{capacity_type="on-demand",outcome="completed",vendor="oci"} 1' in text
    assert 'outcome="harness_error"' in text
    assert 'capacity_type="none",outcome="provisioning_failed"' in text
    assert text.count("run_attempts_total{") == 4


def test_backlog_lag_spend_and_node_age(runs, metrics):
    running = make_run(
        "a",
        RunPhase.RUNNING,
        [done("lscpu", 0, minutes_ago=90), done("geekbench", 0, minutes_ago=10, published=True)],
        node="node-1",
        price_per_hour="0.25",
        attempts=[AttemptStatus(1, CapacityType.SPOT, start=NOW - timedelta(minutes=45))],
    )
    runs.append(running)
    runs.append(make_run("b", RunPhase.PUBLISHING, [done("lscpu", 0)], vendor="aws"))  # aws does not publish
    text = metrics.render()

    assert "k8s_inspector_publish_backlog 1\n" in text
    assert "k8s_inspector_publish_lag_seconds 5400\n" in text
    assert 'k8s_inspector_spend_usd_per_hour{vendor="oci"} 0.25' in text
    assert 'k8s_inspector_spend_usd_per_hour{vendor="aws"} 0' in text
    assert 'k8s_inspector_node_age_seconds{run="oci-a",sku="a",vendor="oci"} 2700' in text


def test_backlog_counts_only_skus_that_may_be_published(runs):
    listed = Settings(vendors={"oci": VendorSettings(enabled=True, publish=("a",))})
    runs.append(make_run("a", RunPhase.PUBLISHING, [done("lscpu", 0)]))
    runs.append(make_run("b", RunPhase.PUBLISHING, [done("lscpu", 0), done("openssl", 0)]))
    text = Metrics(None, lambda: listed, clock=lambda: NOW).render()
    assert "k8s_inspector_publish_backlog 1\n" in text


def test_unpublishable_tasks_counter(runs, metrics):
    assert "k8s_inspector_unpublishable_tasks_total 0\n" in metrics.render()
    metrics.record_unpublishable(2)
    assert "k8s_inspector_unpublishable_tasks_total 2\n" in metrics.render()


def test_last_publish_timestamp_starts_at_process_start(runs, metrics):
    def stamp():
        line = next(x for x in metrics.render().splitlines() if x.startswith("k8s_inspector_last_publish"))
        return float(line.split()[1])

    started = stamp()
    assert started > 1e9
    metrics._last_publish = 0.0
    metrics.record_publish()
    assert stamp() >= started


def test_refresh_failure_serves_previous_snapshot(runs, metrics, monkeypatch):
    runs.append(make_run("a", RunPhase.RUNNING))
    metrics.render()

    def boom(kube):
        raise RuntimeError("api down")

    monkeypatch.setattr(crd, "list_runs", boom)
    assert 'k8s_inspector_runs{phase="Running",vendor="oci"} 1' in metrics.render()


def test_label_values_are_escaped(runs, metrics):
    runs.append(make_run("a", RunPhase.RUNNING, [done('we"ird\\', 0)]))
    assert 'task="we\\"ird\\\\"' in metrics.render()


def test_http_endpoints(runs, metrics):
    import threading

    runs.append(make_run("a", RunPhase.RUNNING))
    server = make_server(metrics, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with urllib.request.urlopen(f"{base}/metrics", timeout=5) as r:
            assert r.status == 200
            assert r.headers["Content-Type"].startswith("text/plain")
            assert b"k8s_inspector_runs" in r.read()
        with urllib.request.urlopen(f"{base}/healthz", timeout=5) as r:
            assert r.read() == b"ok\n"
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(f"{base}/nope", timeout=5)
        assert e.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def test_launch_gauges_and_failure_counter():
    from fakes import Clock, FakeKube

    from k8s_inspector import launch

    kube = FakeKube(Clock(NOW))
    launch.record_failure(kube, "oci", "a", "ICE", NOW)
    launch.record_failure(kube, "oci", "b", "ICE", NOW - timedelta(days=1))  # retry due: deferred, not blocked
    launch.record_failure(kube, "oci", "c", "QUOTA", NOW)
    metrics = Metrics(kube, lambda: SETTINGS, clock=lambda: NOW)
    metrics.record_launch_failure("oci", "ICE")
    metrics.record_launch_failure("oci", "ICE")
    metrics.record_launch_failure("aws", "something unbounded")
    text = metrics.render()
    assert 'k8s_inspector_launch_deferred_skus{reason="ICE",vendor="oci"} 2' in text
    assert 'k8s_inspector_launch_blocked{reason="ICE",vendor="oci"} 1' in text
    assert 'k8s_inspector_launch_blocked{reason="QUOTA",vendor="oci"} 1' in text
    assert 'k8s_inspector_launch_blocked{reason="DAILY_LIMIT",vendor="aws"} 0' in text
    assert 'k8s_inspector_launch_failures_total{reason="ICE",vendor="oci"} 2' in text
    assert 'k8s_inspector_launch_failures_total{reason="OTHER",vendor="aws"} 1' in text
    assert 'k8s_inspector_launch_paused{vendor="oci"} 0' in text
    metrics.record_vendor_skipped("oci", "offered_types_unusable")
    assert 'k8s_inspector_autoplan_vendor_skipped_total{reason="offered_types_unusable",vendor="oci"} 1' in (
        metrics.render()
    )


def test_a_vendor_paused_by_boot_failures_shows_in_the_gauge():
    from fakes import Clock, FakeKube

    from k8s_inspector import launch

    kube = FakeKube(Clock(NOW))
    for sku in "abc":
        launch.record_failure(kube, "oci", sku, "BOOT_FAILURE", NOW)
    metrics = Metrics(kube, lambda: SETTINGS, clock=lambda: NOW)
    metrics.record_launch_failure("oci", "BOOT_FAILURE")
    text = metrics.render()
    assert 'k8s_inspector_launch_paused{vendor="oci"} 1' in text
    assert 'k8s_inspector_launch_failures_total{reason="BOOT_FAILURE",vendor="oci"} 1' in text


def test_launch_gauges_survive_a_damaged_launch_configmap():
    from fakes import Clock, FakeKube

    from k8s_inspector import launch

    kube = FakeKube(Clock(NOW))
    launch.record_failure(kube, "oci", "a", "ICE", NOW)
    for skus in (
        '["a"]',
        json.dumps({"a": {"reason": ["ICE"], "next": 5}, "b": "x", "c": {"reason": "ICE", "next": None}}),
    ):
        kube.merge_patch("configmap", "k8s-inspector-launch-oci", {"data": {"skus": skus}})
        text = Metrics(kube, lambda: SETTINGS, clock=lambda: NOW).render()
        assert 'k8s_inspector_launch_blocked{reason="ICE",vendor="oci"} 0' in text


def test_dbaas_stack_ages_come_from_the_stack_and_the_inventory_clock_starts_with_the_process(runs):
    from dataclasses import replace

    from fakes import Clock, FakeKube

    from k8s_inspector.dbaas_sweep import Stack
    from k8s_inspector.model import DbaasVendor

    settings = replace(SETTINGS, dbaas=replace(SETTINGS.dbaas, vendors={"oci": DbaasVendor()}))
    clock = Clock(NOW)
    metrics = Metrics(FakeKube(clock), lambda: settings, clock=clock)
    clock.advance(minutes=10)
    # a run that ended with -1 and holds no database; its stack is 9 h old whatever the run's age
    runs.append(make_run("vm", RunPhase.FAILED))
    metrics.record_dbaas_stacks([Stack(runs[0].name, "oci", NOW + timedelta(minutes=10) - timedelta(hours=9), False)])
    metrics.record_dbaas_leftover_failure("oci")
    text = metrics.render()
    assert 'k8s_inspector_dbaas_instances{vendor="oci"} 1' in text
    assert 'k8s_inspector_dbaas_oldest_age_seconds{vendor="oci"} 32400' in text
    assert 'k8s_inspector_dbaas_leftover_stack_failures_total{vendor="oci"} 1' in text
    assert f'k8s_inspector_dbaas_inventory_last_success_timestamp_seconds{{vendor="oci"}} {NOW.timestamp():g}' in text


def test_spot_try_counter_and_waiting_gauge_and_exhausted_counter():
    from fakes import Clock, FakeKube

    from k8s_inspector import spot

    settings = Settings(
        vendors={
            "oci": VendorSettings(enabled=True, capacity=CapacityMode.SPOT_UNTIL_EXHAUSTED),
            "aws": VendorSettings(enabled=True),
        }
    )
    kube = FakeKube(Clock(NOW))
    start = NOW - timedelta(days=15)
    for i in range(56):  # every slot of the week, 15 days ago: exhausted
        spot.record_try(kube, "oci", "full", start + timedelta(hours=3 * i), settings)
    spot.record_try(kube, "oci", "partial", NOW - timedelta(hours=1), settings)  # waiting
    spot.record_try(kube, "oci", "other", NOW - timedelta(hours=1), settings)  # waiting
    metrics = Metrics(kube, lambda: settings, clock=lambda: NOW)
    for result in ("capacity", "unavailable", "unavailable", "quota_busy"):
        metrics.record_spot_try("oci", result)
    out = metrics.render()
    assert 'k8s_inspector_spot_tries_total{result="unavailable",vendor="oci"} 2' in out
    assert 'k8s_inspector_spot_tries_total{result="capacity",vendor="oci"} 1' in out
    assert 'k8s_inspector_spot_tries_total{result="quota_busy",vendor="oci"} 1' in out
    assert 'k8s_inspector_spot_waiting_skus{vendor="oci"} 2' in out
    assert 'spot_waiting_skus{vendor="aws"}' not in out  # only vendors with a spot-only mode
    assert 'k8s_inspector_spot_exhausted_total{vendor="oci"} 1' in out
    assert 'k8s_inspector_spot_exhausted_total{vendor="oci"} 1' in metrics.render()  # counted once


def test_spot_only_vendors_never_exhaust():
    from fakes import Clock, FakeKube

    from k8s_inspector import spot

    settings = Settings(vendors={"oci": VendorSettings(enabled=True, capacity=CapacityMode.SPOT_ONLY)})
    kube = FakeKube(Clock(NOW))
    for i in range(56):
        spot.record_try(kube, "oci", "full", NOW - timedelta(days=15) + timedelta(hours=3 * i), settings)
    out = Metrics(kube, lambda: settings, clock=lambda: NOW).render()
    assert 'k8s_inspector_spot_waiting_skus{vendor="oci"} 1' in out
    assert 'k8s_inspector_spot_exhausted_total{vendor="oci"} 0' in out


def test_a_spot_unavailable_attempt_is_counted_as_an_attempt_outcome(runs, metrics):
    run = make_run("a", RunPhase.FAILED, attempts=[AttemptStatus(1, None, outcome=AttemptOutcome.SPOT_UNAVAILABLE)])
    runs.append(run)
    assert (
        'k8s_inspector_run_attempts_total{capacity_type="none",outcome="spot_unavailable",vendor="oci"} 1'
        in metrics.render()
    )
