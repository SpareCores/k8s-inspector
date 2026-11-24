"""Prometheus metrics of the controller (monitoring.md "What k8s-inspector exposes") and the HTTP endpoint.

The numbers are derived from the BenchmarkRuns on every scrape (one LIST, 5-20 objects), so they need no
cooperation from the runner and survive a restart. Counters (`*_total`) count what this process has seen:
each finished task and each closed attempt once, keyed by run uid. After a restart they start again, which
Prometheus handles as a counter reset (kept runs are counted once more).

Exposed: `k8s_inspector_runs`, `_tasks_total`, `_run_attempts_total`, `_node_age_seconds`, `_publish_backlog`,
`_publish_lag_seconds` (age of the oldest done, unpublished task), `_unpublishable_tasks_total` (tasks given up
on: no lscpu result), `_last_publish_timestamp_seconds`
(last publish cycle that did not fail, set by `record_publish`; process start before the first one),
`_spend_usd_per_hour`, `_task_duration_seconds` (summary sum/count), `_launch_failures_total{vendor,reason}`
(runs that ended because no node could be launched, counted by the runner), `_launch_deferred_skus{vendor,reason}`
and `_launch_blocked{vendor,reason}` (SKUs in the launch ConfigMap, and those of them still waiting for their
retry). `_dbaas_instances{vendor}` and `_dbaas_oldest_age_seconds{vendor}`: the managed databases (stacks) that exist,
from the DBaaS
runs not yet destroyed and the leaked stacks the sweeper sees (stacks of a vendor the configuration does not know are
`vendor="unknown"`); alert when the oldest is over 8 h. `_dbaas_tagged_databases{vendor}` and
`_dbaas_tagged_oldest_age_seconds{vendor}`: the databases the vendor's inventory Job found by their `sc_inspector_run`
tag (also the ones no state records), refreshed every 30 minutes; alert when the oldest is over 8 h. A vendor whose
inventory failed has no tagged metrics. `_dbaas_inventory_last_success_timestamp_seconds{vendor}`: when the last
inventory of every configured vendor worked (0: not since the controller started); alert when it is over 2 h old.
`/healthz` answers 200 as long as the process serves.
"""

import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from k8s_inspector import crd, launch
from k8s_inspector.kube import Kube
from k8s_inspector.model import AttemptOutcome, DbPhase, ExitCode, RunPhase, Settings, TaskState
from k8s_inspector.planner import ACTIVE_PHASES, spend_per_hour
from k8s_inspector.publisher import publish_allowed

log = logging.getLogger(__name__)

PORT = 9090
PREFIX = "k8s_inspector_"

_ATTEMPT_LABEL = {
    AttemptOutcome.COMPLETED: "completed",
    AttemptOutcome.SPOT_INTERRUPTED: "interrupted",
    AttemptOutcome.NODE_LOST: "node_lost",
    AttemptOutcome.PROVISIONING_FAILED: "provisioning_failed",
    AttemptOutcome.LAUNCH_FAILED: "launch_failed",
    AttemptOutcome.CANCELLED: "cancelled",
    AttemptOutcome.HARNESS_ERROR: "harness_error",
}

LAUNCH_REASONS = (*launch.REASONS, *launch.PERMANENT, "NO_OFFERING")  # the bounded `reason` label values

Labels = tuple[tuple[str, str], ...]  # sorted (name, value) pairs: hashable key of one series


def task_result(exit_code: int) -> str:
    """monitoring.md result label: ok, timeout, lost (never finished or no node) or failed."""
    if exit_code == ExitCode.OK:
        return "ok"
    if exit_code == ExitCode.TIMEOUT:
        return "timeout"
    if exit_code in (ExitCode.NO_NODE, ExitCode.NEVER_FINISHED, ExitCode.RETRIES_EXHAUSTED):
        return "lost"
    return "failed"


def _labels(**values: str) -> Labels:
    return tuple(sorted(values.items()))


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _line(name: str, labels: Labels, value: float) -> str:
    rendered = ",".join(f'{k}="{_escape(v)}"' for k, v in labels)
    return f"{PREFIX}{name}{{{rendered}}} {value:g}" if rendered else f"{PREFIX}{name} {value:g}"


class Metrics:
    def __init__(
        self,
        kube: Kube,
        settings_provider: Callable[[], Settings],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._kube = kube
        self._settings = settings_provider
        self._clock = clock
        self._lock = threading.Lock()
        self._counters: dict[str, dict[Labels, float]] = defaultdict(lambda: defaultdict(float))
        self._seen: set[tuple] = set()
        self._gauges: list[str] = []  # last good snapshot, served when the API server is unreachable
        self._last_publish = time.time()  # process start until the first cycle: a stuck publisher still ages
        self._counters["unpublishable_tasks_total"][()] = 0
        self._orphans: list[tuple[str, float]] = []  # (vendor, age in seconds) of the stacks without a run
        self._tagged: dict = {}  # vendor -> `dbaas_sweep.Tagged`: the databases the last inventory found by their tag
        self._inventory_success: dict[str, datetime] = {}  # vendor -> when its inventory last worked

    def record_dbaas_stacks(self, stacks) -> None:
        """The stacks the sweeper saw (`dbaas_sweep.Stack`); those without a BenchmarkRun are counted here, the others
        come from their runs."""
        now = self._clock()
        with self._lock:
            self._orphans = [(s.vendor or "unknown", (now - s.modified).total_seconds()) for s in stacks if s.orphan]

    def record_dbaas_inventory(self, tagged: dict, last_success: dict) -> None:
        """The latest good inventory of every vendor (`dbaas_sweep.Tagged` by vendor) and when each vendor's last one
        worked."""
        with self._lock:
            self._tagged, self._inventory_success = dict(tagged), dict(last_success)

    def record_publish(self) -> None:
        """A publish cycle finished without error."""
        with self._lock:
            self._last_publish = time.time()

    def record_launch_failure(self, vendor: str, reason: str) -> None:
        """A run ended because no node could be launched. Reasons outside `LAUNCH_REASONS` count as OTHER."""
        reason = reason if reason in LAUNCH_REASONS else "OTHER"
        with self._lock:
            self._counters["launch_failures_total"][_labels(vendor=vendor, reason=reason)] += 1

    def record_unpublishable(self, count: int) -> None:
        """Tasks the publisher gave up on (finished, but no lscpu result to pick a variant from)."""
        with self._lock:
            self._counters["unpublishable_tasks_total"][()] += count

    def render(self) -> str:
        """Prometheus text format. A failing LIST keeps the previous gauges: the liveness probe scrapes this
        endpoint, and an API server hiccup must not restart the controller."""
        with self._lock:
            try:
                self._refresh()
            except Exception:
                log.exception("metrics refresh failed, serving the previous snapshot")
            lines = list(self._gauges)
            for name, series in sorted(self._counters.items()):
                lines.append(f"# TYPE {PREFIX}{name} counter")
                lines.extend(_line(name, labels, value) for labels, value in sorted(series.items()))
            lines.append(f"# TYPE {PREFIX}last_publish_timestamp_seconds gauge")
            lines.append(_line("last_publish_timestamp_seconds", (), self._last_publish))
            return "\n".join(lines) + "\n"

    def _refresh(self) -> None:
        settings = self._settings()
        now = self._clock()
        runs = crd.list_runs(self._kube)

        phases: dict[tuple[str, str], int] = {
            (vendor, phase.value): 0 for vendor in settings.vendors for phase in RunPhase
        }
        spend: dict[str, float] = defaultdict(float)
        node_age: dict[Labels, float] = {}
        databases: dict[str, list[float]] = defaultdict(list)  # vendor -> ages of its existing databases
        backlog = 0
        oldest_unpublished: datetime | None = None

        for run in runs:
            vendor = run.spec.vendor
            if run.status.database and run.status.database.phase != DbPhase.DESTROYED:
                databases[vendor].append((now - run.created).total_seconds())
            for phase in RunPhase:
                phases.setdefault((vendor, phase.value), 0)
            phases[(vendor, run.status.phase.value)] += 1

            if run.status.phase in ACTIVE_PHASES and run.status.node:
                spend[vendor] += spend_per_hour([run])
                open_attempts = [a for a in run.status.attempts if a.start and not a.end]
                if open_attempts:
                    labels = _labels(vendor=vendor, sku=run.spec.sku, run=run.name)
                    node_age[labels] = (now - open_attempts[-1].start).total_seconds()

            for attempt in run.status.attempts:
                if attempt.outcome is not None and (run.uid, "attempt", attempt.n) not in self._seen:
                    self._seen.add((run.uid, "attempt", attempt.n))
                    self._counters["run_attempts_total"][
                        _labels(
                            vendor=vendor,
                            capacity_type=attempt.capacity_type.value if attempt.capacity_type else "none",
                            outcome=_ATTEMPT_LABEL[attempt.outcome],
                        )
                    ] += 1

            for task in run.status.tasks:
                if task.state != TaskState.DONE:
                    continue
                database = run.spec.database
                allowed = (
                    publish_allowed(settings, database["vendor"], database["id"])
                    if database
                    else publish_allowed(settings, vendor, run.spec.sku)
                )
                if not task.published and allowed:
                    backlog += 1
                    if task.end and (oldest_unpublished is None or task.end < oldest_unpublished):
                        oldest_unpublished = task.end
                if (run.uid, "task", task.name) in self._seen or task.exit_code is None:
                    continue
                self._seen.add((run.uid, "task", task.name))
                result = task_result(task.exit_code)
                self._counters["tasks_total"][_labels(vendor=vendor, task=task.name, result=result)] += 1
                if task.start and task.end:
                    duration = _labels(vendor=vendor, task=task.name)
                    self._counters["task_duration_seconds_sum"][duration] += (task.end - task.start).total_seconds()
                    self._counters["task_duration_seconds_count"][duration] += 1

        deferred: dict[tuple[str, str], int] = {(v, r): 0 for v in settings.vendors for r in launch.REASONS}
        blocked = dict(deferred)
        for vendor in settings.vendors:
            for entry in launch.read(self._kube, vendor).values():
                key = (vendor, str(entry.get("reason")))
                if key in deferred:
                    deferred[key] += 1
                    blocked[key] += launch.is_waiting(entry, now)

        lines = [f"# TYPE {PREFIX}runs gauge"]
        lines += [_line("runs", _labels(vendor=v, phase=p), n) for (v, p), n in sorted(phases.items())]
        lines.append(f"# TYPE {PREFIX}spend_usd_per_hour gauge")
        lines += [
            _line("spend_usd_per_hour", _labels(vendor=v), spend.get(v, 0.0))
            for v in sorted({vendor for vendor, _ in phases})
        ]
        lines.append(f"# TYPE {PREFIX}node_age_seconds gauge")
        lines += [_line("node_age_seconds", labels, age) for labels, age in sorted(node_age.items())]
        for vendor, age in self._orphans:
            databases[vendor].append(age)
        for vendor in settings.vendors:
            databases.setdefault(vendor, [])  # a vendor without databases reports 0, not nothing
        lines.append(f"# TYPE {PREFIX}dbaas_instances gauge")
        lines += [_line("dbaas_instances", _labels(vendor=v), len(ages)) for v, ages in sorted(databases.items())]
        lines.append(f"# TYPE {PREFIX}dbaas_oldest_age_seconds gauge")
        lines += [
            _line("dbaas_oldest_age_seconds", _labels(vendor=v), max(ages, default=0))
            for v, ages in sorted(databases.items())
        ]
        lines.append(f"# TYPE {PREFIX}dbaas_tagged_databases gauge")
        lines += [_line("dbaas_tagged_databases", _labels(vendor=v), t.count) for v, t in sorted(self._tagged.items())]
        lines.append(f"# TYPE {PREFIX}dbaas_tagged_oldest_age_seconds gauge")
        lines += [
            _line(
                "dbaas_tagged_oldest_age_seconds",
                _labels(vendor=v),
                (now - t.oldest_created).total_seconds() if t.oldest_created else 0,
            )
            for v, t in sorted(self._tagged.items())
        ]
        lines.append(f"# TYPE {PREFIX}dbaas_inventory_last_success_timestamp_seconds gauge")
        lines += [
            _line(
                "dbaas_inventory_last_success_timestamp_seconds",
                _labels(vendor=v),
                self._inventory_success[v].timestamp() if v in self._inventory_success else 0,
            )
            for v in sorted(settings.dbaas.vendors)
        ]
        lines.append(f"# TYPE {PREFIX}publish_backlog gauge")
        lines.append(_line("publish_backlog", (), backlog))
        lines.append(f"# TYPE {PREFIX}publish_lag_seconds gauge")
        lag = (now - oldest_unpublished).total_seconds() if oldest_unpublished else 0
        lines.append(_line("publish_lag_seconds", (), lag))
        lines.append(f"# TYPE {PREFIX}launch_paused gauge")
        lines += [
            _line("launch_paused", _labels(vendor=v), launch.paused(self._kube, v, now))
            for v in sorted(settings.vendors)
        ]
        for name, counts in (("launch_deferred_skus", deferred), ("launch_blocked", blocked)):
            lines.append(f"# TYPE {PREFIX}{name} gauge")
            lines += [_line(name, _labels(vendor=v, reason=r), n) for (v, r), n in sorted(counts.items())]
        self._gauges = lines


class _Handler(BaseHTTPRequestHandler):
    metrics: Metrics  # set on the subclass made by `make_server`

    def do_GET(self) -> None:
        if self.path == "/metrics":
            body = self.metrics.render().encode()
            content_type = "text/plain; version=0.0.4; charset=utf-8"
        elif self.path == "/healthz":
            body, content_type = b"ok\n", "text/plain; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A002 (stdlib signature)
        pass  # probes every few seconds; not worth a log line


def make_server(metrics: Metrics, port: int = PORT) -> ThreadingHTTPServer:
    """`/metrics` and `/healthz` on `port` (0 = any free port, for tests). Caller runs `serve_forever()` in a
    thread and `shutdown()` + `server_close()` at the end."""
    handler = type("Handler", (_Handler,), {"metrics": metrics})
    server = ThreadingHTTPServer(("", port), handler)
    server.daemon_threads = True
    return server
