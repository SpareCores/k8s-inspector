"""Command line: `k8s-inspector serve | plan | run | status | cancel | publish` (configuration.md "CLI").

All commands except `serve` run one-shot against the cluster (inside it via
`kubectl exec deploy/k8s-inspector -- k8s-inspector ...`).

Exit codes: 0 success; 1 the operation failed or was refused (validation, run exists, unknown run, a thread of
`serve` crashed); 2 usage or configuration error (argparse, missing environment variable, empty
`K8SI_SC_RUN_IMAGE`, invalid ConfigMap).

`serve`: loads the configuration (`ConfigProvider` re-reads the ConfigMap every 30 s and keeps the last good
one when it becomes invalid), then runs the runner, the planner, the publisher and the `/metrics` + `/healthz`
endpoint (port 9090) in threads. SIGTERM/SIGINT set one `threading.Event`; every loop returns and the process
exits 0. A thread that dies on its own stops the others and `serve` exits 1, so Kubernetes restarts the Pod.
"""

import argparse
import logging
import os
import re
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from k8s_inspector import config as config_loader
from k8s_inspector import crd, planner
from k8s_inspector.kube import Kube, NotFound, TransientApiError, call_with_retries
from k8s_inspector.log import setup_logging
from k8s_inspector.metrics import PORT as METRICS_PORT
from k8s_inspector.metrics import Metrics, make_server
from k8s_inspector.model import (
    BenchmarkRun,
    CapacityMode,
    Config,
    EnvConfig,
    Settings,
    TaskState,
)
from k8s_inspector.publisher import Publisher, PublishResult
from k8s_inspector.runner import Runner
from k8s_inspector.storage import Storage

log = logging.getLogger(__name__)

CONFIG_TTL_SECONDS = 30.0
SHUTDOWN_TIMEOUT_SECONDS = 30.0

_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> timedelta:
    """`"2h"`, `"90m"`, `"7d"`, `"0s"` -> timedelta. Raise `argparse.ArgumentTypeError` otherwise."""
    m = _DURATION.match(text)
    if not m:
        raise argparse.ArgumentTypeError(f"invalid duration {text!r} (use e.g. 90s, 30m, 2h, 7d)")
    return timedelta(seconds=int(m.group(1)) * _UNITS[m.group(2)])


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="k8s-inspector", description="Spare Cores benchmark controller")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("serve", help="run the controller (planner, runner, publisher)")

    plan = sub.add_parser("plan", help="run one planning cycle")
    plan.add_argument("--dry-run", action="store_true", help="print what would be started")

    run = sub.add_parser("run", help="start a forced run of one SKU")
    run.add_argument("vendor")
    run.add_argument("sku")
    run.add_argument(
        "--tasks", type=lambda s: [t for t in s.split(",") if t], default=[], help="comma-separated task names"
    )
    run.add_argument("--force", action="store_true", help="skip Keeper and the due check")
    run.add_argument("--on-demand", action="store_true", help="do not use spot")
    run.add_argument("--hold", type=parse_duration, default=timedelta(0), help="keep the node after the last step")

    status = sub.add_parser("status", help="list runs")
    status.add_argument("--failing", action="store_true", help="only failing tasks")

    cancel = sub.add_parser("cancel", help="cancel a run")
    cancel.add_argument("run")

    pub = sub.add_parser("publish", help="publish finished tasks now")
    pub.add_argument("--rescan", action="store_true", help="recover from object storage after a cluster loss")
    pub.add_argument("--since", type=parse_duration, default=timedelta(days=7))
    pub.add_argument("--sku", action="append", default=[], help="with --rescan: a SKU never published (repeatable)")
    return p


class CliError(Exception):
    """Printed as `error: <message>` on stderr; `exit_code` becomes the process exit code."""

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class ConfigProvider:
    """Zero-argument callable returning the current `Config`: the environment is read once, the ConfigMap at
    most every `ttl` seconds. A failing reload (API error, invalid YAML) keeps the previous settings."""

    def __init__(
        self, kube: Kube, env: EnvConfig, ttl: float = CONFIG_TTL_SECONDS, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._kube, self._env, self._ttl, self._clock = kube, env, ttl, clock
        self._lock = threading.Lock()
        self._settings: Settings | None = None
        self._loaded_at = 0.0

    def __call__(self) -> Config:
        with self._lock:
            if self._settings is None or self._clock() - self._loaded_at >= self._ttl:
                try:
                    self._settings = config_loader.load_settings(self._kube, self._env.namespace)
                except Exception:
                    if self._settings is None:
                        raise
                    log.exception("cannot reload the ConfigMap, keeping the previous settings")
                self._loaded_at = self._clock()
            return Config(env=self._env, settings=self._settings)


def _load_env(environ: Mapping[str, str]) -> EnvConfig:
    try:
        return config_loader.load_env(environ)
    except ValueError as e:
        raise CliError(str(e), 2) from e


def _require_run_image(env: EnvConfig) -> None:
    if not env.sc_run_image:
        raise CliError("K8SI_SC_RUN_IMAGE is empty: set it to the digest-pinned sc-run image", 2)


def _connect(*, need_run_image: bool = False) -> tuple[Kube, ConfigProvider]:
    env = _load_env(os.environ)
    if need_run_image:
        _require_run_image(env)
    kube = Kube(env.namespace)
    provider = ConfigProvider(kube, env)
    try:
        provider()  # fail on an invalid ConfigMap now, not in the first cycle
    except ValueError as e:
        raise CliError(str(e), 2) from e
    return kube, provider


# --------------------------------------------------------------------------- serve


class _MeteredPublisher(Publisher):
    """Records every publish cycle that did not raise, for `k8s_inspector_last_publish_timestamp_seconds`."""

    def __init__(self, *args, metrics: Metrics, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._metrics = metrics

    def publish_once(self, settings: Settings | None = None) -> PublishResult:
        result = super().publish_once(settings)
        self._metrics.record_publish()
        self._metrics.record_unpublishable(result.unpublishable)
        return result


def serve(kube: Kube, provider: ConfigProvider, stop: threading.Event, metrics_port: int = METRICS_PORT) -> int:
    """Run all components until `stop` is set; 0 on a clean stop, 1 when a component died."""
    cfg = provider()
    env = cfg.env
    storage = Storage(env, cfg.settings.uploads)
    metrics = Metrics(kube, lambda: provider().settings)
    runner = Runner(kube, storage, provider, env.namespace, on_launch_failure=metrics.record_launch_failure)
    publisher = _MeteredPublisher(
        kube,
        storage,
        env,
        Path(os.environ.get("HOME", "/work")) / "publisher",
        settings_provider=lambda: provider().settings,
        metrics=metrics,
    )
    server = make_server(metrics, metrics_port)

    crashed: list[str] = []

    def guard(name: str, target: Callable[[], None]) -> Callable[[], None]:
        def run() -> None:
            try:
                target()
            except Exception:
                log.exception("%s thread died", name)
            if not stop.is_set():  # returned or raised before shutdown: the controller is not whole any more
                crashed.append(name)
                stop.set()

        return run

    targets = {
        "runner": lambda: runner.run_forever(stop),
        "planner": lambda: planner.run_forever(kube, provider, publisher.read_schedule_metas, stop),
        "publisher": lambda: publisher.run_forever(lambda: provider().settings, stop),
        "metrics": server.serve_forever,
    }
    threads = [threading.Thread(target=guard(n, t), name=n, daemon=True) for n, t in targets.items()]
    for thread in threads:
        thread.start()
    log.info("controller started", extra={"namespace": env.namespace, "metrics_port": server.server_address[1]})

    stop.wait()
    log.info("shutting down")
    server.shutdown()
    deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
        if thread.is_alive():
            log.warning("%s thread did not stop in time", thread.name)
    server.server_close()
    if crashed:
        log.error("exiting: %s died", ", ".join(crashed))
        return 1
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    kube, provider = _connect(need_run_image=True)
    stop = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        return serve(kube, provider, stop)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


# --------------------------------------------------------------------------- one-shot commands


def cmd_plan(args: argparse.Namespace) -> int:
    kube, provider = _connect()
    cfg = provider()
    with tempfile.TemporaryDirectory(prefix="k8si-plan-") as workdir:  # its own checkout, like `publish`
        publisher = Publisher(kube, Storage(cfg.env, cfg.settings.uploads), cfg.env, Path(workdir))
        planned = planner.plan_cycle(kube, cfg, publisher.read_schedule_metas, dry_run=args.dry_run)
    for run in planned:
        print(f"{run.vendor} {run.sku} {','.join(run.tasks)} ({run.reason})")
    if not planned:
        print("nothing to plan")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """`planner.force_run`; print the run name. Exit 1 on `RunExists` / `ValueError`."""
    kube, provider = _connect(need_run_image=True)
    try:
        run = planner.force_run(
            kube,
            provider(),
            args.vendor,
            args.sku,
            tasks=args.tasks,
            force=args.force,
            capacity=CapacityMode.ON_DEMAND if args.on_demand else None,
            hold_after=args.hold,
        )
    except crd.RunExists:
        name = crd.run_name(args.vendor, args.sku)
        raise CliError(
            f"a run for {args.vendor}/{args.sku} exists: {name} (follow it with kubectl, or cancel it with "
            f"`k8s-inspector cancel {name}`)"
        ) from None
    except ValueError as e:
        raise CliError(str(e)) from e
    print(f"tasks ({len(run.spec.tasks)}): {', '.join(run.spec.tasks)}", file=sys.stderr)
    print(run.name)
    return 0


def _age(created: datetime, now: datetime) -> str:
    seconds = max(0, int((now - created).total_seconds()))
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def _table(header: list[str], rows: list[list[str]]) -> str:
    widths = [max(len(row[i]) for row in [header, *rows]) for i in range(len(header))]
    return "\n".join(
        "  ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=True)).rstrip() for row in [header, *rows]
    )


def render_status(runs: list[BenchmarkRun], now: datetime, failing: bool = False) -> str:
    """Compact table. Default: one row per run (the printer columns of the BenchmarkRun plus the name).
    `failing`: one row per task that finished with a non-zero exit code."""
    runs = sorted(runs, key=lambda r: (r.spec.vendor, r.spec.sku))
    if failing:
        rows = [
            [run.name, task.name, str(task.exit_code), task.error_msg or ""]
            for run in runs
            for task in run.status.tasks
            if task.state == TaskState.DONE and task.exit_code not in (None, 0)
        ]
        return _table(["RUN", "TASK", "EXIT", "ERROR"], rows) if rows else "no failing tasks"
    if not runs:
        return "no runs"
    rows = [
        [
            run.name,
            run.spec.vendor,
            run.spec.sku,
            run.status.phase.value,
            run.status.progress,
            run.status.current_step or "-",
            run.status.capacity_type.value if run.status.capacity_type else "-",
            str(run.status.attempt),
            _age(run.created, now),
        ]
        for run in runs
    ]
    return _table(["NAME", "VENDOR", "SKU", "PHASE", "PROGRESS", "STEP", "CAPACITY", "ATTEMPT", "AGE"], rows)


def cmd_status(args: argparse.Namespace) -> int:
    kube, _ = _connect()
    print(render_status(crd.list_runs(kube), datetime.now(UTC), failing=args.failing))
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    """Request the cancel (`crd.request_cancel`: an annotation) on the BenchmarkRun and return: the `serve` runner ends
    the unfinished tasks `-4 CANCELLED`, deletes the Pods and keeps the BenchmarkRun."""
    kube, _ = _connect()
    try:
        run = crd.get_run(kube, args.run)
    except NotFound:
        raise CliError(f"no such run: {args.run}") from None
    if run.status.phase not in planner.ACTIVE_PHASES:
        print(f"{run.name} is not active (phase {run.status.phase.value}); nothing to cancel")
        return 0
    crd.request_cancel(kube, run.name)
    print(f"cancel requested for {run.name}; the serve process performs it")
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    """One publish cycle (or `--rescan`) in a throwaway checkout, so it never shares a working tree with the
    `serve` process."""
    kube, provider = _connect()
    cfg = provider()
    storage = Storage(cfg.env, cfg.settings.uploads)
    with tempfile.TemporaryDirectory(prefix="k8si-publish-") as workdir:
        publisher = Publisher(kube, storage, cfg.env, Path(workdir), settings_provider=lambda: cfg.settings)
        try:
            result = publisher.rescan(args.since.days, skus=args.sku) if args.rescan else publisher.publish_once()
        except Exception as e:
            log.exception("publish failed")
            raise CliError(f"publish failed: {e}") from e
    print(f"published {result.tasks_published} tasks" + (f" in commit {result.commit}" if result.commit else ""))
    return 0


# One-shot commands retry transient API errors (a 429, a 5xx, the API server (re)initializing storage after a CRD
# update, a dropped connection) a few times; `serve` and `plan` have their own loops. `run` retries inside
# `crd.create_run` and `publish` is never re-run.
RETRIED_COMMANDS = {"status", "cancel"}

COMMANDS = {
    "serve": cmd_serve,
    "plan": cmd_plan,
    "run": cmd_run,
    "status": cmd_status,
    "cancel": cmd_cancel,
    "publish": cmd_publish,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging()
    try:
        command = COMMANDS[args.command]
        if args.command in RETRIED_COMMANDS:
            return call_with_retries(args.command, lambda: command(args))
        return command(args)
    except TransientApiError as e:
        print(f"error: Kubernetes API unavailable after {e.attempts} attempts: {e.__cause__}", file=sys.stderr)
        return 1
    except CliError as e:
        print(f"error: {e}", file=sys.stderr)
        return e.exit_code
