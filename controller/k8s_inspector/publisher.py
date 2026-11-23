"""Publisher: render finished tasks into sc-inspector-data, commit, push, mark published. The only git writer.

Implements docs/explanation/outputs.md "Publishing to git", state.md "Disaster recovery", output-contract.md
"Commits". Git through the `git` CLI via `subprocess` (blobless, shallow, sparse checkout in `workdir/repo`; the
deploy key from `env.git_key_file` via `GIT_SSH_COMMAND`).

One cycle: tasks with `state: Done` and `published: false` in all BenchmarkRuns -> `git fetch` +
`reset --hard origin/<branch>` -> for each task read `status.json` and the outputs
(`Storage.read_json` / `read_bytes`), `render.render_task` (+ `render_variant`, `render_timing`) -> one
commit `k8s-inspector: publish <n> results (<run ids>)` -> push; on non-fast-forward start again at the fetch
(rendering is a pure function of the inputs read once plus the tree, so there is no rebase or conflict handling)
-> `crd.update_status` sets `published: true` and `commit: <sha>` on those tasks (and `status.variant`); a run
whose tasks are all published gets the condition `Published` and, in phase Publishing, moves to phase Done.
Idempotent: a crash between push and patch re-renders the same content, finds no diff and patches.

Decisions:
- P1 safety: a SKU is published only when `settings.vendors[<vendor>]` has `enabled` and `publish` is true or a
  list containing the SKU (`publish: false` is the default). Without settings nothing is published. Finished runs
  that will never be published are deleted `publish.keep_run_hours` after their last task ended, which frees the
  SKU lease.
- The hardware variant comes from the lscpu stdout of the attempt that ran the task (`run.json`
  `attempts[].lscpu_stdout`, attempt = `status.json` `attempt`), matched against the `variant.json` files in
  the checkout (`variant.pick_variant`). Without a usable lscpu result a task with a negative exit code (no
  node, never finished), or 256 in a run that never had a node, goes to `schedule/<task>/meta.json` only (so
  the planner does not plan it again); a task with exit code >= 0 stays unpublished
  (and is retried every cycle) rather than being filed under the wrong variant, until `publish.keep_run_hours`
  after its end: then it is marked published without a commit, logged as an error and counted in
  `PublishResult.unpublishable` (`k8s_inspector_unpublishable_tasks_total`), so the run frees its lease. When the
  run's `lscpu` task failed and every task is done, no lscpu result will come: a failed task without an lscpu
  result of its attempt goes to `schedule/<task>/meta.json` only (like a negative code; the planner then plans by
  its exit code: -4 retried after its wait, 256 final), and a successful one is given up at once (marked
  published without a commit, counted as unpublishable): its hardware is unknown, so it is never filed under a
  guessed variant. The SKU then waits for the launch backoff (`NO_LSCPU`). A forced
  run of a SKU that git does not know, without lscpu (a typo), publishes nothing and is marked published
  without a commit.
- A `-4 CANCELLED` task of a run an operator cancelled (phase `Cancelled`) is discarded at once, whatever the lscpu
  result (marked published without a commit, not counted as unpublishable): nothing is written, so the SKU's
  previous `schedule/<task>/meta.json` stays authoritative and the cancel is no start retry. Such a run never
  defers its SKU (`NO_LSCPU`). A task of such a run that finished (exit code 0 or above) without an lscpu result of
  its attempt is discarded the same way: no lscpu will come, so it is neither filed under a guessed variant nor
  waited for. Other `-4` reasons, and a `CANCELLED` of a SIGTERM in a run that was not cancelled,
  go to `schedule/<task>/meta.json` only (render.py), never over a good result.
- A task whose rendering keeps failing is published without parser output after `MAX_RENDER_FAILURES` cycles.
- vLLM probe keys (`vllm-probe-<image>`) are not tasks of the data tree: they are marked published unrendered.
- Timing files come from `run.json`: the latest attempt that has an lscpu result (`attempts[]`: `api_start`,
  `api_end`, `machine_start`, `node_registered`, `node_ready`), else its `timing` object, completed with
  `inspector_start`/`inspector_end` = first task start / last task end. They go to the variant of that attempt.
- `rescan` finds the SKUs of a vendor from the git tree (`data/<vendor>/*`): a SKU never published has to be
  passed with `skus`. A DBaaS run is recognised by `run.json` `database`.
"""

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path

from k8s_inspector import crd, launch, render, tasks
from k8s_inspector import storage as storage_keys
from k8s_inspector import variant as variants
from k8s_inspector.kube import Kube, NotFound
from k8s_inspector.model import (
    BenchmarkRun,
    EnvConfig,
    ExitCode,
    Reason,
    RunKey,
    RunPhase,
    RunStatus,
    Settings,
    TaskDef,
    TaskState,
)
from k8s_inspector.storage import Storage

log = logging.getLogger(__name__)

GIT_TIMEOUT_SECONDS = 600  # one git command; a hung network fails the cycle instead of the thread
SSH_OPTIONS = "-o ConnectTimeout=30 -o ServerAliveInterval=15 -o ServerAliveCountMax=4"
MAX_PUSH_ATTEMPTS = 5
MAX_RENDER_FAILURES = 12  # cycles a task may fail to render before it is published without parser output
DONE_PHASES = (RunPhase.PUBLISHING, RunPhase.DONE, RunPhase.FAILED, RunPhase.CANCELLED)
KNOWN_HOSTS = Path(__file__).parent / "github_known_hosts"  # GitHub's published SSH host keys
VLLM_PROBE_PREFIX = "vllm-probe-"
TIMING_NAMES = (
    "api_start",
    "api_end",
    "machine_start",
    "node_registered",
    "node_ready",
    "inspector_start",
    "inspector_end",
)
AUTHOR = {"GIT_AUTHOR_NAME": "k8s-inspector", "GIT_AUTHOR_EMAIL": "infra-ops+k8s-inspector@sparecores.com"}


@dataclass(frozen=True)
class PublishResult:
    tasks_published: int
    commit: str | None  # None when nothing changed
    run_ids: tuple[str, ...]
    unpublishable: int = 0  # tasks given up on: finished, but no lscpu result to pick a variant from


@dataclass
class _Item:
    """One finished task to publish. `run_name` is None for rescan items (no BenchmarkRun)."""

    run: RunKey
    key: str
    run_name: str | None = None
    database: dict | None = None
    forced: bool = False
    end: datetime | None = None  # TaskStatus.end / .attempt as read: `_mark` marks only when they are unchanged
    attempt: int | None = None
    run_times: tuple[tuple[datetime | None, datetime | None], ...] = ()  # start/end of every task of the run
    status: dict | None = None  # status.json when already read (rescan)
    no_lscpu_coming: bool = False  # the run's lscpu task failed and every task is done: no lscpu result will come
    cancelled: bool = False  # the run was cancelled by an operator (phase Cancelled)


@dataclass
class _Loaded:
    item: _Item
    inp: render.RenderInput | None  # None: vLLM probe, nothing to render
    lscpu: bytes | None = None  # stdout of the run's lscpu task, when it succeeded
    lscpu_status: dict | None = None
    run_json: dict | None = None
    variant: int | None = None


def vendor_allowed(settings: Settings, vendor: str) -> bool:
    """The vendor is enabled and publishes at least one SKU."""
    vendor_settings = settings.vendors.get(vendor)
    return bool(vendor_settings and vendor_settings.enabled and vendor_settings.publish)


def publish_allowed(settings: Settings, vendor: str, sku: str) -> bool:
    """P1 safety: publish a SKU only when its vendor is enabled and `publish` is true or lists the SKU."""
    vendor_settings = settings.vendors.get(vendor)
    if not (vendor_settings and vendor_settings.enabled):
        return False
    return vendor_settings.publish is True or (
        isinstance(vendor_settings.publish, tuple) and sku in vendor_settings.publish
    )


def _publishable(settings: Settings, run: BenchmarkRun) -> bool:
    """The run's SKU, and for a DBaaS run its database, may be published."""
    database = run.spec.database
    return publish_allowed(settings, run.spec.vendor, run.spec.sku) and (
        not database or publish_allowed(settings, database["vendor"], database["id"])
    )


def _safe_part(value: str) -> str:
    """A path component taken from the cluster or the bucket must not escape the tree."""
    if not value or value in (".", "..") or "/" in value or "\\" in value or "\x00" in value:
        raise ValueError(f"unsafe path component {value!r}")
    return value


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _all_published(run: BenchmarkRun) -> bool:
    tasks_ = run.status.tasks
    return bool(tasks_) and all(t.state == TaskState.DONE and t.published for t in tasks_)


def _finished_at(run: BenchmarkRun) -> datetime:
    """When the run's last task ended (the run's creation when none did)."""
    ends = [_utc(t.end) for t in run.status.tasks if t.end]
    return max(ends, default=_utc(run.created))


def publishing_stalled(settings: Settings, vendor: str, runs: list[BenchmarkRun]) -> bool:
    """A run of `vendor` in phase Publishing (publishable) has a finished task that stayed unpublished for more
    than two publish intervals: pushes keep failing, so the planner starts no new run for the vendor."""
    limit = timedelta(minutes=2 * settings.publish.interval_minutes)
    now = datetime.now(UTC)
    return any(
        run.spec.vendor == vendor
        and run.status.phase == RunPhase.PUBLISHING
        and _publishable(settings, run)
        and any(t.state == TaskState.DONE and not t.published and t.end and now - _utc(t.end) > limit
                for t in run.status.tasks)
        for run in runs
    )  # fmt: skip


def _published_at(run: BenchmarkRun) -> datetime | None:
    for condition in run.status.conditions:
        if condition.get("type") == "Published" and condition.get("lastTransitionTime"):
            return _utc(datetime.fromisoformat(condition["lastTransitionTime"]))
    return None


class Publisher:
    def __init__(
        self,
        kube: Kube,
        storage: Storage,
        env: EnvConfig,
        workdir: Path,
        *,
        settings_provider: Callable[[], Settings] | None = None,
        get_task: Callable[[str], TaskDef] = tasks.get_task,
    ) -> None:
        """`settings_provider` supplies the vendor allowlist when `publish_once()`/`rescan()` get no settings."""
        self._kube = kube
        self._storage = storage
        self._env = env
        self._repo = Path(workdir) / "repo"
        self._render_failures: dict[tuple[RunKey, str], int] = {}
        self._settings_provider = settings_provider
        self._get_task = get_task
        self._lock = threading.Lock()  # one cycle at a time (run_forever, CLI, trigger)
        self._wake = threading.Event()

    # ------------------------------------------------------------------ public

    def publish_once(self, settings: Settings | None = None) -> PublishResult:
        """One publish cycle as described in the module docstring."""
        with self._lock:
            settings = self._settings(settings)
            if settings is None:
                return PublishResult(0, None, ())
            runs = self._delete_temporary_failures(crd.list_runs(self._kube))
            for run in runs:
                if run.status.phase == RunPhase.PUBLISHING and _all_published(run):
                    # everything was published while the run was still releasing: nothing is left to publish
                    try:
                        crd.update_status(self._kube, run.name, partial(_mark, seen={}, sha=None, variant=None))
                    except Exception:
                        log.exception("cannot finish run %s, leaving it in phase Publishing", run.name)
            items = []
            for run in runs:
                items.extend(self._items_of(run, settings))
            return self._publish(items, settings)

    def rescan(self, since_days: int, skus: Iterable[str] = (), settings: Settings | None = None) -> PublishResult:
        """Disaster recovery (`publish --rescan --since 7d`): list recent date prefixes of the enabled
        vendors' SKUs in object storage (`Storage.list_status_keys`), publish every task with a
        `status.json` that git does not have (the stale-start guard skips what it has)."""
        with self._lock:
            settings = self._settings(settings)
            if settings is None:
                return PublishResult(0, None, ())
            vendors = [v for v in settings.vendors if vendor_allowed(settings, v)]
            self._sync([])
            today = datetime.now(UTC).date()
            items = []
            for vendor in vendors:
                listed = settings.vendors[vendor].publish
                vendor_skus = {
                    *skus,
                    *(listed if isinstance(listed, tuple) else ()),
                    *self._git_names(f"data/{vendor}"),
                }
                for sku in sorted(vendor_skus - {"schedule"}):
                    if not publish_allowed(settings, vendor, sku):
                        continue
                    for days_back in range(since_days + 1):
                        items.extend(self._items_in_bucket(settings, vendor, sku, today - timedelta(days=days_back)))
            return self._publish(items, settings)

    def _delete_temporary_failures(self, runs: list[BenchmarkRun]) -> list[BenchmarkRun]:
        """Delete the Failed runs of earlier versions whose tasks all ended `-1` with a temporary launch reason
        (now such a run is deleted by the runner and never recorded). Returns the other runs."""
        kept = []
        for run in runs:
            tasks_ = run.status.tasks
            if (
                run.status.phase == RunPhase.FAILED
                and tasks_
                and all(t.exit_code == ExitCode.NO_NODE and launch.is_temporary(t.error_msg) for t in tasks_)
            ):
                crd.delete_run(self._kube, run.name)
                log.info("deleted run %s: temporary launch failure, nothing to publish", run.name)
            else:
                kept.append(run)
        return kept

    def delete_expired_runs(self, settings: Settings) -> int:
        """Delete BenchmarkRuns whose tasks are all published, `publish.keep_run_hours` after the last one,
        and finished runs that will never be published (SKU not allowed, see `publish_allowed`) that long
        after their last task ended: deleting a run frees its SKU lease. Without the ConfigMap the allowlist is
        the default (nothing allowed), so unpublished runs are kept then. Returns how many."""
        keep = timedelta(hours=settings.publish.keep_run_hours)
        now = datetime.now(UTC)
        deleted = 0
        for run in crd.list_runs(self._kube):
            published_at = _published_at(run)
            if _all_published(run) and published_at:
                since, why = published_at, "published"
            elif settings.from_configmap and run.status.phase in DONE_PHASES and not _publishable(settings, run):
                since, why = _finished_at(run), "never to be published"
            else:
                continue
            if now - since >= keep:
                crd.delete_run(self._kube, run.name)
                log.info("deleted %s run %s", why, run.name)
                deleted += 1
        return deleted

    def run_forever(self, settings_provider, stop: threading.Event) -> None:
        """Every `publish.interval_minutes` (and when `trigger()` is called): `publish_once`,
        `delete_expired_runs`; exceptions are logged, never fatal."""
        while not stop.is_set():
            interval = 300.0
            try:
                settings = settings_provider()
                interval = settings.publish.interval_minutes * 60
                result = self.publish_once(settings)
                if result.tasks_published:
                    log.info("published %d results (%s)", result.tasks_published, ", ".join(result.run_ids))
                self.delete_expired_runs(settings)
            except Exception:
                log.exception("publish cycle failed")
            deadline = time.monotonic() + interval
            while not (stop.is_set() or self._wake.is_set() or time.monotonic() >= deadline):
                self._wake.wait(1.0)
            self._wake.clear()

    def trigger(self) -> None:
        """Wake `run_forever` for an on-demand cycle (publish CLI / run completion)."""
        self._wake.set()

    # ------------------------------------------------------------------ what to publish

    def _settings(self, settings: Settings | None) -> Settings | None:
        if settings is None and self._settings_provider is not None:
            settings = self._settings_provider()
        if settings is None:
            log.error("no settings: publishing nothing (a vendor must be enabled explicitly)")
        return settings

    def _items_of(self, run: BenchmarkRun, settings: Settings) -> list[_Item]:
        pending = [t for t in run.status.tasks if t.state == TaskState.DONE and not t.published]
        if not pending:
            return []
        if not _publishable(settings, run):
            log.warning("run %s: not enabled for publishing, leaving %d tasks", run.name, len(pending))
            return []
        key = crd.run_key(run)
        times = tuple((t.start, t.end) for t in run.status.tasks)
        no_lscpu_coming = _no_lscpu_coming(run)
        if no_lscpu_coming:
            log.warning(
                "run %s: its lscpu task failed and every task is done: tasks without an lscpu result of their "
                "attempt go to schedule/ only (failures) or are given up (successes)",
                run.name,
            )
        return [
            _Item(
                key,
                t.name,
                run.name,
                run.spec.database,
                run.spec.force,
                t.end,
                t.attempt,
                times,
                no_lscpu_coming=no_lscpu_coming,
                cancelled=run.status.phase == RunPhase.CANCELLED,
            )
            for t in pending
        ]

    def _items_in_bucket(self, settings: Settings, vendor: str, sku: str, day: date) -> list[_Item]:
        prefix = storage_keys.day_prefix(vendor, sku, day)
        by_run: dict[str, list[tuple[str, dict]]] = {}
        for object_key in self._storage.list_status_keys(prefix):
            run_id, _, rest = object_key.removeprefix(prefix).partition("/tasks/")
            key = rest.removesuffix("/status.json")
            run = RunKey(vendor, sku, day, run_id)
            status = self._storage.read_json(run, key, "status.json")
            if status is not None:
                by_run.setdefault(run_id, []).append((key, status))
        items = []
        for run_id, statuses in by_run.items():
            run = RunKey(vendor, sku, day, run_id)
            database = (self._read_run_json(run) or {}).get("database")
            if database and not publish_allowed(settings, database["vendor"], database["id"]):
                continue
            times = tuple((_parse_status_time(s.get("start")), _parse_status_time(s.get("end"))) for _, s in statuses)
            items.extend(_Item(run, key, database=database, run_times=times, status=status) for key, status in statuses)
        return items

    # ------------------------------------------------------------------ reading from object storage

    def _load(self, item: _Item, cache: dict[RunKey, tuple]) -> _Loaded | None:
        if item.key.startswith(VLLM_PROBE_PREFIX):
            return _Loaded(item, None)
        status = item.status or self._storage.read_json(item.run, item.key, "status.json")
        if status is None:
            log.warning("%s/%s: no status.json yet", item.run.run_id, item.key)
            return None
        task = self._get_task(item.key)
        stdout = self._storage.read_bytes(storage_keys.object_key(item.run, item.key, "stdout")) or b""
        stderr = self._storage.read_bytes(storage_keys.object_key(item.run, item.key, "stderr")) or b""
        if item.run not in cache:
            cache[item.run] = self._read_run_facts(item.run)
        run_json, legacy_status = cache[item.run]
        lscpu, lscpu_status = _lscpu_of(status, run_json, legacy_status)
        return _Loaded(
            item, render.RenderInput(item.run, task, item.key, status, stdout, stderr), lscpu, lscpu_status, run_json
        )

    def _read_run_json(self, run: RunKey) -> dict | None:
        raw = self._storage.read_bytes(storage_keys.run_json_key(run))
        return json.loads(raw) if raw else None

    def _read_run_facts(self, run: RunKey) -> tuple[dict | None, dict | None]:
        """`run.json` (attempts, timing) and the status of the shared `tasks/lscpu` task (`first_seen` and
        version of the variant, used when it is from the attempt that ran the task)."""
        return self._read_run_json(run), self._storage.read_json(run, "lscpu", "status.json")

    # ------------------------------------------------------------------ the cycle

    def _publish(self, items: list[_Item], settings: Settings) -> PublishResult:
        items = sorted(items, key=lambda i: (i.run.vendor, i.run.sku, i.run.run_id, i.key))
        cache: dict[RunKey, tuple] = {}
        loaded: list[_Loaded] = []
        for item in items:
            try:
                for part in (item.run.vendor, item.run.sku, item.run.run_id, item.key):
                    _safe_part(part)
                one = self._load(item, cache)
            except Exception:
                log.exception("cannot read %s/%s, skipping", item.run.run_id, item.key)
                continue
            if one:
                loaded.append(one)
        if not loaded:
            return PublishResult(0, None, ())

        for attempt in range(1, MAX_PUSH_ATTEMPTS + 1):
            self._sync(self._sparse_dirs(loaded))
            keep = timedelta(hours=settings.publish.keep_run_hours)
            rendered, discarded, unpublishable = self._render_all(loaded, keep)
            commit = self._commit(rendered)
            if commit is None:
                sha = self._git("rev-parse", "HEAD").strip()
                break
            if self._push():
                sha = commit
                break
            log.warning("push rejected (attempt %d), starting again from a fresh fetch", attempt)
        else:
            raise RuntimeError(f"push rejected {MAX_PUSH_ATTEMPTS} times")
        finished = self._mark_published(rendered, sha) | self._mark_published(discarded + unpublishable, None)
        self._defer_skus_without_lscpu([one for one, _ in rendered] + unpublishable, finished)
        run_ids = tuple(sorted({r[0].item.run.run_id for r in rendered}))
        return PublishResult(len(rendered), commit, run_ids, sum(1 for one in unpublishable if one.item.run_name))

    def _defer_skus_without_lscpu(self, published: list[_Loaded], finished: set[str]) -> None:
        """A run ended without an lscpu result and became fully published in this cycle (`finished`: run names): its
        SKU waits for the launch backoff (`NO_LSCPU`) before the planner starts it again, as its successful tasks
        left no result for the planner to see. Not for a forced run: it is the operator's."""
        runs = {
            (one.item.run, one.item.run_name)
            for one in published
            if one.item.no_lscpu_coming and not one.item.forced and one.item.run_name in finished
        }
        for run, _ in runs:
            launch.record_failure(self._kube, run.vendor, run.sku, "NO_LSCPU", datetime.now(UTC))

    def _render_all(
        self, loaded: list[_Loaded], keep: timedelta
    ) -> tuple[list[tuple[_Loaded, int | None]], list[_Loaded], list[_Loaded]]:
        """Render into the checkout; returns the tasks that are done (rendered, or skipped as stale), the ones
        to mark published without a commit (nothing to publish) and the unpublishable ones (no variant to file
        them under: `keep` after their end, or at once when the run's lscpu task failed)."""
        rendered = []
        discarded = []
        unpublishable = []
        first_with_variant: dict[RunKey, _Loaded] = {}
        for one in loaded:
            item = one.item
            if one.inp is None:
                rendered.append((one, None))
                continue
            if item.cancelled and _is_cancelled(one.inp.status):
                log.info(
                    "%s/%s: cancelled by an operator before it finished, nothing to publish", item.run.run_id, item.key
                )
                discarded.append(one)  # the SKU's previous result stays authoritative
                continue
            failed = (item.run, item.key)
            try:
                variant = self._render_one(one, degraded=self._render_failures.get(failed, 0) >= MAX_RENDER_FAILURES)
            except Exception:
                self._render_failures[failed] = self._render_failures.get(failed, 0) + 1
                log.exception("cannot render %s/%s, leaving it unpublished", item.run.run_id, item.key)
                continue
            self._render_failures.pop(failed, None)
            if variant is _DISCARDED:
                discarded.append(one)
            elif variant is _SKIPPED:
                end = _parse_status_time(one.inp.status.get("end"))
                if item.cancelled:
                    log.info(
                        "%s/%s: finished in a cancelled run without an lscpu result, nothing to publish",
                        item.run.run_id,
                        item.key,
                    )
                    discarded.append(one)  # no lscpu will come: not worth blocking the vendor for keep_run_hours
                elif item.no_lscpu_coming:
                    log.error("%s/%s: succeeded without an lscpu result, not publishable", item.run.run_id, item.key)
                    unpublishable.append(one)
                elif end and datetime.now(UTC) - end >= keep:
                    log.error("%s/%s: no lscpu result after %s, not publishable", item.run.run_id, item.key, keep)
                    unpublishable.append(one)
            else:
                one.variant = variant
                rendered.append((one, variant))
                if variant is not None:
                    first_with_variant.setdefault(item.run, one)
        for one in first_with_variant.values():
            self._render_timing(one)
        return rendered, discarded, unpublishable

    def _render_one(self, one: _Loaded, *, degraded: bool = False):
        item, inp = one.item, one.inp
        if degraded:
            log.warning("%s/%s: publishing without parser output after repeated failures", item.run.run_id, item.key)
            inp = replace(inp, task=replace(inp.task, parse=None, transforms=("raw",)))
        if item.database:
            render.render_dbaas_task(self._repo, item.database, inp)
            return None
        run = item.run
        variant = None
        fields = None
        if one.lscpu is not None:
            fields = variants.parse_lscpu_stdout(one.lscpu)
            variant = variants.pick_variant(self._existing_variants(run.vendor, run.sku), fields)
        elif _waits_for_lscpu(one):
            log.warning("%s/%s: no lscpu result for the task's attempt, cannot pick a variant", run.run_id, item.key)
            return _SKIPPED  # a success of unknown hardware is never filed under a guessed variant
        elif item.forced and not (self._repo / "data" / run.vendor / run.sku).exists():
            log.warning("%s/%s: forced run of a SKU git does not know, not publishing", run.run_id, item.key)
            return _DISCARDED
        render.render_task(self._repo, run.vendor, run.sku, variant, inp)
        if variant is not None:  # after the task: a task that fails to render leaves no variant behind
            status = one.lscpu_status or {}
            render.render_variant(
                self._repo, run.vendor, run.sku, variant, one.lscpu,
                first_seen=status.get("end"), lscpu_version=status.get("version"),
            )  # fmt: skip
        return variant

    def _render_timing(self, one: _Loaded) -> None:
        """`timing/<run_id>/*` in the variant of the attempt the timings are from."""
        item, run = one.item, one.item.run
        attempt = _timing_attempt(one.run_json)
        variant = one.variant
        if attempt:
            fields = variants.parse_lscpu_stdout(attempt["lscpu_stdout"])
            variant = variants.pick_variant(self._existing_variants(run.vendor, run.sku), fields)
            if not (self._repo / "data" / run.vendor / run.sku / str(variant) / "variant.json").exists():
                return
        render.render_timing(self._repo, run.vendor, run.sku, variant, run, self._timings(item, one.run_json))

    def _existing_variants(self, vendor: str, sku: str) -> dict[int, dict]:
        found = {}
        for path in (self._repo / "data" / vendor / sku).glob("[0-9]*/variant.json"):
            if path.parent.name.isdigit():
                found[int(path.parent.name)] = json.loads(path.read_text())
        return found

    @staticmethod
    def _timings(item: _Item, run_json: dict | None) -> dict[str, str]:
        source = _timing_attempt(run_json) or (run_json or {}).get("timing") or {}
        timings = {}
        for name, value in source.items():
            if name in TIMING_NAMES and value:
                timings[name] = render.format_timing_utc(datetime.fromisoformat(value))
        starts = [_utc(s) for s, _ in item.run_times if s]
        ends = [_utc(e) for _, e in item.run_times if e]
        defaults = {
            "inspector_start": min(starts) if starts else None,
            "inspector_end": max(ends) if ends else None,
        }
        for name, value in defaults.items():
            if value and name not in timings:
                timings[name] = render.format_timing_utc(value)
        return timings

    # ------------------------------------------------------------------ marking published

    def _mark_published(self, rendered: list, sha: str | None) -> set[str]:
        """Mark tasks published (`rendered`: `(loaded, variant)` pairs or bare `_Loaded`s) with `sha`. Returns the
        names of the runs whose tasks are all published now."""
        finished = set()
        by_run: dict[str, list[tuple[_Item, int | None]]] = {}
        for entry in rendered:
            one, variant = entry if isinstance(entry, tuple) else (entry, None)
            if one.item.run_name:
                by_run.setdefault(one.item.run_name, []).append((one.item, variant))
        for name, entries in by_run.items():
            seen = {item.key: (item.end, item.attempt) for item, _ in entries}
            variant = next((v for _, v in entries if v is not None), None)
            try:
                run = crd.update_status(self._kube, name, partial(_mark, seen=seen, sha=sha, variant=variant))
            except NotFound:
                log.warning("run %s vanished before it could be marked published", name)
            else:
                if _all_published(run):
                    finished.add(name)
        return finished

    # ------------------------------------------------------------------ git

    def _git(self, *args: str, check: bool = True, cwd: Path | None = None) -> str:
        return self._run_git(args, check, cwd).stdout

    def _run_git(self, args: tuple[str, ...], check: bool, cwd: Path | None = None) -> subprocess.CompletedProcess:
        env = {**os.environ, **AUTHOR, "GIT_TERMINAL_PROMPT": "0"}
        env["GIT_COMMITTER_NAME"], env["GIT_COMMITTER_EMAIL"] = AUTHOR["GIT_AUTHOR_NAME"], AUTHOR["GIT_AUTHOR_EMAIL"]
        if self._has_deploy_key():
            # GitHub's host keys ship with the package; any other host key is refused
            env["GIT_SSH_COMMAND"] = (
                f"ssh -i {shlex.quote(str(self._private_key()))} -o IdentitiesOnly=yes "
                f"-o StrictHostKeyChecking=yes -o UserKnownHostsFile={shlex.quote(str(KNOWN_HOSTS))} {SSH_OPTIONS}"
            )
        try:
            return subprocess.run(
                ["git", "-c", "commit.gpgsign=false", *args],
                cwd=cwd or self._repo,
                env=env,
                capture_output=True,
                text=True,
                check=check,
                timeout=GIT_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as e:  # fails the cycle; the next one starts from a fresh fetch
            raise RuntimeError(f"git {args[0]} timed out after {GIT_TIMEOUT_SECONDS} s") from e

    def _private_key(self) -> Path:
        """Copy of the deploy key in the workdir, mode 0600 and owned by this process: a Secret mount is root-owned
        with mode 0440 (fsGroup), which ssh may refuse. Copied again when the Secret changes (rotation)."""
        source = Path(str(self._env.git_key_file))
        copy = self._repo.parent / "deploy-key"
        data = source.read_bytes()
        if not copy.is_file() or copy.read_bytes() != data or copy.stat().st_mode & 0o777 != 0o600:
            copy.parent.mkdir(parents=True, exist_ok=True)
            copy.unlink(missing_ok=True)
            fd = os.open(copy, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
        return copy

    def _has_deploy_key(self) -> bool:
        """The deploy key Secret is an optional mount: the file may be absent."""
        return bool(self._env.git_key_file) and Path(str(self._env.git_key_file)).is_file()

    def _sync(self, sparse: list[str], *, cone: bool = True) -> None:
        """Fresh checkout of origin/<branch> restricted to `sparse`: directories (plus the repo root files), or with
        `cone=False` file patterns."""
        url = self._env.git_url
        if (url.startswith("ssh://") or re.match(r"^[\w.-]+@[\w.-]+:", url)) and not self._has_deploy_key():
            raise RuntimeError(
                f"K8SI_GIT_URL {url} is an SSH URL but there is no deploy key: set K8SI_GIT_KEY_FILE "
                f"to a mounted key file (currently {self._env.git_key_file or 'unset'})"
            )
        branch = self._env.git_branch
        if not self._checkout_usable():
            if not self._env.git_url:
                raise RuntimeError("K8SI_GIT_URL is not set")
            shutil.rmtree(self._repo, ignore_errors=True)  # a broken or half-cloned checkout
            self._repo.parent.mkdir(parents=True, exist_ok=True)
            try:
                self._git(
                    "clone", "--filter=blob:none", "--depth", "1", "--branch", branch, "--sparse",
                    self._env.git_url, str(self._repo), cwd=self._repo.parent,
                )  # fmt: skip
            except Exception:  # failed or timed out: leave no partial checkout behind
                shutil.rmtree(self._repo, ignore_errors=True)
                raise
        self._git("fetch", "--depth", "1", "origin", branch)
        self._git("reset", "--hard", f"origin/{branch}")
        self._git("clean", "-fd")
        self._git("sparse-checkout", "set", "--cone" if cone else "--no-cone", *sparse)

    def _checkout_usable(self) -> bool:
        """The checkout exists and git can resolve its `HEAD`."""
        if not (self._repo / ".git").exists():
            return False
        try:
            return all(
                self._run_git(args, False).returncode == 0
                for args in (("rev-parse", "--git-dir"), ("rev-parse", "--verify", "-q", "HEAD"))
            )
        except RuntimeError:  # timeout
            return False

    def read_schedule_metas(self, vendor: str) -> dict[str, dict[str, dict | None]]:
        """`{sku: {task: meta}}` from the `data/<vendor>/<sku>/schedule/<task>/meta.json` files of origin/<branch>,
        for the planner. Only those files are checked out (the other outputs are never downloaded), in the
        publisher's checkout, under its lock. A malformed file is logged and its meta is None: the planner then
        skips that SKU (reading it as "never run" would plan the task again)."""
        _safe_part(vendor)
        with self._lock:
            self._sync([f"/data/{vendor}/*/schedule/*/meta.json"], cone=False)
            found: dict[str, dict[str, dict | None]] = {}
            for path in (self._repo / "data" / vendor).glob("*/schedule/*/meta.json"):
                try:
                    meta = json.loads(path.read_text())
                    if not isinstance(meta, dict):
                        raise ValueError("not a JSON object")
                except ValueError as e:  # includes JSONDecodeError and UnicodeDecodeError
                    log.warning("malformed %s: %s", path.relative_to(self._repo), e)
                    meta = None
                found.setdefault(path.parts[-4], {})[path.parts[-2]] = meta
            return found

    def _git_names(self, path: str) -> list[str]:
        """Entry names under `path` in origin/<branch> (no checkout needed)."""
        out = self._git("ls-tree", "--name-only", f"origin/{self._env.git_branch}", f"{path}/")
        return [line.rsplit("/", 1)[-1] for line in out.splitlines()]

    @staticmethod
    def _sparse_dirs(loaded: list[_Loaded]) -> list[str]:
        dirs = set()
        for one in loaded:
            run = one.item.run
            dirs.add(f"data/{run.vendor}/{run.sku}")
            if one.item.database:
                db = one.item.database
                dirs.add(f"dbaas/{_safe_part(db['vendor'])}/{_safe_part(db['id'])}")
        return sorted(dirs)

    def _commit(self, rendered: list[tuple[_Loaded, int | None]]) -> str | None:
        """Commit what was rendered (only below data/ and dbaas/); the new SHA, None without changes."""
        roots = [d for d in ("data", "dbaas") if (self._repo / d).exists()]
        if not roots:
            return None
        self._git("add", "-A", "--", *roots)
        if not self._git("status", "--porcelain").strip():
            return None
        run_ids = sorted({one.item.run.run_id for one, _ in rendered})
        self._git("commit", "-m", f"k8s-inspector: publish {len(rendered)} results ({', '.join(run_ids)})")
        return self._git("rev-parse", "HEAD").strip()

    def _push(self) -> bool:
        """True when pushed, False on a non-fast-forward rejection; other failures raise."""
        result = self._run_git(("push", "origin", f"HEAD:{self._env.git_branch}"), check=False)
        if result.returncode == 0:
            return True
        if any(text in result.stderr for text in ("rejected", "non-fast-forward", "fetch first")):
            return False
        raise RuntimeError(f"git push failed: {result.stderr.strip()[-500:]}")


_SKIPPED = object()  # a task that cannot be published yet (no variant)
_DISCARDED = object()  # a task with nothing to publish: marked published without a commit


def _attempt_entry(run_json: dict | None, n: int | None) -> dict | None:
    return next((a for a in (run_json or {}).get("attempts") or [] if n is not None and a.get("attempt") == n), None)


def _lscpu_of(status: dict, run_json: dict | None, legacy_status: dict | None):
    """`(lscpu stdout, lscpu status)` of the attempt that ran the task: `run.json` attempts[n].lscpu_stdout.
    The status is the shared `tasks/lscpu` one when it is from the same attempt (a later attempt overwrites it)."""
    n = status.get("attempt")
    entry = _attempt_entry(run_json, n)
    if entry and entry.get("lscpu_stdout"):
        same = legacy_status and legacy_status.get("attempt") == n
        return entry["lscpu_stdout"].encode(), legacy_status if same else None
    return None, None


def _no_lscpu_coming(run: BenchmarkRun) -> bool:
    """The run's lscpu task failed (or was rejected) and every task is done, so no lscpu result will come. Not for
    a run an operator cancelled: its unfinished lscpu says nothing about the SKU."""
    lscpu = next((t for t in run.status.tasks if t.name == "lscpu"), None)
    return (
        run.status.phase != RunPhase.CANCELLED
        and lscpu is not None
        and lscpu.exit_code != ExitCode.OK
        and all(t.state == TaskState.DONE for t in run.status.tasks)
    )


def _is_cancelled(status: dict) -> bool:
    """`-4 CANCELLED`. Only in a run of phase Cancelled is it an operator's cancel: a SIGTERM of a step Pod (node
    shutdown, external delete) is also `CANCELLED` (`pods.outcome_from_message`) and is published as a `-4`."""
    return status.get("exit_code") == ExitCode.NEVER_FINISHED and status.get("error_msg") == Reason.CANCELLED


def _waits_for_lscpu(one: _Loaded) -> bool:
    """A task without an lscpu result that can only be filed once there is one (`_SKIPPED`): exit code 0 or above
    on a node. Not a failure of a run whose lscpu will never come: it goes to `schedule/` only, like a negative code."""
    status = one.inp.status
    failed = one.item.no_lscpu_coming and status.get("exit_code") != ExitCode.OK
    return status.get("exit_code", 0) >= 0 and not _harness_error_without_node(status, one.run_json) and not failed


def _harness_error_without_node(status: dict, run_json: dict | None) -> bool:
    """A `256` of a run that never had a node: no lscpu will ever come, so it is filed like a negative code
    (meta only) and git records it."""
    attempts = (run_json or {}).get("attempts") or []
    return status.get("exit_code") == ExitCode.HARNESS_ERROR and not any(a.get("node") for a in attempts)


def _timing_attempt(run_json: dict | None) -> dict | None:
    """The latest attempt of the run that got as far as an lscpu result."""
    done = [a for a in (run_json or {}).get("attempts") or [] if a.get("lscpu_stdout")]
    return max(done, key=lambda a: a["attempt"], default=None)


def _parse_status_time(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text).replace(tzinfo=UTC) if text else None


def _mark(status: RunStatus, *, seen: dict[str, tuple], sha: str | None, variant: int | None) -> None:
    """`crd.update_status` mutation: tasks published with the commit, the run's variant, Published condition.
    Only tasks that are still Done with the `(end, attempt)` the publisher read: a task the runner recorded
    again meanwhile has a new result that was not published."""
    for task in status.tasks:
        if task.name in seen and task.state == TaskState.DONE and (task.end, task.attempt) == seen[task.name]:
            task.published = True
            task.commit = sha
    if variant is not None:
        status.variant = variant
    if status.tasks and all(t.state == TaskState.DONE and t.published for t in status.tasks):
        status.conditions = [c for c in status.conditions if c.get("type") != "Published"]
        status.conditions.append(
            {
                "type": "Published",
                "status": "True",
                "reason": "AllTasksPublished",
                "lastTransitionTime": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            }
        )
        if status.phase == RunPhase.PUBLISHING:
            status.phase = RunPhase.DONE
