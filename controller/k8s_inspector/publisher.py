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
- P1 safety: a vendor is published only when `settings.vendors[<vendor>]` has `enabled` and `publish` true (not
  `publish: false`, the default). Without settings nothing is published.
- The hardware variant comes from the run's own `lscpu` task (`tasks/lscpu/{status.json,stdout}`), matched against
  the `variant.json` files in the checkout (`variant.pick_variant`). Without a usable lscpu result a task with a
  negative exit code (no node, never finished) goes to `schedule/<task>/meta.json` only; a task with exit code
  >= 0 stays unpublished (and is retried every cycle) rather than being filed under the wrong variant.
- vLLM probe keys (`vllm-probe-<image>`) are not tasks of the data tree: they are marked published unrendered.
- Timing files come from `run.json` key `timing` ({name: ISO timestamp}, written by the runner: `api_end`,
  `machine_start`, `node_registered`, `node_ready`, ...), completed with `api_start` = run creation and
  `inspector_start`/`inspector_end` = first task start / last task end.
- `rescan` finds the SKUs of a vendor from the git tree (`data/<vendor>/*`): a SKU never published has to be
  passed with `skus`.
"""

import json
import logging
import os
import shlex
import subprocess
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path

from k8s_inspector import crd, render, tasks
from k8s_inspector import storage as storage_keys
from k8s_inspector import variant as variants
from k8s_inspector.kube import Kube, NotFound
from k8s_inspector.model import (
    BenchmarkRun,
    EnvConfig,
    RunKey,
    RunPhase,
    RunStatus,
    Settings,
    TaskDef,
    TaskState,
)
from k8s_inspector.storage import Storage

log = logging.getLogger(__name__)

MAX_PUSH_ATTEMPTS = 5
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


@dataclass
class _Item:
    """One finished task to publish. `run_name` is None for rescan items (no BenchmarkRun)."""

    run: RunKey
    key: str
    run_name: str | None = None
    database: dict | None = None
    created: datetime | None = None
    run_times: tuple[tuple[datetime | None, datetime | None], ...] = ()  # start/end of every task of the run
    status: dict | None = None  # status.json when already read (rescan)


@dataclass
class _Loaded:
    item: _Item
    inp: render.RenderInput | None  # None: vLLM probe, nothing to render
    lscpu: bytes | None = None  # stdout of the run's lscpu task, when it succeeded
    lscpu_status: dict | None = None
    run_json: dict | None = None


def vendor_allowed(settings: Settings, vendor: str) -> bool:
    """P1 safety: publish a vendor only when it is explicitly enabled in the ConfigMap."""
    vendor_settings = settings.vendors.get(vendor)
    return bool(vendor_settings and vendor_settings.enabled and vendor_settings.publish)


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
        self._known_hosts = Path(workdir) / "known_hosts"
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
            runs = crd.list_runs(self._kube)
            items = []
            for run in runs:
                if run.deleting:
                    continue
                items.extend(self._items_of(run, settings))
            return self._publish(items)

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
                vendor_skus = {*skus, *self._git_names(f"data/{vendor}")} - {"schedule"}
                for sku in sorted(vendor_skus):
                    for days_back in range(since_days + 1):
                        items.extend(self._items_in_bucket(vendor, sku, today - timedelta(days=days_back)))
            return self._publish(items)

    def delete_expired_runs(self, settings: Settings) -> int:
        """Delete BenchmarkRuns whose tasks are all published, `publish.keep_run_hours` after the last one.
        Returns how many."""
        keep = timedelta(hours=settings.publish.keep_run_hours)
        now = datetime.now(UTC)
        deleted = 0
        for run in crd.list_runs(self._kube):
            published_at = _published_at(run)
            if not run.deleting and _all_published(run) and published_at and now - published_at >= keep:
                crd.delete_run(self._kube, run.name)
                log.info("deleted published run %s", run.name)
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
        vendors = {run.spec.vendor, *([run.spec.database["vendor"]] if run.spec.database else [])}
        pending = [t for t in run.status.tasks if t.state == TaskState.DONE and not t.published]
        if not pending:
            return []
        if not all(vendor_allowed(settings, v) for v in vendors):
            log.warning("run %s: vendor not enabled for publishing, leaving %d tasks", run.name, len(pending))
            return []
        key = crd.run_key(run)
        times = tuple((t.start, t.end) for t in run.status.tasks)
        return [_Item(key, t.name, run.name, run.spec.database, run.created, times) for t in pending]

    def _items_in_bucket(self, vendor: str, sku: str, day: date) -> list[_Item]:
        prefix = f"logs/inspect/{vendor}/{sku}/{day:%Y/%m/%d}/"
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
            times = tuple((_parse_status_time(s.get("start")), _parse_status_time(s.get("end"))) for _, s in statuses)
            items.extend(
                _Item(RunKey(vendor, sku, day, run_id), key, run_times=times, status=status) for key, status in statuses
            )
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
        lscpu, lscpu_status, run_json = cache[item.run]
        return _Loaded(
            item, render.RenderInput(item.run, task, item.key, status, stdout, stderr), lscpu, lscpu_status, run_json
        )

    def _read_run_facts(self, run: RunKey) -> tuple[bytes | None, dict | None, dict | None]:
        """The run's lscpu result (identifies the hardware variant) and `run.json` (timing)."""
        lscpu_status = self._storage.read_json(run, "lscpu", "status.json")
        lscpu = None
        if lscpu_status and lscpu_status.get("exit_code") == 0:
            lscpu = self._storage.read_bytes(storage_keys.object_key(run, "lscpu", "stdout"))
        run_json = None
        raw = self._storage.read_bytes(storage_keys.run_json_key(run))
        if raw:
            run_json = json.loads(raw)
        return lscpu, lscpu_status, run_json

    # ------------------------------------------------------------------ the cycle

    def _publish(self, items: list[_Item]) -> PublishResult:
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
            rendered = self._render_all(loaded)
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
        self._mark_published(rendered, sha)
        return PublishResult(len(rendered), commit, tuple(sorted({r[0].item.run.run_id for r in rendered})))

    def _render_all(self, loaded: list[_Loaded]) -> list[tuple[_Loaded, int | None]]:
        """Render into the checkout; returns the tasks that are done (rendered, or skipped as stale)."""
        rendered = []
        timed: set[RunKey] = set()
        for one in loaded:
            item = one.item
            if one.inp is None:
                rendered.append((one, None))
                continue
            try:
                variant = self._render_one(one, timed)
            except Exception:
                log.exception("cannot render %s/%s, leaving it unpublished", item.run.run_id, item.key)
                continue
            if variant is not _SKIPPED:
                rendered.append((one, variant))
        return rendered

    def _render_one(self, one: _Loaded, timed: set[RunKey]):
        item, inp = one.item, one.inp
        if item.database:
            render.render_dbaas_task(self._repo, item.database, inp)
            return None
        run = item.run
        variant = None
        if one.lscpu is not None:
            fields = variants.parse_lscpu_stdout(one.lscpu)
            variant = variants.pick_variant(self._existing_variants(run.vendor, run.sku), fields)
            status = one.lscpu_status or {}
            render.render_variant(
                self._repo, run.vendor, run.sku, variant, one.lscpu,
                first_seen=status.get("end"), lscpu_version=status.get("version"),
            )  # fmt: skip
        elif inp.status.get("exit_code", 0) >= 0:
            log.warning("%s/%s: no lscpu result for the run, cannot pick a variant yet", run.run_id, item.key)
            return _SKIPPED
        render.render_task(self._repo, run.vendor, run.sku, variant, inp)
        if variant is not None and run not in timed:
            timed.add(run)
            timings = self._timings(item, one.run_json)
            render.render_timing(self._repo, run.vendor, run.sku, variant, run, timings)
        return variant

    def _existing_variants(self, vendor: str, sku: str) -> dict[int, dict]:
        found = {}
        for path in (self._repo / "data" / vendor / sku).glob("[0-9]*/variant.json"):
            if path.parent.name.isdigit():
                found[int(path.parent.name)] = json.loads(path.read_text())
        return found

    @staticmethod
    def _timings(item: _Item, run_json: dict | None) -> dict[str, str]:
        timings = {}
        for name, value in ((run_json or {}).get("timing") or {}).items():
            if name in TIMING_NAMES and value:
                timings[name] = render.format_timing_utc(datetime.fromisoformat(value))
        starts = [_utc(s) for s, _ in item.run_times if s]
        ends = [_utc(e) for _, e in item.run_times if e]
        defaults = {
            "api_start": item.created,
            "inspector_start": min(starts) if starts else None,
            "inspector_end": max(ends) if ends else None,
        }
        for name, value in defaults.items():
            if value and name not in timings:
                timings[name] = render.format_timing_utc(value)
        return timings

    # ------------------------------------------------------------------ marking published

    def _mark_published(self, rendered: list[tuple[_Loaded, int | None]], sha: str) -> None:
        by_run: dict[str, list[tuple[str, int | None]]] = {}
        for one, variant in rendered:
            if one.item.run_name:
                by_run.setdefault(one.item.run_name, []).append((one.item.key, variant))
        for name, entries in by_run.items():
            keys = {key for key, _ in entries}
            variant = next((v for _, v in entries if v is not None), None)
            try:
                crd.update_status(self._kube, name, partial(_mark, keys=keys, sha=sha, variant=variant))
            except NotFound:
                log.warning("run %s vanished before it could be marked published", name)

    # ------------------------------------------------------------------ git

    def _git(self, *args: str, check: bool = True, cwd: Path | None = None) -> str:
        return self._run_git(args, check, cwd).stdout

    def _run_git(self, args: tuple[str, ...], check: bool, cwd: Path | None = None) -> subprocess.CompletedProcess:
        env = {**os.environ, **AUTHOR, "GIT_TERMINAL_PROMPT": "0"}
        env["GIT_COMMITTER_NAME"], env["GIT_COMMITTER_EMAIL"] = AUTHOR["GIT_AUTHOR_NAME"], AUTHOR["GIT_AUTHOR_EMAIL"]
        if self._env.git_key_file:
            # accept-new: trust the host key on first contact, then pin it in the workdir
            env["GIT_SSH_COMMAND"] = (
                f"ssh -i {shlex.quote(self._env.git_key_file)} -o IdentitiesOnly=yes "
                f"-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile={shlex.quote(str(self._known_hosts))}"
            )
        return subprocess.run(
            ["git", "-c", "commit.gpgsign=false", *args],
            cwd=cwd or self._repo,
            env=env,
            capture_output=True,
            text=True,
            check=check,
        )

    def _sync(self, sparse_dirs: list[str]) -> None:
        """Fresh checkout of origin/<branch> restricted to `sparse_dirs` (plus the repo root files)."""
        branch = self._env.git_branch
        if not (self._repo / ".git").exists():
            if not self._env.git_url:
                raise RuntimeError("K8SI_GIT_URL is not set")
            self._repo.parent.mkdir(parents=True, exist_ok=True)
            self._git(
                "clone", "--filter=blob:none", "--depth", "1", "--branch", branch, "--sparse",
                self._env.git_url, str(self._repo), cwd=self._repo.parent,
            )  # fmt: skip
        self._git("fetch", "--depth", "1", "origin", branch)
        self._git("reset", "--hard", f"origin/{branch}")
        self._git("clean", "-fd")
        self._git("sparse-checkout", "set", *sparse_dirs)

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


def _parse_status_time(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text).replace(tzinfo=UTC) if text else None


def _mark(status: RunStatus, *, keys: set[str], sha: str, variant: int | None) -> None:
    """`crd.update_status` mutation: tasks published with the commit, the run's variant, Published condition."""
    for task in status.tasks:
        if task.name in keys:
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
