"""The run state machine: a watch-driven reconciler for BenchmarkRuns.

Implements docs/explanation/run-model.md, state.md ("How a restarted controller knows what is ongoing"),
outputs.md "In the controller" and spot.md "How k8s-inspector reacts".

Threading: one worker thread drains a queue of run names, so `reconcile` never runs concurrently with itself
and needs no locks. Producers: the BenchmarkRun and Pod watches, a 5 minute re-LIST (which also notices runs
that were deleted while a watch was down) and `enqueue`. `reconcile` never blocks waiting for a Pod.

`reconcile(name)` is level-triggered and idempotent: it reads the BenchmarkRun and its labelled Pods, decides
the next action, does it and returns. Nothing but caches lives in memory, so a new `Runner` continues any run
from its objects. All object names are deterministic and `AlreadyExists` means "already done".

Where the state is:
- `status.phase`, `status.attempt`, `status.attempts`, `status.tasks`: the BenchmarkRun.
- `status.current_step`: name of the step Pod created last (`07-geekbench`, `07-geekbench-cpu` after the GPU
  fallback); its tasks are `Running` until `Done`. The step itself is recomputed with `steps.*` (they are pure),
  the next step's number is `current_step`'s plus one. The probe step is number 1 of every attempt.
- The in-flight multipart uploads: the urls Secret (`<run>-urls`), which holds only the current step's keys.

Phases:
- Pending: choose capacity (`spot.capacity_for_next_attempt`), create the anchor, record the attempt.
- Provisioning: anchor Running+ready on a Node created after it -> record region, zone, capacity type, price
  and node timings, phase Running. A Node older than the anchor: delete anchor and NodeClaim, new attempt.
  A NodeClaim capacity/quota error or the provisioning deadline: remaining tasks end `-1 SCARPENTER_*`, Failed.
- Running: no step in flight -> prechecks (`-3`), vLLM ladder failures, next step (`steps.probe_step` first,
  which always contains `lscpu`, then `steps.next_step`): sign URLs into the Secret, heartbeat, create the Pod.
  Step Pod finished -> `pods.classify`, `storage.finish_task`, a `flush` Pod (max 2) while uploads are
  incomplete, then `status.json` per task (the commit point) and `status.tasks`. A step that cannot start its GPU
  runtime is recreated once without it. A step stuck pulling its image -> `-4 IMAGE_PULL_TIMEOUT`. Every
  `uploads.resign_every_minutes` the Secret is re-signed. No step left -> Releasing.
- Anchor, Node or NodeClaim lost while Running: `spot.classify_node_loss`, salvage the in-flight tasks, close the
  attempt. A spot interruption starts the next attempt with the unfinished tasks (the in-flight task runs
  again); any other loss, or too many attempts, ends the unfinished tasks as `-4 NODE_LOST|SPOT_INTERRUPTED`.
- Releasing: honour `spec.hold_after`, write `run.json`, delete anchor and Secret, phase Publishing (the
  publisher takes over and sets Done).
- Cancelled/Failed/Publishing/Done: only leftover anchors are deleted.

Cancel: `cancel()` (and a run found with a deletion timestamp) records `-4 CANCELLED` for every unfinished task,
deletes the Pods and sets phase Cancelled. The BenchmarkRun is kept for the publisher: the CRD has no finalizer,
so a plain `kubectl delete` leaves no object to record anything on (leftover Pods are still deleted).
"""

import base64
import contextlib
import json
import logging
import queue
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, pods, render, spot, steps, tasks, variant
from k8s_inspector.kube import AlreadyExists, Kube, NotFound
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    BenchmarkRun,
    CapacityType,
    Config,
    ExitCode,
    NodeFacts,
    OwnerRef,
    PodRole,
    Reason,
    RunPhase,
    RunStatus,
    Step,
    StepContainer,
    TaskDef,
    TaskOutcome,
    TaskState,
    TaskStatus,
    Verification,
)
from k8s_inspector.storage import Storage, object_key, render_urls_json, run_json_key, split_urls_key

log = logging.getLogger(__name__)

LABEL_RUN = crd.LABEL_RUN
RESYNC_SECONDS = 300
FLUSH_STEPS = 2  # flush Pods per step before the outputs are given up
MAX_PROVISION_RETRIES = 3  # attempts lost before a node was usable (stale node, anchor deleted)
EXTRA_ATTEMPTS = 2  # attempts beyond `max_spot_attempts` (the on-demand one plus one spare)
BACKOFF_SECONDS = (2, 5, 15, 30, 60)

# Kubernetes and SCarpenter names read from Node and NodeClaim.
NODEPOOL_LABEL = "karpenter.sh/nodepool"
CAPACITY_LABEL = "karpenter.sh/capacity-type"
INSTANCE_TYPE_LABEL = "node.kubernetes.io/instance-type"
SCARPENTER = "scarpenter.sparecores.com"
PRICE_ANNOTATION = f"{SCARPENTER}/price"
NODECLASS_HASH_ANNOTATION = f"{SCARPENTER}/nodeclass-hash"
ACCELERATOR_MANUFACTURER_LABEL = f"{SCARPENTER}/accelerator-manufacturer"
GPU_RESOURCE = "nvidia.com/gpu"

_UNITS = {
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
    "k": 10**3,
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
}


class _Stale(Exception):
    """The run changed phase under this pass (cancel from the CLI, a restart race): drop the pass."""


@dataclass
class _Node:
    """The attempt's node: facts for planning plus what the NodeClaim said. A cache, rebuilt from the cluster."""

    attempt: int
    facts: NodeFacts
    gpu_count: int = 0
    claim: dict | None = None


@dataclass
class _Result:
    """The final record of one task, input of `status.json` and `status.tasks`."""

    task: TaskDef
    key: str
    exit_code: int
    error_msg: str | None = None
    outcome: TaskOutcome | None = None
    verification: Verification | None = None
    image: str | None = None
    pod: str | None = None


# --------------------------------------------------------------------------- small pure helpers


def _time(text: str | None) -> datetime | None:
    if not text:
        return None
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _quantity(text) -> int:
    """Kubernetes quantity ("15Gi", "16000000Ki", "4", "3920m") as an integer."""
    text = str(text)
    if text.endswith("m"):
        return int(float(text[:-1]) / 1000)
    for suffix, factor in _UNITS.items():
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    return int(float(text))


def _meta(obj: dict) -> dict:
    return obj.get("metadata", {})


def _condition_time(claim: dict | None, kind: str) -> datetime | None:
    for condition in (claim or {}).get("status", {}).get("conditions") or []:
        if condition.get("type") == kind and condition.get("status") == "True":
            return _time(condition.get("lastTransitionTime"))
    return None


def _interrupted(claim: dict | None) -> bool:
    """The NodeClaim carries an interruption notice (annotation or `Interrupted` condition)."""
    if not claim:
        return False
    annotations = _meta(claim).get("annotations") or {}
    conditions = claim.get("status", {}).get("conditions") or []
    return bool(annotations.get(spot.INTERRUPTION_ANNOTATION)) or any(
        c.get("type") == spot.INTERRUPTED_CONDITION and c.get("status") == "True" for c in conditions
    )


def scarpenter_error(message: str, default: str = "ICE") -> str:
    """`SCARPENTER_<ICE|QUOTA|QUOTA_ZERO|NO_OFFERING|BOOT_FAILURE>: <message>` from a NodeClaim message. The
    code is a keyword guess; `default` applies when nothing matches."""
    text = message.lower()
    if re.search(r"no (matching )?offering|no instance type", text):
        code = "NO_OFFERING"
    elif "quota" in text and re.search(r"\b(zero|0)\b", text):
        code = "QUOTA_ZERO"
    elif "quota" in text or "limit exceeded" in text or "limitexceeded" in text:
        code = "QUOTA"
    elif re.search(r"capacity|insufficient|out of host", text):
        code = "ICE"
    else:
        code = default
    return f"SCARPENTER_{code}: {message}"


def _claim_error(claims: list[dict]) -> str | None:
    """The error of a NodeClaim that failed to launch or register, else None."""
    for claim in claims:
        for condition in claim.get("status", {}).get("conditions") or []:
            if condition.get("status") != "False" or condition.get("type") not in ("Launched", "Registered"):
                continue
            message = condition.get("message") or condition.get("reason") or "NodeClaim failed"
            return scarpenter_error(message, "BOOT_FAILURE" if condition["type"] == "Registered" else "ICE")
    return None


def parse_gpu_facts(stdout: bytes) -> tuple[int | None, tuple[int, int] | None]:
    """`gpu_facts` output, one line per GPU `<VRAM MiB>, <compute capability>` -> (minimum VRAM, minimum
    capability). Unreadable lines are skipped."""
    vram, cap = [], []
    for line in stdout.decode(errors="replace").splitlines():
        try:
            memory, capability = (part.strip() for part in line.split(","))
            major, _, minor = capability.partition(".")
            vram.append(int(float(memory)))
            cap.append((int(major), int(minor or 0)))
        except ValueError:
            continue
    return (min(vram) if vram else None), (min(cap) if cap else None)


def _find(status: RunStatus, name: str) -> TaskStatus | None:
    return next((t for t in status.tasks if t.name == name), None)


def _outcome_of(status: TaskStatus) -> TaskOutcome:
    """What `steps.next_step` needs of a finished task."""
    return TaskOutcome(key=status.name, exit_code=status.exit_code or 0, error_msg=status.error_msg)


def _step_index(name: str | None) -> int:
    return int(name[:2]) if name else 0


def _budget_steps(selected: list[TaskDef]) -> list[Step]:
    """Stand-ins for the steps of a run, only to size the anchor's deadline (`pods.step_deadline_seconds`)."""
    result = []
    probes = tuple(StepContainer(t.name, t, "") for t in selected if t.parallel)
    if probes:
        result.append(Step(1, "probes", probes))
    for task in selected:
        if not task.parallel:
            result.append(Step(2, task.name, (StepContainer(task.name, task, ""),)))
            for _ in task.images:  # the vLLM probes
                result.append(
                    Step(2, task.name, (StepContainer(task.name, task, "", timeout=steps.VLLM_PROBE_TIMEOUT),))
                )
    return result


class Runner:
    def __init__(
        self,
        kube: Kube,
        storage: Storage,
        cfg_provider,
        namespace: str,
        *,
        clock: Callable[[], datetime] | None = None,
        sku_shape: Callable[[str, str], tuple[bool, bool]] | None = None,
    ) -> None:
        """`cfg_provider`: zero-argument callable returning the current `Config` (settings are re-read from the
        ConfigMap every cycle, so a callable is passed instead of a snapshot). `clock` (tests) returns the
        current UTC time. `sku_shape(vendor, sku) -> (gpu, lowmem)` picks the NodePool; the default is the plain
        `cpu` pool (the planner knows the SKU, the runner does not)."""
        self.kube = kube
        self.storage = storage
        self.namespace = namespace
        self._cfg = cfg_provider
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sku_shape = sku_shape or (lambda vendor, sku: (False, False))
        self._queue: queue.Queue[str] = queue.Queue()
        self._lock = threading.Lock()
        self._queued: set[str] = set()
        self._known: set[str] = set()  # run names seen, to notice deletions between LISTs
        self._failures: dict[str, int] = {}
        self._nodes: dict[str, _Node] = {}  # cache per run
        self._signed_at: dict[str, datetime] = {}  # when the urls Secret was last signed, per run

    def check_config(self) -> None:
        """Fail fast when `K8SI_SC_RUN_IMAGE` is empty (anchor and flush Pods could not be created)."""
        if not self._cfg().env.sc_run_image:
            raise ValueError("K8SI_SC_RUN_IMAGE is empty: set it to the sc-run image reference")

    # ----------------------------------------------------------------------- public API

    def enqueue(self, run_name: str) -> None:
        """Ask for a reconcile of one run. Thread-safe; duplicates are harmless."""
        with self._lock:
            if run_name in self._queued:
                return
            self._queued.add(run_name)
        self._queue.put(run_name)

    def reconcile(self, run_name: str) -> None:
        """One reconcile pass of one run (see module docstring). Exceptions propagate: the worker loop logs
        them and re-queues the run with backoff; a bug in one run must not stop the others."""
        try:
            run = crd.get_run(self.kube, run_name)
        except NotFound:
            self._forget(run_name)
            return
        cfg = self._cfg()
        live = {_meta(p)["name"]: p for p in self.kube.list("pod", labels={LABEL_RUN: run.name})}
        handlers = {
            RunPhase.PENDING: self._pending,
            RunPhase.PROVISIONING: self._provisioning,
            RunPhase.RUNNING: self._running,
            RunPhase.RELEASING: self._releasing,
        }
        try:
            if run.deleting and run.status.phase in handlers:
                self._cancel(run, cfg, live)
            elif run.status.phase in handlers:
                handlers[run.status.phase](run, cfg, live)
            else:
                self._delete_anchors(live)  # finished: nothing may be left holding a node
        except _Stale:
            log.info("run %s changed under the reconcile, dropping the pass", run.name)

    def cancel(self, run_name: str) -> None:
        """Cancel the run: unfinished tasks end `-4 CANCELLED`, Pods are deleted, phase Cancelled. Synchronous (the
        CLI calls it from its own process). Does nothing for a run that is already over."""
        run = crd.get_run(self.kube, run_name)
        if run.status.phase in (RunPhase.PENDING, RunPhase.PROVISIONING, RunPhase.RUNNING, RunPhase.RELEASING):
            live = {_meta(p)["name"]: p for p in self.kube.list("pod", labels={LABEL_RUN: run.name})}
            self._cancel(run, self._cfg(), live)

    def run_forever(self, stop: threading.Event) -> None:
        """Start the watch threads (BenchmarkRuns, labelled Pods), the resync timer and the worker loop in this
        thread until `stop` is set. On start and after every watch end: LIST, enqueue all runs."""
        self.check_config()
        watches = [("benchmarkrun", None), ("pod", {LABEL_RUN: None})]  # None: label exists
        for kind, labels in watches:
            threading.Thread(target=self._watch, args=(kind, labels, stop), daemon=True, name=f"watch-{kind}").start()
        threading.Thread(target=self._resync_loop, args=(stop,), daemon=True, name="resync").start()
        self._resync()
        while not stop.is_set():
            try:
                name = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            with self._lock:
                self._queued.discard(name)
            self._work(name)

    # ----------------------------------------------------------------------- worker and watches

    def _work(self, name: str) -> None:
        try:
            self.reconcile(name)
            self._failures.pop(name, None)
        except Exception:
            failures = self._failures.get(name, 0)
            self._failures[name] = failures + 1
            delay = BACKOFF_SECONDS[min(failures, len(BACKOFF_SECONDS) - 1)]
            log.exception("reconcile of %s failed, retrying in %ds", name, delay)
            timer = threading.Timer(delay, self.enqueue, [name])
            timer.daemon = True
            timer.start()

    def _watch(self, kind: str, labels: dict | None, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                for event in self.kube.watch(kind, labels=labels):
                    if stop.is_set():
                        return
                    name = (
                        _meta(event.obj).get("name")
                        if kind == "benchmarkrun"
                        else (_meta(event.obj).get("labels") or {}).get(LABEL_RUN)
                    )
                    if name:
                        if kind == "benchmarkrun":
                            self._known.add(name)
                        self.enqueue(name)
            except Exception:
                log.warning("watch of %s ended, restarting", kind, exc_info=True)
            stop.wait(5.0)

    def _resync_loop(self, stop: threading.Event) -> None:
        while not stop.wait(RESYNC_SECONDS):
            try:
                self._resync()
            except Exception:
                log.warning("resync failed", exc_info=True)

    def _resync(self) -> None:
        """LIST the runs and enqueue all of them, and the ones that vanished since the last LIST (a re-listing
        watch yields no DELETED for them)."""
        names = {run.name for run in crd.list_runs(self.kube)}
        for name in names | self._known:
            self.enqueue(name)
        self._known = names

    def _forget(self, name: str) -> None:
        """The BenchmarkRun is gone: drop caches and delete the Pods that outlived it."""
        self._nodes.pop(name, None)
        self._signed_at.pop(name, None)
        self._known.discard(name)
        for pod in self.kube.list("pod", labels={LABEL_RUN: name}):
            self.kube.delete("pod", _meta(pod)["name"])

    # ----------------------------------------------------------------------- helpers: objects

    def _now(self) -> datetime:
        return self._clock()

    def _update(self, run: BenchmarkRun, mutate: Callable[[RunStatus], None]) -> BenchmarkRun:
        """`crd.update_status`, refused (`_Stale`) when the run left the phase this pass started in."""
        phase = run.status.phase

        def guarded(status: RunStatus) -> None:
            if status.phase != phase:
                raise _Stale
            mutate(status)

        return crd.update_status(self.kube, run.name, guarded)

    def _create(self, kind: str, manifest: dict) -> None:
        with contextlib.suppress(AlreadyExists):
            self.kube.create(kind, manifest)

    def _run_owner(self, run: BenchmarkRun) -> OwnerRef:
        return OwnerRef(crd.API_VERSION, crd.KIND, run.name, run.uid)

    def _anchor_owner(self, run: BenchmarkRun, live: dict[str, dict]) -> OwnerRef:
        anchor = live[pods.anchor_name(run.name, run.status.attempt)]
        return OwnerRef("v1", "Pod", _meta(anchor)["name"], _meta(anchor).get("uid", ""))

    def _selected(self, run: BenchmarkRun) -> list[TaskDef]:
        return tasks.select_tasks(run.spec.tasks)

    def _attempt(self, run: BenchmarkRun) -> AttemptStatus | None:
        return next((a for a in run.status.attempts if a.n == run.status.attempt), None)

    def _delete_anchors(self, live: dict[str, dict], keep: str | None = None) -> None:
        """Deleting an anchor removes its step Pods (owner references) and releases the node."""
        for name, pod in live.items():
            if (_meta(pod).get("labels") or {}).get("inspector.sparecores.com/role") == PodRole.ANCHOR and name != keep:
                self.kube.delete("pod", name)

    def _delete_secret(self, run: BenchmarkRun) -> None:
        self.kube.delete("secret", pods.urls_secret_name(run.name))
        self._signed_at.pop(run.name, None)

    def _heartbeat(self, run: BenchmarkRun) -> None:
        """Keep the anchor's hold from idling out between steps. A failure is only logged: a vanished anchor is
        found by the next pass."""
        try:
            self.kube.exec_heartbeat(
                pods.anchor_name(run.name, run.status.attempt), f"{pods.HEARTBEAT_ROOT}/{run.spec.run_id}"
            )
        except Exception as e:
            log.warning("heartbeat of %s failed: %s", run.name, e)

    # ----------------------------------------------------------------------- helpers: uploads

    def _current_plans(self, run: BenchmarkRun) -> list:
        """The upload plans in the urls Secret (empty when there is none yet)."""
        try:
            secret = self.kube.get("secret", pods.urls_secret_name(run.name))
        except NotFound:
            return []
        data = (secret.get("data") or {}).get("urls.json")
        text = base64.b64decode(data).decode() if data else (secret.get("stringData") or {}).get("urls.json")
        return self.storage.plans_from_urls_json(crd.run_key(run), text) if text else []

    def _apply_urls(self, run: BenchmarkRun, plans: list) -> None:
        signed = self.storage.presign(plans)
        secret = pods.render_urls_secret(run, render_urls_json(signed), self._run_owner(run))
        self.kube.apply_secret(secret)
        self._signed_at[run.name] = self._now()

    def _sign_step(self, run: BenchmarkRun, step: Step) -> None:
        """urls.json for the step: the entries already in the Secret for its keys (a repeated call or the GPU
        fallback must reuse the multipart uploads), new upload slots for the rest."""
        keys = {c.key for c in step.containers}
        reuse = [p for p in self._current_plans(run) if split_urls_key(p.urls_key)[0] in keys]
        have = {split_urls_key(p.urls_key)[0] for p in reuse}
        fresh = [c for c in step.containers if c.key not in have]
        plans = reuse + (self.storage.plan_uploads(crd.run_key(run), fresh) if fresh else [])
        self._apply_urls(run, plans)

    def _resign_if_due(self, run: BenchmarkRun, cfg: Config) -> None:
        signed = self._signed_at.get(run.name)
        every = timedelta(minutes=cfg.settings.uploads.resign_every_minutes)
        if signed is None or self._now() - signed >= every:
            self._apply_urls(run, self._current_plans(run))
            self._heartbeat(run)

    # ----------------------------------------------------------------------- helpers: recording results

    def _record(self, run: BenchmarkRun, nd: _Node | None, results: list[_Result]) -> BenchmarkRun:
        """Write `status.json` (the commit point) of each task, then mark them Done in `status.tasks`."""
        if not results:
            return run
        now = self._now()
        attempt = self._attempt(run)
        key = crd.run_key(run)
        done = []
        for r in results:
            old = _find(run.status, r.key) or TaskStatus(r.key)
            start = (r.outcome.start if r.outcome else None) or old.start or now
            end = (r.outcome.end if r.outcome else None) or now
            ts = replace(
                old,
                state=TaskState.DONE,
                attempt=run.status.attempt or None,
                start=start,
                end=end,
                exit_code=int(r.exit_code),
                error_msg=r.error_msg,
            )
            doc = render.build_status(
                run,
                ts,
                task_hash=tasks.task_hash(r.task),
                outcome=r.outcome,
                verification=r.verification,
                node=nd.facts if nd else None,
                attempt=attempt,
                image=r.image,
                pod=r.pod,
            )
            self.storage.write_status_json(key, r.key, doc)
            done.append(ts)

        def mutate(status: RunStatus) -> None:
            for ts in done:
                if _find(status, ts.name) is None:
                    status.tasks.append(ts)
                else:
                    status.tasks = [ts if t.name == ts.name else t for t in status.tasks]

        return self._update(run, mutate)

    def _unfinished(self, run: BenchmarkRun) -> list[TaskStatus]:
        return [t for t in run.status.tasks if t.state != TaskState.DONE]

    def _task_of(self, key: str) -> TaskDef:
        """The catalogue task of a status key (vLLM probe keys belong to the vllm task)."""
        for task in tasks.all_tasks():
            if task.name == key or key in {steps.vllm_probe_key(image) for image in task.images}:
                return task
        raise KeyError(key)

    def _fail_tasks(self, run: BenchmarkRun, nd: _Node | None, code: int, message: str) -> BenchmarkRun:
        """Every unfinished task ends with `code` and `message`."""
        results = [_Result(self._task_of(t.name), t.name, code, message) for t in self._unfinished(run)]
        return self._record(run, nd, results)

    def _close_attempt(self, status: RunStatus, outcome: AttemptOutcome) -> None:
        for attempt in status.attempts:
            if attempt.n == status.attempt and attempt.outcome is None:
                attempt.outcome, attempt.end = outcome, self._now()

    def _fail_run(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], message: str) -> None:
        """No usable node: unfinished tasks end `-1`, the run is Failed."""
        log.warning("run %s failed: %s", run.name, message)
        self._delete_anchors(live)
        self._delete_secret(run)
        run = self._fail_tasks(run, None, ExitCode.NO_NODE, message)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.PROVISIONING_FAILED)
            status.phase = RunPhase.FAILED
            status.current_step = status.node = None

        run = self._update(run, mutate)
        self._write_run_json(run, None)

    # ----------------------------------------------------------------------- run.json

    def _write_run_json(self, run: BenchmarkRun, nd: _Node | None) -> None:
        """`run.json`: plan, attempts and the `timing` the publisher turns into timing files. Earlier content
        (timings, per-attempt region/zone/price) is kept, so a restarted controller does not lose it."""
        key = crd.run_key(run)
        raw = self.storage.read_bytes(run_json_key(key))
        old = json.loads(raw) if raw else {}
        timing = dict(old.get("timing") or {})
        if nd:
            claim = self._get_claim(nd.facts.node_claim) or nd.claim
            for name, kind in (
                ("api_end", "Launched"),
                ("node_registered", "Registered"),
                ("node_ready", "Initialized"),
            ):
                when = _condition_time(claim, kind)
                if when:
                    timing[name] = render.format_timing_utc(when)
            if nd.facts.boot_time:
                timing["machine_start"] = render.format_timing_utc(nd.facts.boot_time)
        before = {a["n"]: a for a in old.get("attempts") or []}
        attempts = []
        for a in run.status.attempts:
            entry = before.get(a.n, {}) | {
                "n": a.n,
                "capacity_type": a.capacity_type.value,
                "node": a.node,
                "node_claim": a.node_claim,
                "start": a.start.isoformat() if a.start else None,
                "end": a.end.isoformat() if a.end else None,
                "outcome": a.outcome.value if a.outcome else None,
            }
            if a.n == run.status.attempt and run.status.region:
                entry |= {"region": run.status.region, "zone": run.status.zone, "price": run.status.price_per_hour}
            attempts.append(entry)
        doc = {
            "vendor": run.spec.vendor,
            "sku": run.spec.sku,
            "run_id": run.spec.run_id,
            "plan": [t.name for t in run.status.tasks],
            "attempts": attempts,
            "variant": run.status.variant,
            "timing": timing,
        }
        self.storage.write_run_json(key, doc)

    # ----------------------------------------------------------------------- Pending

    def _pending(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        self.check_config()
        settings = cfg.settings
        selected = self._selected(run)
        attempt = run.status.attempt + 1
        now = self._now()
        quarantined = spot.is_quarantined(self.kube, run.spec.vendor, run.spec.sku, settings, now)
        capacity = spot.capacity_for_next_attempt(run, selected, settings, quarantined)
        gpu, lowmem = self._sku_shape(run.spec.vendor, run.spec.sku)
        try:
            pool = pods.node_pool(run, cfg, gpu=gpu, lowmem=lowmem)
        except ValueError as e:
            self._fail_tasks_and_stop(run, f"cannot place the run: {e}")
            return
        anchor = pods.render_anchor(
            run,
            attempt,
            cfg,
            nodepool=pool,
            capacity=capacity,
            deadline_seconds=pods.anchor_deadline_seconds(_budget_steps(selected), cfg),
            owner=self._run_owner(run),
        )
        self._delete_anchors(live, keep=_meta(anchor)["name"])  # leftovers of earlier attempts
        self._delete_secret(run)  # every attempt signs fresh URLs
        self._create("pod", anchor)

        def mutate(status: RunStatus) -> None:
            if not status.tasks:
                status.tasks = [TaskStatus(t.name) for t in selected]
            status.attempt = attempt
            status.phase = RunPhase.PROVISIONING
            status.capacity_type = capacity[0]
            status.current_step = status.node = None
            if not any(a.n == attempt for a in status.attempts):
                status.attempts.append(AttemptStatus(n=attempt, capacity_type=capacity[0], start=now))

        self._update(run, mutate)

    def _fail_tasks_and_stop(self, run: BenchmarkRun, message: str) -> None:
        """A harness-level failure before any node: unfinished tasks end 256, the run is Failed."""
        run = self._fail_tasks(run, None, ExitCode.HARNESS_ERROR, message)
        self._update(run, lambda status: setattr(status, "phase", RunPhase.FAILED))

    # ----------------------------------------------------------------------- Provisioning

    def _provisioning(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        attempt = run.status.attempt
        anchor = live.get(pods.anchor_name(run.name, attempt))
        if anchor is None:
            return self._retry_provisioning(run, cfg, live, "the anchor Pod disappeared")
        if pods.is_finished(anchor):
            reason = anchor.get("status", {}).get("reason") or anchor["status"].get("phase")
            return self._fail_run(run, cfg, live, f"SCARPENTER_BOOT_FAILURE: anchor ended before running ({reason})")
        if pods.is_running(anchor) and pods.is_ready(anchor):
            return self._node_ready(run, cfg, live, anchor)

        claims = self._pending_claims(anchor)
        error = _claim_error(claims)
        started = self._attempt(run).start or _time(_meta(anchor).get("creationTimestamp"))
        gpu, _ = self._sku_shape(run.spec.vendor, run.spec.sku)
        budget = (
            timedelta(hours=cfg.settings.deadlines.provision_gpu_hours)
            if gpu
            else timedelta(minutes=cfg.settings.deadlines.provision_cpu_minutes)
        )
        if error is None and self._now() - started > budget:
            default = "BOOT_FAILURE" if claims else "ICE"
            error = scarpenter_error(f"no node after {int(budget.total_seconds() // 60)} minutes", default)
        if error:
            self._fail_run(run, cfg, live, error)

    def _pending_claims(self, anchor: dict) -> list[dict]:
        """NodeClaims created for this anchor's pool and instance type since the anchor exists."""
        selector = anchor["spec"]["nodeSelector"]
        created = _time(_meta(anchor).get("creationTimestamp"))
        claims = self.kube.list(
            "nodeclaim",
            labels={NODEPOOL_LABEL: selector[NODEPOOL_LABEL], INSTANCE_TYPE_LABEL: selector[INSTANCE_TYPE_LABEL]},
        )
        return [
            c for c in claims if created is None or (_time(_meta(c).get("creationTimestamp")) or created) >= created
        ]

    def _retry_provisioning(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], why: str) -> None:
        """The attempt never got a usable node: close it and start the next one, a few times."""
        lost = sum(1 for a in run.status.attempts if a.outcome == AttemptOutcome.PROVISIONING_FAILED)
        if lost + 1 >= MAX_PROVISION_RETRIES:
            return self._fail_run(run, cfg, live, f"SCARPENTER_BOOT_FAILURE: {why}, {lost + 1} attempts")
        log.warning("run %s attempt %d: %s, retrying", run.name, run.status.attempt, why)
        self._delete_anchors(live)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.PROVISIONING_FAILED)
            status.phase = RunPhase.PENDING

        self._update(run, mutate)

    def _node_ready(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], anchor: dict) -> None:
        try:
            node = self.kube.get("node", pods.pod_node(anchor))
        except NotFound:
            return
        anchor_created = _time(_meta(anchor)["creationTimestamp"])
        if _time(_meta(node)["creationTimestamp"]) < anchor_created:
            # The node existed before the anchor (billing retention): not a fresh machine.
            claim = self._claim_of(node, anchor)
            if claim:
                self.kube.delete("nodeclaim", _meta(claim)["name"])
            return self._retry_provisioning(run, cfg, live, "the node is older than the anchor")
        nd = self._build_node(run, node, anchor)
        self._nodes[run.name] = nd
        facts = nd.facts

        def mutate(status: RunStatus) -> None:
            status.phase = RunPhase.RUNNING
            status.node, status.region, status.zone = facts.node_name, facts.region, facts.zone
            status.price_per_hour = facts.price_per_hour
            if facts.capacity_type:
                status.capacity_type = facts.capacity_type
            for a in status.attempts:
                if a.n == status.attempt:
                    a.node, a.node_claim = facts.node_name, facts.node_claim
                    a.capacity_type = facts.capacity_type or a.capacity_type

        run = self._update(run, mutate)
        self._write_run_json(run, nd)
        self._heartbeat(run)
        self._running(run, cfg, live)

    # ----------------------------------------------------------------------- node facts

    def _build_node(self, run: BenchmarkRun, node: dict, anchor: dict) -> _Node:
        meta, status = _meta(node), node.get("status", {})
        labels, info = meta.get("labels") or {}, status.get("nodeInfo") or {}
        allocatable, capacity = status.get("allocatable") or {}, status.get("capacity") or {}
        claim = self._claim_of(node, anchor)
        annotations = _meta(claim or {}).get("annotations") or {}
        gpu_count = _quantity(allocatable.get(GPU_RESOURCE, 0))
        runtime = info.get("containerRuntimeVersion", "")
        facts = NodeFacts(
            node_name=meta["name"],
            instance_type=labels.get(INSTANCE_TYPE_LABEL, ""),
            arch=labels.get("kubernetes.io/arch", "amd64"),
            created=_time(meta["creationTimestamp"]),
            allocatable_memory_bytes=_quantity(allocatable.get("memory", 0)),
            vcpus=_quantity(capacity["cpu"]) if "cpu" in capacity else None,
            memory_bytes=_quantity(capacity["memory"]) if "memory" in capacity else None,
            region=labels.get("topology.kubernetes.io/region"),
            zone=labels.get("topology.kubernetes.io/zone"),
            capacity_type=CapacityType(labels[CAPACITY_LABEL]) if labels.get(CAPACITY_LABEL) else None,
            price_per_hour=annotations.get(PRICE_ANNOTATION),
            node_claim=_meta(claim or {}).get("name"),
            nodeclass_hash=annotations.get(NODECLASS_HASH_ANNOTATION),
            node_os=info.get("osImage"),
            kubelet=info.get("kubeletVersion"),
            runtime=f"k8s-{runtime.split('://')[0]}" if runtime else None,
            has_nvidia_gpu=gpu_count > 0 or labels.get(ACCELERATOR_MANUFACTURER_LABEL, "").lower() == "nvidia",
        )
        nd = _Node(run.status.attempt, facts, gpu_count, claim)
        self._learn_probe_facts(run, nd)
        return nd

    def _claim_of(self, node: dict, anchor: dict) -> dict | None:
        """The NodeClaim that produced the node (`status.nodeName`)."""
        pool = anchor["spec"]["nodeSelector"][NODEPOOL_LABEL]
        for claim in self.kube.list("nodeclaim", labels={NODEPOOL_LABEL: pool}):
            if claim.get("status", {}).get("nodeName") == _meta(node)["name"]:
                return claim
        return None

    def _get_claim(self, name: str | None) -> dict | None:
        if not name:
            return None
        try:
            return self.kube.get("nodeclaim", name)
        except NotFound:
            return None

    def _learn_probe_facts(self, run: BenchmarkRun, nd: _Node) -> None:
        """lscpu and GPU facts from this attempt's finished probe tasks (object storage, so a restarted
        controller learns them again)."""
        key = crd.run_key(run)
        ok = {
            t.name
            for t in run.status.tasks
            if t.state == TaskState.DONE and t.attempt == nd.attempt and t.exit_code == 0
        }
        if "lscpu" in ok:
            raw = self.storage.read_bytes(object_key(key, "lscpu", "stdout"))
            try:
                nd.facts.lscpu = variant.parse_lscpu_stdout(raw) if raw else {}
            except ValueError, TypeError:
                log.warning("run %s: lscpu output not parseable", run.name)
        if "gpu_facts" in ok:
            raw = self.storage.read_bytes(object_key(key, "gpu_facts", "stdout"))
            nd.facts.gpu_vram_mib, nd.facts.gpu_compute_cap = parse_gpu_facts(raw or b"")

    def _node_state(self, run: BenchmarkRun, anchor: dict | None) -> tuple[_Node | None, bool]:
        """(node, draining) of the running attempt. node None: anchor or Node are gone. draining: the Node is being
        deleted or its NodeClaim carries an interruption notice."""
        if anchor is None or pods.is_finished(anchor):
            return None, False
        try:
            node = self.kube.get("node", pods.pod_node(anchor))
        except NotFound:
            return None, False
        nd = self._nodes.get(run.name)
        if nd is None or nd.attempt != run.status.attempt or nd.facts.node_name != _meta(node)["name"]:
            nd = self._build_node(run, node, anchor)
            self._nodes[run.name] = nd
        nd.claim = self._get_claim(nd.facts.node_claim) or self._claim_of(node, anchor)
        draining = bool(_meta(node).get("deletionTimestamp")) or _interrupted(nd.claim)
        return nd, draining

    # ----------------------------------------------------------------------- Running

    def _running(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        anchor = live.get(pods.anchor_name(run.name, run.status.attempt))
        nd, draining = self._node_state(run, anchor)
        if nd is None:
            return self._attempt_lost(run, cfg, live, self._nodes.get(run.name))
        if any(t.state == TaskState.RUNNING for t in run.status.tasks):
            return self._watch_step(run, cfg, live, nd, draining)
        if draining:
            return  # the node goes away: no new step on it
        run = self._precheck(run, nd)
        run = self._ladder_failures(run, nd)
        step = self._plan(run, nd, _step_index(run.status.current_step) + 1)
        if step is None:
            return self._to_releasing(run, cfg, live)
        self._start_step(run, cfg, live, nd, step)

    def _probe_tasks(self, run: BenchmarkRun, nd: _Node) -> list[TaskDef]:
        """The probe step's tasks: the selected parallel tasks plus `lscpu` (always: the variant comes from it)
        and `gpu_facts` on GPU nodes, minus what the precheck refuses."""
        selected = {t.name: t for t in self._selected(run)}
        wanted = {name: selected.get(name) or tasks.get_task(name) for name in ("lscpu",)}
        if nd.facts.has_nvidia_gpu:
            wanted["gpu_facts"] = selected.get("gpu_facts") or tasks.get_task("gpu_facts")
        wanted |= {t.name: t for t in selected.values() if t.parallel}
        return [t for t in wanted.values() if tasks.precheck(t, nd.facts) is None]

    def _finished(self, run: BenchmarkRun) -> dict[str, TaskOutcome]:
        return {t.name: _outcome_of(t) for t in run.status.tasks if t.state == TaskState.DONE}

    def _plan(self, run: BenchmarkRun, nd: _Node, index: int) -> Step | None:
        """Step number `index` of the attempt given what is finished. Deterministic until a task of the step
        finishes, which is how an in-flight step is found again after a restart."""
        if index == 1:
            done_here = {
                t.name for t in run.status.tasks if t.state == TaskState.DONE and t.attempt == run.status.attempt
            }
            probes = [t for t in self._probe_tasks(run, nd) if t.name not in done_here]
            if step := steps.probe_step(probes, index=1):
                return step
        finished = self._finished(run)
        remaining = [t for t in self._selected(run) if not t.parallel and t.name not in finished]
        return steps.next_step(remaining, finished, nd.facts, index=index)

    def _precheck(self, run: BenchmarkRun, nd: _Node) -> BenchmarkRun:
        """Tasks the node contradicts (GPU task, no GPU) end `-3`."""
        results = []
        for task in self._selected(run):
            found = _find(run.status, task.name)
            reason = tasks.precheck(task, nd.facts)
            if reason and not (found and found.state == TaskState.DONE):
                results.append(_Result(task, task.name, ExitCode.PRECHECK_FAILED, reason))
        return self._record(run, nd, results)

    def _ladder_failures(self, run: BenchmarkRun, nd: _Node) -> BenchmarkRun:
        """A vLLM task whose every image failed its probe ends with the probes' errors."""
        finished = self._finished(run)
        results = []
        for task in self._selected(run):
            if task.images and task.name not in finished:
                message = steps.vllm_ladder_failure(task, finished, nd.facts)
                if message:
                    results.append(_Result(task, task.name, 1, message))
        return self._record(run, nd, results)

    def _start_step(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, step: Step) -> None:
        """Mark the step's tasks Running, sign URLs, heartbeat, create the Pod. Every part is repeatable."""
        keys = [c.key for c in step.containers]
        now = self._now()
        cpu_retry = step.name.endswith("-cpu")

        def mutate(status: RunStatus) -> None:
            status.current_step = step.name
            for key in keys:
                found = _find(status, key)
                if found is None:
                    found = TaskStatus(key)
                    status.tasks.append(found)
                found.state, found.attempt, found.start, found.end = TaskState.RUNNING, status.attempt, now, None
                found.exit_code = found.error_msg = found.commit = None
                found.published, found.gpu_fallback = False, cpu_retry

        run = self._update(run, mutate)
        self._sign_step(run, step)
        self._heartbeat(run)
        pod = pods.render_step(
            run,
            run.status.attempt,
            step,
            cfg,
            node_name=nd.facts.node_name,
            node_allocatable_memory_bytes=nd.facts.allocatable_memory_bytes,
            owner=self._anchor_owner(run, live),
            gpu_count=nd.gpu_count,
        )
        self._create("pod", pod)

    def _current_step(self, run: BenchmarkRun, nd: _Node) -> Step | None:
        """The in-flight step, recomputed; None when the inputs no longer produce it."""
        name = run.status.current_step or ""
        step = self._plan(run, nd, _step_index(name))
        if step is None or step.name != name.removesuffix("-cpu"):
            return None
        return replace(step, name=name, gpu_runtime=False) if name.endswith("-cpu") else step

    def _watch_step(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, draining: bool) -> None:
        step = self._current_step(run, nd)
        if step is None:
            log.error("run %s: step %s cannot be reconstructed", run.name, run.status.current_step)
            self._fail_running(run, nd, "step lost: the plan changed under a running step")
            return
        pod = live.get(pods.step_pod_name(run.name, run.status.attempt, step))
        if pod is None:
            if not draining:
                self._start_step(run, cfg, live, nd, step)
        elif pods.is_finished(pod):
            self._finish_step(run, cfg, live, nd, step, pod, draining)
        elif pods.pull_stuck(pod, self._now(), cfg.settings.deadlines.image_pull_minutes):
            self.kube.delete("pod", _meta(pod)["name"])
            self._abort_step(run, nd, step, pod, ExitCode.NEVER_FINISHED, Reason.IMAGE_PULL_TIMEOUT.value)
        else:
            self._resign_if_due(run, cfg)

    def _fail_running(self, run: BenchmarkRun, nd: _Node | None, message: str) -> None:
        running = [t for t in run.status.tasks if t.state == TaskState.RUNNING]
        results = [_Result(self._task_of(t.name), t.name, ExitCode.HARNESS_ERROR, message) for t in running]
        self._record(run, nd, results)

    def _abort_step(self, run: BenchmarkRun, nd: _Node, step: Step, pod: dict, code: int, message: str) -> None:
        """The step produced nothing (image never pulled): record every container and drop its open uploads."""
        results = []
        for c in step.containers:
            self.storage.abort_open(crd.run_key(run), c.key)
            outcome = TaskOutcome(c.key, code, message, from_sc_run=False)
            results.append(_Result(c.task, c.key, code, message, outcome, None, c.image, _meta(pod)["name"]))
        run = self._record(run, nd, results)
        self._heartbeat(run)

    def _finish_step(
        self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, step: Step, pod: dict, draining: bool
    ) -> None:
        outcomes = pods.classify(pod, step.containers)
        if draining and any(
            o.exit_code == ExitCode.NEVER_FINISHED and o.error_msg in (Reason.EVICTED, Reason.CANCELLED)
            for o in outcomes.values()
        ):
            return self._attempt_lost(run, cfg, live, nd)
        if not step.name.endswith("-cpu"):
            retry = steps.retry_without_gpu(step, outcomes)
            if retry:
                return self._start_step(run, cfg, live, nd, replace(retry, name=f"{step.name}-cpu"))

        pod_name = _meta(pod)["name"]
        flushes = sorted(n for n in live if n.startswith(f"{pod_name}-flush"))
        if flushes and not pods.is_finished(live[flushes[-1]]):
            return  # a flush Pod is still uploading
        key = crd.run_key(run)
        verdicts: dict[str, Verification | None] = {}
        for c in step.containers:
            if outcomes[c.key].container_start_error:
                self.storage.abort_open(key, c.key)  # nothing ran: nothing to verify
                verdicts[c.key] = None
            else:
                verdicts[c.key] = self.storage.finish_task(key, c.key)
        failing = [c for c in step.containers if verdicts[c.key] is not None and not verdicts[c.key].ok]
        if failing and len(flushes) < FLUSH_STEPS:
            self._apply_urls(run, self._current_plans(run))  # fresh URLs for the flush
            self._heartbeat(run)
            flush = pods.render_flush_step(
                run,
                run.status.attempt,
                replace(step, containers=tuple(failing)),
                len(flushes) + 1,
                cfg,
                node_name=nd.facts.node_name,
                owner=self._anchor_owner(run, live),
            )
            return self._create("pod", flush)

        plans = self._current_plans(run) if failing else []
        results = []
        for c in step.containers:
            outcome, verdict = outcomes[c.key], verdicts[c.key]
            code, message = outcome.exit_code, outcome.error_msg
            if c in failing:
                verdict = self.storage.salvage(key, c.key, plans)  # the last resort: keep what the parts allow
                if code == 0:
                    code, message = ExitCode.NEVER_FINISHED, Reason.OUTPUTS_LOST.value
            results.append(_Result(c.task, c.key, code, message, outcome, verdict, c.image, pod_name))
        if step.name == steps.PROBE_STEP_NAME:
            nd.facts.boot_time = next((o.btime for o in outcomes.values() if o.btime), nd.facts.boot_time)
        run = self._record(run, nd, results)
        if step.name == steps.PROBE_STEP_NAME:
            self._learn_probe_facts(run, nd)
            self._write_run_json(run, nd)
        self._heartbeat(run)
        if not draining:
            self._running(run, cfg, live)

    # ----------------------------------------------------------------------- losing the node

    def _attempt_lost(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node | None) -> None:
        """Anchor or Node gone (spot reclaim, expiry, manual delete). Salvage the in-flight tasks, close the
        attempt, then either run the rest on a new node or end the unfinished tasks as `-4`."""
        key = crd.run_key(run)
        claim = self._get_claim(self._attempt(run).node_claim) or (nd.claim if nd else None)
        outcome = spot.classify_node_loss(run, nd.facts if nd else None, claim)
        interrupted = outcome == AttemptOutcome.SPOT_INTERRUPTED
        reason = Reason.SPOT_INTERRUPTED if interrupted else Reason.NODE_LOST
        log.warning("run %s attempt %d lost its node: %s", run.name, run.status.attempt, outcome.value)

        running = [t for t in run.status.tasks if t.state == TaskState.RUNNING]
        plans = self._current_plans(run)
        verdicts = {t.name: self.storage.salvage(key, t.name, plans) for t in running}
        self._delete_anchors(live)
        self._delete_secret(run)
        self._nodes.pop(run.name, None)

        retry = interrupted and len(run.status.attempts) < cfg.settings.spot.max_spot_attempts + EXTRA_ATTEMPTS
        if not retry:
            results = []
            for t in self._unfinished(run):
                result = _Result(self._task_of(t.name), t.name, ExitCode.NEVER_FINISHED, reason.value)
                result.verification = verdicts.get(t.name)
                results.append(result)
            run = self._record(run, nd, results)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, outcome)
            status.node = status.current_step = None
            if retry:
                for t in status.tasks:
                    if t.state == TaskState.RUNNING:
                        t.state, t.attempt, t.start, t.gpu_fallback = TaskState.PENDING, None, None, False
                status.phase = RunPhase.PENDING
            else:
                status.phase = RunPhase.PUBLISHING

        run = self._update(run, mutate)
        if interrupted:
            spot.record_interruption(self.kube, run.spec.vendor, run.spec.sku, self._now())
        self._write_run_json(run, nd)
        if retry:
            self.enqueue(run.name)

    # ----------------------------------------------------------------------- Releasing

    def _to_releasing(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        now = self._now().replace(microsecond=0).isoformat().replace("+00:00", "Z")

        def mutate(status: RunStatus) -> None:
            status.phase = RunPhase.RELEASING
            status.conditions = [c for c in status.conditions if c.get("type") != "Releasing"]
            status.conditions.append({"type": "Releasing", "status": "True", "lastTransitionTime": now})

        run = self._update(run, mutate)
        self._releasing(run, cfg, live)

    def _releasing(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        began = next(
            (_time(c["lastTransitionTime"]) for c in run.status.conditions if c.get("type") == "Releasing"), None
        )
        if began and self._now() < began + run.spec.hold_after:
            self._heartbeat(run)  # a hold may outlast the idle timeout
            return
        self._write_run_json(run, self._nodes.get(run.name))
        self._delete_anchors(live)
        self._delete_secret(run)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.COMPLETED)
            status.phase = RunPhase.PUBLISHING
            status.current_step = None

        run = self._update(run, mutate)
        self._write_run_json(run, self._nodes.pop(run.name, None))

    # ----------------------------------------------------------------------- cancel

    def _cancel(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        key = crd.run_key(run)
        running = [t.name for t in run.status.tasks if t.state == TaskState.RUNNING]
        plans = self._current_plans(run)
        for pod_name in live:
            self.kube.delete("pod", pod_name)
        verdicts = {name: self.storage.salvage(key, name, plans) for name in running}
        self._delete_secret(run)
        nd = self._nodes.pop(run.name, None)
        results = []
        for t in self._unfinished(run):
            result = _Result(self._task_of(t.name), t.name, ExitCode.NEVER_FINISHED, Reason.CANCELLED.value)
            result.verification = verdicts.get(t.name)
            results.append(result)
        run = self._record(run, nd, results)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.CANCELLED)
            status.phase = RunPhase.CANCELLED
            status.node = status.current_step = None

        run = self._update(run, mutate)
        self._write_run_json(run, nd)
