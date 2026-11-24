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
- `status.step`: the step in flight (containers, images, commands, GPU runtime: `steps.to_status`), its tasks are
  `Running` until `Done`. It is read back from there, never planned again: what the Pod does is what was
  stored. `status.current_step` is the name of the step Pod created last (`07-geekbench`, `07-geekbench-cpu`
  after the GPU fallback); the next step's number is its plus one. The probe step is number 1 of every attempt.
- The in-flight multipart uploads: the urls Secret (`<run>-urls`), which holds only the current step's keys.

Phases:
- Pending: choose capacity (`spot.capacity_for_next_attempt`), create the anchor, record the attempt.
- Provisioning: anchor Running+ready on a Node created after it -> record region, zone, capacity type, price
  and node timings, phase Running. A Node older than the anchor: delete anchor and NodeClaim, new attempt.
  A NodeClaim capacity/quota error, 2 distinct NodeClaims of the attempt that failed to launch
  (`LaunchFailed`, kept in `status.launchFailures`) or the provisioning deadline: the reason comes from the class
  SCarpenter put in the NodeClaim message (`launch.reason_of_class`). A temporary reason (`launch.TEMPORARY`)
  deletes the run without a record (`_defer_launch`); otherwise remaining tasks end `-1 SCARPENTER_*`, Failed.
- Running: no step in flight -> prechecks (`-3`), vLLM ladder failures, next step (`steps.probe_step` first,
  which always contains `lscpu`, then `steps.next_step`): sign URLs into the Secret, heartbeat, create the Pod.
  Step Pod finished -> `pods.classify`, `storage.finish_task`, a `flush` Pod (max 2) while uploads are
  incomplete, then `status.json` per task (the commit point) and `status.tasks`. A reconcile with no step in
  flight deletes every step and flush Pod (`_delete_step_pods`), so the kubelet can free the images of finished
  steps; the anchor stays. The containers of a step that cannot start their GPU runtime are recreated once
  without it (the others keep their results). Containers stuck pulling their image -> `-4 IMAGE_PULL_TIMEOUT`,
  or stuck in a waiting state that never resolves (bad image name, missing Secret key) -> `-4` with the waiting
  message (the others of the Pod keep their results). A
  Pod still unfinished at `activeDeadlineSeconds` + grace + 2 min (wedged containerd) is force-deleted and recorded
  (`_wedged_outcome`); a Pod stuck terminating 2 min past its grace is force-deleted. While the node has
  DiskPressure no step is launched (up to 10 min); a step Pod the kubelet rejected for it is created once more
  after the pressure clears, else `-4` with the rejection message. Before a step is launched the tasks' old
  `manifest.json`, `result.json` and `status.json` are deleted from object storage. Every
  `uploads.resign_every_minutes` the Secret is re-signed. No step
  left -> Releasing.
- Anchor, Node or NodeClaim lost (or draining) while Running: close the attempt (an anchor that ended on a
  healthy node is `NODE_LOST`, else `spot.classify_node_loss`), delete the Pods and wait until none of them can still be
  uploading, then salvage the in-flight tasks (only when they are not retried). A spot interruption starts the
  next attempt with the unfinished tasks (the in-flight task runs again); any other loss, or too many attempts,
  ends the unfinished tasks as `-4 NODE_LOST|SPOT_INTERRUPTED`. The Secret is deleted last.
- Releasing: honour `spec.hold_after`, write `run.json`, delete anchor and Secret, phase Publishing (the
  publisher takes over and sets Done).
- Cancelled/Failed/Publishing/Done: only leftover anchors are deleted.

A reconcile that keeps failing for `MAX_FAILING` ends the run: unfinished tasks `256 HARNESS_ERROR`, open
uploads aborted, phase Failed (attempt outcome `HarnessError`). Nothing is deleted there: the Failed phase
removes the anchor (and with it the step Pods) on the next pass; the Secret is deleted at once.

DBaaS runs (`spec.database`, `runner_dbaas.py`): while Running, the database is created and bootstrapped before the
first step (`status.database`); no probe step is run; and `reconcile` ends by driving the destroy of the database,
which starts as soon as the run leaves the node phases (or is cancelled or deleted), in the pass that releases the node.

Cancel: the CLI sets the annotation `inspector.sparecores.com/cancel: "true"` (`crd.request_cancel`); the runner
sees it and `_cancel` records `-4 CANCELLED` for every unfinished task, deletes the Pods and sets phase
Cancelled. The BenchmarkRun is kept for the publisher: the CRD has no finalizer, so a plain `kubectl delete`
leaves no object to record anything on (leftover Pods are still deleted).
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

from k8s_inspector import crd, launch, planner, pods, render, spot, steps, tasks, variant
from k8s_inspector.kube import AlreadyExists, Forbidden, Kube, NotFound, backoff_delay, retry_until_ok
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    BenchmarkRun,
    CapacityType,
    Config,
    ExitCode,
    LaunchFailure,
    NodeFacts,
    OwnerRef,
    PodRole,
    Reason,
    RunPhase,
    RunStatus,
    Step,
    TaskDef,
    TaskOutcome,
    TaskState,
    TaskStatus,
    UninitializedClaim,
    Verification,
)
from k8s_inspector.runner_dbaas import DbaasMixin
from k8s_inspector.storage import Storage, UrlsTooLarge, object_key, render_urls_json, run_json_key, split_urls_key

log = logging.getLogger(__name__)

LABEL_RUN = crd.LABEL_RUN
RESYNC_SECONDS = 300
MAX_FAILING = timedelta(minutes=30)  # reconciles of a run failing for this long end the run
SKU_NOT_SELECTABLE = "SKU name is not a valid label value; SCarpenter cannot select it by api-reference"
ACTIVE = (RunPhase.PENDING, RunPhase.PROVISIONING, RunPhase.RUNNING, RunPhase.RELEASING)
NO_NODE_YET = (RunPhase.PENDING, RunPhase.PROVISIONING)  # a DBaaS run in these phases has created nothing that bills
MAX_PROVISION_RETRIES = 3  # attempts lost before a node was usable (stale node, anchor deleted)
EXTRA_ATTEMPTS = 2  # attempts beyond `max_spot_attempts` (the on-demand one plus one spare)
NOT_ELIGIBLE = "SKU is no longer eligible in Keeper"
ELIGIBILITY_CHECK_INTERVAL = timedelta(minutes=10)  # per run without a node: how often Keeper is asked again
MAX_LAUNCH_FAILURES = 2  # distinct NodeClaims that failed to launch in one attempt before the run gives up; ICE
# ones go on while SCarpenter still offers the SKU (`_launch_failures_exhausted`)
SPOT_FALLBACK_ERRORS = (
    "SCARPENTER_LAUNCH_REJECTED",
)  # a spot attempt ending so goes on-demand (not a capacity error: those wait, or end as ICE or quota; a node that
# never joined is no capacity problem either)
# SKUs that are bare metal, by name: OCI `BM.`, OVH `bm-`, AWS `.metal`, GCP `-metal`, Alibaba `ecs.ebm`, Vultr
# `vbm`/`vdm`. (Some Vultr `vcg` GPU plans are metal too, but only by plan; GPU SKUs get the long registration limit.)
BARE_METAL_SKU = re.compile(r"^(BM\.|bm-|vbm|vdm|ecs\.ebm)|[.-]metal(-|$)")

MAX_SPOT_RECLAIMS = 2  # spot claims of one attempt lost before they were Initialized; then the attempt goes on-demand
MAX_ICE_LAUNCH_FAILURES = 20  # ICE claims of one attempt, whatever SCarpenter still offers, before the run gives up
# Entries of `status.launchFailures`. The run ends at `MAX_ICE_LAUNCH_FAILURES`, so no claim is ever dropped and
# an old InsufficientCapacityError Event never looks new again.
MAX_LAUNCH_FAILURES_KEPT = MAX_ICE_LAUNCH_FAILURES
ICE_EVENT = re.compile(r"NodeClaim (?P<claim>\S+) nominated for this pod failed to launch: (?P<message>.*)", re.S)
LAUNCH_FAILED_REASON = "LaunchFailed"  # reason of a NodeClaim's `Launched` condition
BACKOFF_SECONDS = (2, 5, 15, 30, 60)
RECLAIM_WAIT = timedelta(minutes=5)  # how long a task cancelled or evicted on a spot node waits for the reclaim to show
RECLAIM_WAIT_NOT_READY = timedelta(minutes=15)  # ... when the Node's Ready condition is already not True
RECLAIM_RECHECK_SECONDS = 30
DISK_PRESSURE_WAIT = timedelta(minutes=10)  # how long a step waits for a node's DiskPressure to clear
DISK_PRESSURE_RECHECK_SECONDS = 30
DISK_PRESSURE_TAINT = "node.kubernetes.io/disk-pressure"

# Kubernetes and SCarpenter names read from Node and NodeClaim.
NODEPOOL_LABEL = pods.NODEPOOL_LABEL
CAPACITY_LABEL = "karpenter.sh/capacity-type"
INSTANCE_TYPE_LABEL = "node.kubernetes.io/instance-type"  # the node's label, recorded as the instance type
SCARPENTER = "scarpenter.sparecores.com"
MARKER_PREFIX = "inspector.sparecores.com/"  # the relaunch markers below; a relaunch carries all of them over
CPU_REFUSALS_ANNOTATION = f"{MARKER_PREFIX}cpu-refusals"  # on a recreated step Pod: OutOfcpu refusals so far
RELAUNCH_ANNOTATION = f"{MARKER_PREFIX}relaunch"  # on a step Pod recreated after a DiskPressure rejection
CPU_REFUSALS_FLOOR = 2  # after this many OutOfcpu refusals the step Pod requests the floor
CPU_REFUSALS_MAX = 3  # the refusal after the floor request ends the step
PRICE_ANNOTATION = f"{SCARPENTER}/price"
PRICE_INCOMPLETE_ANNOTATION = f"{SCARPENTER}/price-incomplete"
NODECLASS_HASH_ANNOTATION = f"{SCARPENTER}/nodeclass-hash"
ACCELERATOR_MANUFACTURER_LABEL = f"{SCARPENTER}/accelerator-manufacturer"
ACCELERATOR_COUNT_LABEL = f"{SCARPENTER}/accelerator-count"
GPU_RESOURCE = "nvidia.com/gpu"


def _markers(pod: dict) -> dict[str, str]:
    """The inspector annotations of the Pod: the relaunch markers a recreated Pod carries over."""
    return {k: v for k, v in (_meta(pod).get("annotations") or {}).items() if k.startswith(MARKER_PREFIX)}


def _cpu_refusals(annotations: dict[str, str] | None) -> int:
    try:
        return int((annotations or {}).get(CPU_REFUSALS_ANNOTATION, 0))
    except ValueError:
        return CPU_REFUSALS_MAX  # garbled marker: stop retrying


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
    disk_pressure: bool = False  # the kubelet rejects new Pods while it is set
    not_ready: bool = False  # the Node's Ready condition is Unknown or False
    lscpu_stdout: str | None = None  # raw output of the attempt's lscpu task, for `run.json`


@dataclass
class _Result:
    """The final record of one task, input of `status.json` and `status.tasks`."""

    task: TaskDef | None  # None: not in the catalogue (any more); recorded in `status.tasks` only
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


def _reclaimed(claim: dict) -> bool:
    """A spot interruption notice, or a spot claim being deleted: Karpenter reclaims those, no boot failure."""
    deleting = bool(_meta(claim).get("deletionTimestamp"))
    spot_claim = (_meta(claim).get("labels") or {}).get(CAPACITY_LABEL) == "spot"
    return _interrupted(claim) or (deleting and spot_claim)


def _claim_error(claims: list[dict]) -> str | None:
    """The error of a NodeClaim that failed to launch or register, else None. The reason is the class in the
    condition message; without one a launch failure is ICE and a registration failure BOOT_FAILURE."""
    for claim in claims:
        for condition in claim.get("status", {}).get("conditions") or []:
            if condition.get("status") != "False" or condition.get("type") not in ("Launched", "Registered"):
                continue
            if condition.get("reason") == LAUNCH_FAILED_REASON:
                continue  # counted by `_launch_failures`: Karpenter retries with a new NodeClaim
            message = condition.get("message") or condition.get("reason") or "NodeClaim failed"
            default = "BOOT_FAILURE" if condition["type"] == "Registered" else "ICE"
            return launch.error_message(launch.reason_of_class(message, default), message)
    return None


def _is_true(claim: dict, kind: str) -> bool:
    return any(
        c.get("type") == kind and c.get("status") == "True" for c in claim.get("status", {}).get("conditions") or []
    )


def _is_gpu_task(name: str) -> bool:
    try:
        return tasks.get_task(name).gpu
    except KeyError:  # a name the catalogue does not have
        return False


def _has_gpu(claim: dict) -> bool:
    """The NodeClaim's labels (Karpenter copies the instance type's) say the node has accelerators."""
    labels = _meta(claim).get("labels") or {}
    try:
        return int(labels[ACCELERATOR_COUNT_LABEL]) > 0  # absent on a CPU type
    except KeyError, TypeError, ValueError:
        return False


def _minutes(delta: timedelta) -> int:
    return int(delta.total_seconds() // 60)


def _launch_failed_message(claim: dict) -> str | None:
    """The message of a NodeClaim whose `Launched` condition has reason `LaunchFailed` (status False or Unknown)."""
    for condition in claim.get("status", {}).get("conditions") or []:
        if condition.get("type") == "Launched" and condition.get("reason") == LAUNCH_FAILED_REASON:
            return condition.get("message") or LAUNCH_FAILED_REASON
    return None


def _unschedulable(anchor: dict) -> str | None:
    """`SCARPENTER_NO_OFFERING: <message>` when the anchor's `PodScheduled` condition is False with a message, else
    None. Only for an anchor that still has no NodeClaim: Karpenter creates one within seconds for any SKU it can
    launch, so a SKU without one is not offered (the detailed Karpenter reason is only in an Event)."""
    for condition in anchor.get("status", {}).get("conditions") or []:
        if condition.get("type") == "PodScheduled" and condition.get("status") == "False" and condition.get("message"):
            return f"SCARPENTER_NO_OFFERING: {condition['message']}"
    return None


def _event_time(event: dict) -> datetime | None:
    for key in ("lastTimestamp", "eventTime", "firstTimestamp"):
        if event.get(key):
            return _time(event[key])
    return _time(event["metadata"].get("creationTimestamp"))


def _bench_pool_message(events: list[dict], bench_pool: str, since: datetime) -> str | None:
    """`no instance type met all requirements for <api-reference> in nodepool <bench_pool>` when the most recent
    Karpenter `FailedScheduling` event since `since` has such a clause for the bench pool (Karpenter lists one
    `; `-separated clause per pool)."""
    recent = [
        (time, e)
        for e in events
        if e.get("reason") == "FailedScheduling"
        and "karpenter"
        in (e.get("reportingComponent"), e.get("reportingController"), (e.get("source") or {}).get("component"))
        and (time := _event_time(e)) is not None
        and time >= since
    ]
    if not recent:
        return None
    message = max(recent, key=lambda item: item[0])[1].get("message", "").removeprefix("Failed to schedule pod, ")
    for clause in message.split("; "):
        if "no instance type met all requirements" in clause and f"karpenter.sh/nodepool In [{bench_pool}]" in clause:
            found = re.search(r"scarpenter\.sparecores\.com/api-reference In \[([^\]]*)\]", clause)
            sku = f" for {found.group(1)}" if found else ""
            return f"no instance type met all requirements{sku} in nodepool {bench_pool}"
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


def _not_ready(node: dict) -> bool:
    """Condition `Ready` is not True (Unknown once the kubelet stops reporting, as in a shutdown)."""
    conditions = node.get("status", {}).get("conditions") or []
    return not any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)


def _disk_pressure(node: dict) -> bool:
    """Condition `DiskPressure` True or the `disk-pressure` taint."""
    conditions = node.get("status", {}).get("conditions") or []
    taints = node.get("spec", {}).get("taints") or []
    return any(c.get("type") == "DiskPressure" and c.get("status") == "True" for c in conditions) or any(
        t.get("key") == DISK_PRESSURE_TAINT for t in taints
    )


def _outcome_of(status: TaskStatus) -> TaskOutcome:
    """What `steps.next_step` needs of a finished task."""
    return TaskOutcome(key=status.name, exit_code=status.exit_code or 0, error_msg=status.error_msg)


def _step_index(name: str | None) -> int:
    return int(name[:2]) if name else 0


class Runner(DbaasMixin):
    def __init__(
        self,
        kube: Kube,
        storage: Storage,
        cfg_provider,
        namespace: str,
        *,
        clock: Callable[[], datetime] | None = None,
        on_launch_failure: Callable[[str, str], None] | None = None,
        keeper_cache: planner.KeeperCache | None = None,
        on_leftover_stack_failure: Callable[[str], None] | None = None,
    ) -> None:
        """`cfg_provider`: zero-argument callable returning the current `Config` (settings are re-read from the
        ConfigMap every cycle, so a callable is passed instead of a snapshot). `clock` (tests) returns the
        current UTC time. `on_launch_failure(vendor, reason)` is called
        for each run that ends because no node could be launched, `on_leftover_stack_failure(vendor)` for each that gave
        up destroying a stack an earlier run left (metrics). `keeper_cache` is the planner's, so both see the same
        Keeper answer (`_drop_if_ineligible`)."""
        self.kube = kube
        self.storage = storage
        self.namespace = namespace
        self._cfg = cfg_provider
        self._clock = clock or (lambda: datetime.now(UTC))
        self._on_launch_failure = on_launch_failure
        self._on_leftover_stack_failure = on_leftover_stack_failure
        self._queue: queue.Queue[str] = queue.Queue()
        self._lock = threading.Lock()
        self._queued: set[str] = set()
        self._known: set[str] = set()  # run names seen, to notice deletions between LISTs
        self._failing: dict[str, tuple[int, datetime]] = {}  # run -> (reconciles failed in a row, since when)
        self._nodes: dict[str, _Node] = {}  # cache per run
        self._signed_at: dict[str, datetime] = {}  # when the urls Secret was last signed, per run
        # run -> since when its node has had DiskPressure. In memory: a controller restart restarts the wait.
        self._disk_wait: dict[str, datetime] = {}
        self._keeper_cache = keeper_cache or planner.KeeperCache()  # shared with the planner: `_drop_if_ineligible`
        self._eligibility_checked: dict[str, datetime] = {}  # run -> when Keeper was last asked about its SKU

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
        live = self._live_pods(run)
        for name, pod in live.items():  # a Pod wedged in Terminating keeps the node busy
            if pods.terminating_overdue(pod, self._now()):
                log.warning("run %s: Pod %s is stuck terminating, force-deleting it", run.name, name)
                self.kube.delete("pod", name, grace_seconds=0)
        handlers = {
            RunPhase.PENDING: self._pending,
            RunPhase.PROVISIONING: self._provisioning,
            RunPhase.RUNNING: self._running,
            RunPhase.RELEASING: self._releasing,
        }
        try:
            if (
                run.deleting
                and run.spec.database is not None
                and (run.status.database is None or self._clearing(run))
                and run.status.phase in NO_NODE_YET
            ):
                # A DBaaS run deleted before it created anything (its finalizer is only there for the database, or
                # for the leftover stack it clears): drop it like a deleted server run, leaving no record.
                self._delete_anchors(live)
                self._delete_secret(run)
            elif (run.cancel_requested or run.deleting) and run.status.phase in ACTIVE:
                self._cancel(run, cfg, live)
            elif run.status.phase in handlers:
                handlers[run.status.phase](run, cfg, live)
            else:
                self._delete_anchors(live)  # finished: nothing may be left holding a node
        except _Stale:
            log.info("run %s changed under the reconcile, dropping the pass", run.name)
        if run.spec.database is not None:  # after the handler: the node is released and the database destroyed together
            try:
                self._database_teardown(crd.get_run(self.kube, run_name), cfg)
            except NotFound:
                self._forget(run_name)

    def run_forever(self, stop: threading.Event) -> None:
        """Start the watch threads (BenchmarkRuns, labelled Pods), the resync timer and the worker loop in this
        thread until `stop` is set. On start and after every watch end: LIST, enqueue all runs."""
        self.check_config()
        watches = [("benchmarkrun", None), ("pod", {LABEL_RUN: None}), ("job", {LABEL_RUN: None})]  # None: label exists
        for kind, labels in watches:
            threading.Thread(target=self._watch, args=(kind, labels, stop), daemon=True, name=f"watch-{kind}").start()
        threading.Thread(target=self._resync_loop, args=(stop,), daemon=True, name="resync").start()
        retry_until_ok("initial LIST of runs", self._resync, stop)
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
            self._failing.pop(name, None)
        except Exception as error:
            failures, since = self._failing.get(name, (0, self._now()))
            self._failing[name] = (failures + 1, since)
            if self._now() - since >= MAX_FAILING:
                log.exception("reconcile of %s keeps failing, ending the run", name)
                try:
                    self._give_up(name, error)
                    self._failing.pop(name, None)
                    return
                except Exception:
                    log.exception("ending run %s failed, retrying", name)
            delay = BACKOFF_SECONDS[min(failures, len(BACKOFF_SECONDS) - 1)]
            log.warning("reconcile of %s failed, retrying in %ds", name, delay, exc_info=True)
            self._enqueue_later(name, delay)

    def _enqueue_later(self, name: str, delay: float) -> None:
        timer = threading.Timer(delay, self.enqueue, [name])
        timer.daemon = True
        timer.start()

    def _give_up(self, name: str, error: Exception) -> None:
        """The run cannot be reconciled: its unfinished tasks end 256 so the lease can be released."""
        try:
            run = crd.get_run(self.kube, name)
        except NotFound:
            return self._forget(name)
        if run.status.phase in ACTIVE:
            message = f"the controller gave up on the run: {type(error).__name__}"  # no details: may hold URLs
            try:
                self._delete_run_claims(run, self._live_pods(run))
            except Exception:
                log.warning("run %s: cannot list its Pods, not deleting its NodeClaims", run.name, exc_info=True)
            self._end_run(run, ExitCode.HARNESS_ERROR, message, AttemptOutcome.HARNESS_ERROR)

    def _watch(self, kind: str, labels: dict | None, stop: threading.Event) -> None:
        """Watch until `stop`; any error (429, 5xx, the CRD not established yet) restarts it with backoff."""
        failures = 0
        while not stop.is_set():
            try:
                for event in self.kube.watch(kind, labels=labels):
                    if stop.is_set():
                        return
                    failures = 0
                    name = (
                        _meta(event.obj).get("name")
                        if kind == "benchmarkrun"
                        else (_meta(event.obj).get("labels") or {}).get(LABEL_RUN)
                    )
                    if name:
                        if kind == "benchmarkrun":
                            self._known.add(name)
                        self.enqueue(name)
                error = None
            except Exception as e:
                error = e
            failures += 1
            delay = backoff_delay(failures, error)
            log.warning("watch of %s ended, restarting in %.0fs", kind, delay, exc_info=error)
            stop.wait(delay)

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
        self._disk_wait.pop(name, None)
        self._eligibility_checked.pop(name, None)
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

    def _live_pods(self, run: BenchmarkRun) -> dict[str, dict]:
        return {_meta(p)["name"]: p for p in self.kube.list("pod", labels={LABEL_RUN: run.name})}

    def _selected(self, run: BenchmarkRun) -> list[TaskDef]:
        return tasks.select_tasks(run.spec.tasks)

    def _attempt(self, run: BenchmarkRun) -> AttemptStatus | None:
        return next((a for a in run.status.attempts if a.n == run.status.attempt), None)

    def _delete_anchors(self, live: dict[str, dict], keep: str | None = None) -> None:
        """Deleting an anchor removes its step Pods (owner references) and releases the node."""
        for name, pod in live.items():
            if (_meta(pod).get("labels") or {}).get("inspector.sparecores.com/role") == PodRole.ANCHOR and name != keep:
                self.kube.delete("pod", name)

    def _delete_run_claims(self, run: BenchmarkRun, live: dict[str, dict]) -> None:
        """Delete the NodeClaims of a run that stops waiting for its node: the ones remembered as uninitialized, the
        current attempt's and those created for its live anchors. Deleting the anchor alone is not enough: Karpenter
        consolidates only nodes that initialized, so a claim still booting would keep billing and hold a
        `maxInFlight` slot. Initialized claims go too (their node serves only this run, and the anchor is going),
        which spares checking each claim. Idempotent: a claim that is gone is ignored (`Kube.delete`). Never
        raises: a failing API call is logged, so it cannot change how the run ends."""
        try:
            names = {c.node_claim for c in run.status.uninitialized_claims}
            attempt = self._attempt(run)
            if attempt and attempt.node_claim:
                names.add(attempt.node_claim)
            for pod in live.values():
                if (_meta(pod).get("labels") or {}).get(pods.LABEL_ROLE) == PodRole.ANCHOR:
                    names |= {_meta(c)["name"] for c in self._pending_claims(pod)}
        except Exception:
            log.warning("run %s: cannot list its NodeClaims, not deleting them", run.name, exc_info=True)
            return
        for name in sorted(names):
            self._delete_claim(name)

    def _delete_claim(self, name: str) -> None:
        """Best effort: a failing delete is logged and never changes how a run ends."""
        try:
            self.kube.delete("nodeclaim", name)
        except Exception as e:
            log.warning("cannot delete NodeClaim %s: %s", name, e)

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

    def _record(
        self,
        run: BenchmarkRun,
        nd: _Node | None,
        results: list[_Result],
        *,
        end_step: bool = False,
        next_step: Step | None = None,
        fallback: bool = False,
    ) -> BenchmarkRun:
        """Write `status.json` (the commit point) of each task, then mark them Done in `status.tasks` (not yet
        published). `end_step`: they were the step in flight, which is over. `next_step` replaces it in the
        same patch, its tasks Running (`fallback`: the GPU fallback), so no pass sees the step half switched;
        the caller launches it afterwards."""
        if not results and next_step is None:
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
                published=r.task is None,  # nothing to publish for a task the catalogue does not know
                commit=None,
            )
            if r.task is not None:
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
            if next_step is not None:
                self._mark_running(status, next_step, now, fallback)
            elif end_step:
                status.step = None

        return self._update(run, mutate)

    def _unfinished(self, run: BenchmarkRun) -> list[TaskStatus]:
        return [t for t in run.status.tasks if t.state != TaskState.DONE]

    def _fail_tasks(self, run: BenchmarkRun, nd: _Node | None, code: int, message: str) -> BenchmarkRun:
        """Every unfinished task ends with `code` and `message`."""
        results = [_Result(steps.task_of(t.name), t.name, code, message) for t in self._unfinished(run)]
        return self._record(run, nd, results)

    def _ensure_tasks(self, run: BenchmarkRun) -> BenchmarkRun:
        """`status.tasks` is filled when the first attempt starts. A run that ends earlier needs it too: the
        publisher releases the lease only for a run that has tasks."""
        if run.status.tasks:
            return run
        try:
            names = [t.name for t in self._selected(run)]
        except ValueError:  # the spec names a task the catalogue does not know
            names = list(run.spec.tasks)
        return self._update(
            run, lambda status: setattr(status, "tasks", status.tasks or [TaskStatus(n) for n in names])
        )

    def _close_attempt(self, status: RunStatus, outcome: AttemptOutcome) -> None:
        for attempt in status.attempts:
            if attempt.n == status.attempt and attempt.outcome is None:
                attempt.outcome, attempt.end = outcome, self._now()

    def _end_run(self, run: BenchmarkRun, code: int, message: str, outcome: AttemptOutcome) -> None:
        """The run cannot go on: unfinished tasks end with `code` (open uploads of the running ones aborted),
        the Secret is deleted, phase Failed. The Pods stay: the Failed phase deletes the anchor on the next pass, and
        recording first means a failing cluster API cannot leave a run without a record."""
        log.warning("run %s failed: %s", run.name, message)
        run = self._ensure_tasks(run)
        for t in run.status.tasks:
            if t.state == TaskState.RUNNING:
                self.storage.abort_open(crd.run_key(run), t.name)
        run = self._fail_tasks(run, None, code, message)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, outcome)
            status.phase = RunPhase.FAILED
            status.current_step = status.step = status.node = None

        run = self._update(run, mutate)
        self._write_run_json(run, self._nodes.pop(run.name, None))
        self._delete_secret(run)

    def _fail_run(self, run: BenchmarkRun, live: dict[str, dict], message: str) -> None:
        """No usable node: its claims are deleted, unfinished tasks end `-1`, unless the failure is temporary
        (`_defer_launch`)."""
        self._delete_run_claims(run, live)
        if not self._defer_launch(run, message):
            self._end_run(run, ExitCode.NO_NODE, message, AttemptOutcome.PROVISIONING_FAILED)

    def _defer_launch(self, run: BenchmarkRun, message: str) -> bool:
        """Count a launch failure and remember when the SKU may be launched again (`launch`). A temporary one
        (BOOT_FAILURE is temporary until its `BOOT_FAILURE_LIMIT`-th run) leaves no record in sc-inspector-data:
        the BenchmarkRun is deleted, which frees the SKU for the planner. True when it was. A run that already
        has finished tasks is kept (they must be published)."""
        reason = launch.reason_of_error(message)
        if reason is None:
            return False
        vendor, sku = run.spec.vendor, run.spec.sku
        if self._on_launch_failure:
            self._on_launch_failure(vendor, reason)
        if reason in launch.PERMANENT and reason != "BOOT_FAILURE":
            launch.clear(self.kube, vendor, [sku])
            return False
        if reason not in launch.REASONS or run.spec.force:  # a forced run is the operator's: it records its result
            return False
        until = (
            planner.offered_until(self.kube, self._cfg(), vendor, sku, self._now()) if reason == "DAILY_LIMIT" else None
        )
        count = launch.record_failure(self.kube, vendor, sku, reason, self._now(), until)
        if reason == "BOOT_FAILURE" and count >= launch.BOOT_FAILURE_LIMIT:
            return False  # the entry stays: it counts for the vendor's breaker until a node comes up
        if any(t.state == TaskState.DONE for t in run.status.tasks):
            return False
        log.warning("run %s: %s, launch of %s/%s deferred (failure %d)", run.name, message, vendor, sku, count)
        self._delete_secret(run)
        crd.delete_run(self.kube, run.name)
        self._forget(run.name)  # deletes the anchor with the Pods
        return True

    # ----------------------------------------------------------------------- run.json

    def _write_run_json(self, run: BenchmarkRun, nd: _Node | None) -> None:
        """`run.json`: plan, per-attempt facts (timings, lscpu output, region/zone/price) and the `timing` of
        the latest attempt the publisher turns into timing files. Earlier content is kept, so a restarted
        controller does not lose what the NodeClaim of an earlier attempt said."""
        key = crd.run_key(run)
        raw = self.storage.read_bytes(run_json_key(key))
        before = {a["attempt"]: a for a in json.loads(raw).get("attempts") or [] if "attempt" in a} if raw else {}
        attempts = []
        for a in run.status.attempts:
            entry = {"api_end": None, "machine_start": None, "node_registered": None, "node_ready": None}
            entry |= {"lscpu_stdout": None} | before.get(a.n, {})
            entry |= {
                "attempt": a.n,
                "capacity_type": a.capacity_type.value if a.capacity_type else None,
                "node": a.node,
                "node_claim": a.node_claim,
                "api_start": render.format_timing_utc(a.start) if a.start else None,
                "end": a.end.isoformat() if a.end else None,
                "outcome": a.outcome.value if a.outcome else None,
            }
            if a.n == run.status.attempt and run.status.region:
                entry |= {"region": run.status.region, "zone": run.status.zone, "price": run.status.price_per_hour}
            if nd and nd.attempt == a.n:
                claim = self._get_claim(nd.facts.node_claim) or nd.claim
                for name, kind in (
                    ("api_end", "Launched"),
                    ("node_registered", "Registered"),
                    ("node_ready", "Initialized"),
                ):
                    when = _condition_time(claim, kind)
                    if when:
                        entry[name] = render.format_timing_utc(when)
                if nd.facts.boot_time:
                    entry["machine_start"] = render.format_timing_utc(nd.facts.boot_time)
                if nd.lscpu_stdout is not None:
                    entry["lscpu_stdout"] = nd.lscpu_stdout
            attempts.append(entry)
        latest = attempts[-1] if attempts else {}
        timing = {
            k: latest[k]
            for k in ("api_start", "api_end", "machine_start", "node_registered", "node_ready")
            if latest.get(k)
        }
        doc = {
            "vendor": run.spec.vendor,
            "sku": run.spec.sku,
            "run_id": run.spec.run_id,
            "plan": [t.name for t in run.status.tasks],
            "attempts": attempts,
            "variant": run.status.variant,
            "timing": timing,
            "database": run.spec.database,
        }
        self.storage.write_run_json(key, doc)

    # ----------------------------------------------------------------------- Pending

    def _pending(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        self.check_config()
        if self._drop_if_ineligible(run, live):
            return
        if run.spec.database is not None:
            run = self._clear_leftover_stack(run, cfg)
            if run is None:
                return
        settings = cfg.settings
        selected = self._selected(run)
        attempt = run.status.attempt + 1
        now = self._now()
        quarantined = spot.is_quarantined(self.kube, run.spec.vendor, run.spec.sku, settings, now)
        capacity = spot.capacity_for_next_attempt(run, selected, settings, quarantined)
        if not pods.is_label_value(run.spec.sku):
            return self._end_run(run, ExitCode.HARNESS_ERROR, SKU_NOT_SELECTABLE, AttemptOutcome.HARNESS_ERROR)
        anchor = pods.render_anchor(
            run,
            attempt,
            cfg,
            capacity=capacity,
            deadline_seconds=pods.anchor_deadline_seconds(
                selected, cfg, run.spec.hold_after, database=run.spec.database is not None
            ),
            owner=self._run_owner(run),
        )
        self._delete_anchors(live, keep=_meta(anchor)["name"])  # leftovers of earlier attempts
        self._delete_secret(run)  # every attempt signs fresh URLs
        self._create("pod", anchor)

        def mutate(status: RunStatus) -> None:
            if not status.tasks:
                status.tasks = [TaskStatus(t.name) for t in selected]
            status.attempt = attempt
            status.launch_failures = []
            status.uninitialized_claims = []
            status.spot_reclaims = 0
            status.phase = RunPhase.PROVISIONING
            status.capacity_type = None  # the node's, once the attempt has one
            status.current_step = status.step = status.node = None
            if not any(a.n == attempt for a in status.attempts):
                status.attempts.append(AttemptStatus(n=attempt, start=now))

        self._update(run, mutate)

    # ----------------------------------------------------------------------- Provisioning

    def _provisioning(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        if self._drop_if_ineligible(run, live):
            return
        attempt = run.status.attempt
        anchor = live.get(pods.anchor_name(run.name, attempt))
        if anchor is None:
            return self._retry_provisioning(run, cfg, live, "the anchor Pod disappeared")
        if pods.is_finished(anchor):
            reason = anchor.get("status", {}).get("reason") or anchor["status"].get("phase")
            return self._fail_run(
                run, live, launch.error_message("ANCHOR_FAILED", f"anchor ended before running ({reason})")
            )
        if pods.is_running(anchor) and pods.is_ready(anchor):
            return self._node_ready(run, cfg, live, anchor)

        claims = self._pending_claims(anchor)
        bound = bool(anchor["spec"].get("nodeName")) or any(_is_true(c, "Initialized") for c in claims)
        if bound:  # the anchor has its node: Karpenter cleans up any other claim
            claims = [c for c in claims if _is_true(c, "Initialized")]
        error = _claim_error(claims)
        events = [] if bound else self._anchor_events(anchor)
        failures = self._record_launch_failures(run, claims, events)
        if error is None and self._launch_failures_exhausted(run, cfg, anchor, failures):
            error = self._launch_error(failures[-1].message)
        reclaims = run.status.spot_reclaims
        if error is None and not bound:
            error, reclaims = self._boot_error(run, cfg, claims)
        if error is None and not bound and reclaims >= MAX_SPOT_RECLAIMS and pods.allows_spot(anchor):
            # Spot claims keep getting reclaimed (or deleted by SCarpenter's GPU check) before they are up: the
            # next attempt is on-demand only, where a lost claim ends the run as BOOT_FAILURE.
            return self._retry_on_demand(run, live, f"{reclaims} spot nodes were lost before they were Ready")
        started = self._attempt(run).start or _time(_meta(anchor).get("creationTimestamp"))
        budget = (
            timedelta(hours=cfg.settings.deadlines.provision_gpu_hours)
            if self._is_gpu(run, claims)
            else timedelta(minutes=cfg.settings.deadlines.provision_cpu_minutes)
        )
        waited = self._now() - started
        if (
            error is None
            and not claims
            and not failures  # ICE claims are deleted at once, so most reconciles see none
            and waited > timedelta(minutes=cfg.settings.deadlines.no_offering_minutes)
        ):
            error = _unschedulable(anchor)
        if (
            error is None
            and not claims
            and not failures  # a launch failure is a transient ICE, not a missing offering
            and waited >= timedelta(minutes=cfg.settings.deadlines.no_offering_fast_minutes)
        ):
            message = _bench_pool_message(events, cfg.settings.bench_pool, started)
            error = f"SCARPENTER_NO_OFFERING: {message}" if message else None
        if (
            error is None
            and failures
            and not claims
            and waited > timedelta(minutes=cfg.settings.deadlines.no_offering_minutes)
            and self._not_offered(run, cfg)
        ):
            error = self._launch_error(failures[-1].message)  # no claim will follow: SCarpenter stopped offering it
        if error is None and bound:
            error = self._anchor_ready_error(run, cfg, anchor, claims)
        if error is None and not bound and waited > budget and not any(_is_true(c, "Launched") for c in claims):
            minutes = int(budget.total_seconds() // 60)
            if failures:  # the launch kept failing: that is the error, not a missing node
                error = self._launch_error(failures[-1].message)
            else:  # Karpenter keeps replacing claims that never launched: capacity or quota
                error = launch.error_message("ICE", f"no node launched after {minutes} minutes")
        if error is None:
            return
        if error.startswith(SPOT_FALLBACK_ERRORS) and pods.allows_spot(anchor):
            # The next attempt (`spot.capacity_for_next_attempt`) is on-demand only.
            return self._retry_on_demand(run, live, error)
        self._fail_run(run, live, error)

    def _launch_failures_exhausted(
        self, run: BenchmarkRun, cfg: Config, anchor: dict, failures: list[LaunchFailure]
    ) -> bool:
        """`MAX_LAUNCH_FAILURES` claims failed to launch. When all of them name the class InsufficientCapacity or
        ZoneUnusable the run goes on while SCarpenter's offered listing has an available offering for the SKU in a
        capacity type the anchor allows (it tries other regions and zones itself), up to `MAX_ICE_LAUNCH_FAILURES`;
        the provisioning budget still ends it. An unreadable listing counts as none (not offered), unlike
        `_not_offered`, where no answer means keep waiting."""
        if len(failures) < MAX_LAUNCH_FAILURES:
            return False
        if not all(launch.is_capacity_class(f.message) for f in failures):
            return True
        if len(failures) >= MAX_ICE_LAUNCH_FAILURES:
            return True
        on_demand_only = not pods.allows_spot(anchor)
        return not planner.is_offered(self.kube, cfg, run.spec.vendor, run.spec.sku, on_demand_only, self._now())

    def _not_offered(self, run: BenchmarkRun, cfg: Config) -> bool:
        """SCarpenter's offered-types listing is readable and does not offer the SKU. An unreadable or stale
        listing is not an answer (False): the run keeps waiting."""
        skipped = planner.not_offered(self.kube, cfg, run.spec.vendor, [run.spec.sku], self._now())
        return bool(skipped)

    def _drop_if_ineligible(self, run: BenchmarkRun, live: dict[str, dict]) -> bool:
        """A run whose SKU Keeper no longer lists as ACTIVE, or blocks for all of its unfinished server tasks, does
        not go on: no node is launched for it and its NodeClaims are deleted (`_delete_run_claims`). A run with no
        finished task that never reached a node is deleted without a record. A run with a finished task or a node
        (a spot-interrupted run back in Pending) ends through `_end_run` instead: its done tasks stay and are
        published, the unfinished ones end `-1`. A DBaaS run is never checked (its teardown has its own path). Asked
        at most every `ELIGIBILITY_CHECK_INTERVAL` per run; a forced run skips Keeper by design, and an unreachable
        Keeper is not an answer. True when the run was ended or deleted."""
        now = self._now()
        last = self._eligibility_checked.get(run.name)
        if run.spec.database is not None or run.spec.force or (last and now - last < ELIGIBILITY_CHECK_INTERVAL):
            return False
        self._eligibility_checked[run.name] = now
        done = {t.name for t in run.status.tasks if t.state == TaskState.DONE}
        names = [t.name for t in self._selected(run) if not t.dbaas and t.name not in done]
        if not names:
            return False
        try:
            blocked = self._keeper_cache.get(self._cfg().env, [t.name for t in tasks.all_tasks() if not t.dbaas])
        except Exception:
            log.warning("run %s: cannot check its eligibility, Keeper is not reachable", run.name, exc_info=True)
            return False
        reasons = blocked.get(run.spec.vendor, {}).get(run.spec.sku)  # None: not ACTIVE
        if reasons is not None and not all(name in reasons for name in names):
            return False
        self._delete_run_claims(run, live)
        if done or any(a.node for a in run.status.attempts):
            self._end_run(run, ExitCode.NO_NODE, NOT_ELIGIBLE, AttemptOutcome.PROVISIONING_FAILED)
            return True
        log.warning(
            "run %s: %s/%s is no longer eligible in Keeper, deleting the run", run.name, run.spec.vendor, run.spec.sku
        )
        self._delete_secret(run)
        crd.delete_run(self.kube, run.name)
        self._forget(run.name)  # deletes the anchor with the Pods
        return True

    @staticmethod
    def _is_gpu(run: BenchmarkRun, claims: list[dict]) -> bool:
        """A GPU SKU: a GPU task is planned for it, or its NodeClaim has accelerators."""
        return any(_is_gpu_task(name) for name in run.spec.tasks) or any(_has_gpu(c) for c in claims)

    def _boot_error(self, run: BenchmarkRun, cfg: Config, claims: list[dict]) -> tuple[str | None, int]:
        """BOOT_FAILURE for the first node of the attempt that does not come up. A NodeClaim that launched and is not
        yet Initialized fails the run when it is being deleted or is gone (Karpenter deletes a claim that does not
        register within its own fixed 15 minutes; SCarpenter one whose GPU check fails); when it is not Registered
        the registration limit after its creation (Karpenter's own start); when it is Registered and not Initialized
        the ready limit after it registered. A claim with a spot interruption notice is a reclaim, not a boot
        failure, and is forgotten. The NodeClaim is deleted once a limit has passed. While a limit is running, the
        run is queued again for the moment it expires."""
        deadlines = cfg.settings.deadlines
        bare_metal = bool(BARE_METAL_SKU.search(run.spec.sku))
        gpu = self._is_gpu(run, claims)
        registration = timedelta(
            minutes=deadlines.registration_minutes_bare_metal if bare_metal or gpu else deadlines.registration_minutes
        )
        if gpu:
            ready = timedelta(minutes=deadlines.ready_minutes_gpu)
        elif bare_metal:
            ready = timedelta(minutes=deadlines.ready_minutes_bare_metal)
        else:
            ready = timedelta(minutes=deadlines.ready_minutes)
        now = self._now()
        coming_up = [
            c for c in claims if _is_true(c, "Launched") and not _is_true(c, "Initialized") and not _reclaimed(c)
        ]
        remembered, reclaims = self._remember_uninitialized(run, coming_up, claims)
        alive = {_meta(c)["name"] for c in coming_up if not _meta(c).get("deletionTimestamp")}
        gone = sorted(remembered.keys() - alive)  # spot claims were dropped from `remembered` already
        if gone:
            return launch.error_message("BOOT_FAILURE", f"node of {gone[0]} was deleted before it was Ready"), reclaims
        expiries = []
        for claim in coming_up:
            name = _meta(claim)["name"]
            if _is_true(claim, "Registered"):
                limit_at = (_condition_time(claim, "Registered") or remembered[name].launched) + ready
                late = f"node of {name} was not Ready within {_minutes(ready)} min"
            else:
                limit_at = (_time(_meta(claim).get("creationTimestamp")) or remembered[name].launched) + registration
                late = f"node of {name} did not register within {_minutes(registration)} min"
            if now >= limit_at:
                return self._boot_failure(claim, late), reclaims
            expiries.append(limit_at)
        if expiries:
            self._enqueue_later(run.name, max((min(expiries) - now).total_seconds(), 1))
        return None, reclaims

    def _anchor_ready_error(self, run: BenchmarkRun, cfg: Config, anchor: dict, claims: list[dict]) -> str | None:
        """ANCHOR_FAILED when the anchor has its node and is not Ready `anchorReadyMinutes` after the node was
        Initialized (else after the anchor was scheduled): an image pull error, a probe, a leftover taint. Not a
        boot failure: the node came up. Otherwise the run is queued again for the moment the limit expires."""
        scheduled = [c for c in anchor.get("status", {}).get("conditions") or [] if c.get("type") == "PodScheduled"]
        times = [_condition_time(c, "Initialized") for c in claims] + [
            _time(c.get("lastTransitionTime")) for c in scheduled
        ]
        times = [t for t in times if t]
        if not times:
            return None
        limit = timedelta(minutes=cfg.settings.deadlines.anchor_ready_minutes)
        limit_at = min(times) + limit
        if self._now() >= limit_at:
            return launch.error_message(
                "ANCHOR_FAILED", f"anchor not Ready {_minutes(limit)} minutes after its node came up"
            )
        self._enqueue_later(run.name, max((limit_at - self._now()).total_seconds(), 1))
        return None

    def _boot_failure(self, claim: dict, message: str) -> str:
        self._delete_claim(_meta(claim)["name"])
        return launch.error_message("BOOT_FAILURE", message)

    def _remember_uninitialized(
        self, run: BenchmarkRun, coming_up: list[dict], claims: list[dict]
    ) -> tuple[dict[str, UninitializedClaim], int]:
        """`status.uninitializedClaims` by name, updated: the claims seen launched and not Initialized are added;
        those that initialized or are reclaimed (`_reclaimed`) are dropped (a reclaimed one counts in
        `status.spotReclaims`), and so is a spot claim that is gone or
        being deleted (a reclaim finalised between two passes; on-demand claims are not deleted by Karpenter before
        they are up, except for a boot failure)."""
        known = {c.node_claim: c for c in run.status.uninitialized_claims}
        present = {_meta(c)["name"] for c in claims if not _meta(c).get("deletionTimestamp")}
        done = {_meta(c)["name"] for c in claims if _is_true(c, "Initialized") or _reclaimed(c)}
        done |= {name for name, c in known.items() if c.capacity_type == "spot" and name not in present}
        initialized = {_meta(c)["name"] for c in claims if _is_true(c, "Initialized")}
        reclaims = run.status.spot_reclaims + len(done & known.keys() - initialized)  # each claim is dropped once
        wanted = {name: c for name, c in known.items() if name not in done}
        for claim in coming_up:
            name = _meta(claim)["name"]
            capacity = (_meta(claim).get("labels") or {}).get(CAPACITY_LABEL)
            launched = _condition_time(claim, "Launched") or self._now()
            old = wanted.get(name)
            wanted[name] = UninitializedClaim(
                name, old.launched if old else launched, capacity or (old and old.capacity_type)
            )
        if wanted != known:

            def mutate(status: RunStatus) -> None:
                status.uninitialized_claims = list(wanted.values())
                status.spot_reclaims = reclaims

            self._update(run, mutate)
        return wanted, reclaims

    @staticmethod
    def _launch_error(message: str) -> str:
        return launch.error_message(launch.reason_of_class(message, "ICE"), message)

    def _anchor_events(self, anchor: dict) -> list[dict]:
        """The anchor Pod's Events; an API error counts as no events."""
        try:
            return self.kube.list_pod_events(_meta(anchor)["name"], _meta(anchor)["uid"])
        except Exception:
            log.warning("listing events of %s failed", _meta(anchor)["name"], exc_info=True)
            return []

    def _record_launch_failures(self, run: BenchmarkRun, claims: list[dict], events: list[dict]) -> list[LaunchFailure]:
        """Remember in `status.launchFailures` each NodeClaim of the attempt that failed to launch. Karpenter keeps
        a claim with a `LaunchFailed` condition for a while, but deletes one that hit InsufficientCapacity at once,
        without any condition: that one is only in the `InsufficientCapacityError` Event on the anchor Pod. Either
        way it is a new claim a few minutes later, so the failure must be recorded here."""
        seen = {f.node_claim for f in run.status.launch_failures}
        found = {}
        for claim in claims:
            message = _launch_failed_message(claim)
            if message:
                found[_meta(claim)["name"]] = LaunchFailure(_meta(claim)["name"], message, self._now())
        for failure in self._ice_events(events):
            found.setdefault(failure.node_claim, failure)
        new = [f for name, f in found.items() if name not in seen]
        if not new:
            return run.status.launch_failures
        new.sort(key=lambda f: f.time)

        def mutate(status: RunStatus) -> None:
            known = {f.node_claim for f in status.launch_failures}
            status.launch_failures += [f for f in new if f.node_claim not in known]
            status.launch_failures = status.launch_failures[-MAX_LAUNCH_FAILURES_KEPT:]

        return self._update(run, mutate).status.launch_failures

    def _ice_events(self, events: list[dict]) -> list[LaunchFailure]:
        """The NodeClaims Karpenter reports in the anchor's `InsufficientCapacityError` Events (`NodeClaim <name>
        nominated for this pod failed to launch: <message>`)."""
        failures = []
        for event in events:
            found = ICE_EVENT.match(event.get("message") or "")
            if event.get("reason") == "InsufficientCapacityError" and found:
                failures.append(LaunchFailure(found["claim"], found["message"], _event_time(event) or self._now()))
        return failures

    def _pending_claims(self, anchor: dict) -> list[dict]:
        """NodeClaims created for this anchor's pool, vendor and SKU since the anchor exists."""
        selector = anchor["spec"]["nodeSelector"]
        created = _time(_meta(anchor).get("creationTimestamp"))
        # Before launch a NodeClaim has only the pool label; its vendor and SKU are in `spec.requirements`.
        claims = self.kube.list("nodeclaim", labels={NODEPOOL_LABEL: selector[NODEPOOL_LABEL]})
        return [
            c
            for c in claims
            if pods.claim_vendor(c) == selector[pods.VENDOR_LABEL]
            and pods.claim_api_reference(c) == selector[pods.API_REFERENCE_LABEL]
            and (created is None or (_time(_meta(c).get("creationTimestamp")) or created) >= created)
        ]

    def _retry_on_demand(self, run: BenchmarkRun, live: dict[str, dict], error: str) -> None:
        """A spot-allowed attempt whose NodeClaim failed to launch or boot: close it and start the next one, which
        is on-demand only (`spot.capacity_for_next_attempt`). A second failure ends the run, so this happens once."""
        log.warning("run %s attempt %d: %s, retrying on-demand", run.name, run.status.attempt, error)
        self._delete_run_claims(run, live)
        self._delete_anchors(live)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.LAUNCH_FAILED)
            status.phase = RunPhase.PENDING

        self._update(run, mutate)

    def _retry_provisioning(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], why: str) -> None:
        """The attempt never got a usable node: close it and start the next one, a few times."""
        lost = sum(1 for a in run.status.attempts if a.outcome == AttemptOutcome.PROVISIONING_FAILED)
        if lost + 1 >= MAX_PROVISION_RETRIES:
            return self._fail_run(run, live, launch.error_message("ANCHOR_FAILED", f"{why}, {lost + 1} attempts"))
        log.warning("run %s attempt %d: %s, retrying", run.name, run.status.attempt, why)
        self._delete_run_claims(run, live)
        self._delete_anchors(live)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.PROVISIONING_FAILED)
            status.phase = RunPhase.PENDING

        self._update(run, mutate)

    def _node_ready(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], anchor: dict) -> None:
        node = self._get_node(pods.pod_node(anchor))
        if node is None:
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
        launch.node_ready(self.kube, run.spec.vendor, run.spec.sku)  # launched: forget earlier failures
        facts = nd.facts
        if not facts.price_per_hour:
            log.warning(
                "run %s: node %s has no %s annotation; maxUsdPerHour cannot count it",
                run.name, facts.node_name, PRICE_ANNOTATION,
            )  # fmt: skip

        def mutate(status: RunStatus) -> None:
            status.phase = RunPhase.RUNNING
            status.node, status.region, status.zone = facts.node_name, facts.region, facts.zone
            status.price_per_hour = facts.price_per_hour
            status.price_incomplete = facts.price_incomplete
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
            allocatable_cpu_millicores=pods.cpu_millicores(allocatable.get("cpu", 0), round_up=False),
            vcpus=_quantity(capacity["cpu"]) if "cpu" in capacity else None,
            memory_bytes=_quantity(capacity["memory"]) if "memory" in capacity else None,
            region=labels.get("topology.kubernetes.io/region"),
            zone=labels.get("topology.kubernetes.io/zone"),
            capacity_type=CapacityType(labels[CAPACITY_LABEL]) if labels.get(CAPACITY_LABEL) else None,
            price_per_hour=annotations.get(PRICE_ANNOTATION),
            price_incomplete=annotations.get(PRICE_INCOMPLETE_ANNOTATION) == "true",
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
        selector = anchor["spec"]["nodeSelector"]
        for claim in self.kube.list("nodeclaim", labels={NODEPOOL_LABEL: selector[NODEPOOL_LABEL]}):
            if pods.claim_vendor(claim) == selector[pods.VENDOR_LABEL] and (
                claim.get("status", {}).get("nodeName") == _meta(node)["name"]
            ):
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
            nd.lscpu_stdout = raw.decode(errors="replace") if raw else None
            try:
                nd.facts.lscpu = variant.parse_lscpu_stdout(raw) if raw else {}
            except ValueError, TypeError:
                log.warning("run %s: lscpu output not parseable", run.name)
        if "gpu_facts" in ok:
            raw = self.storage.read_bytes(object_key(key, "gpu_facts", "stdout"))
            nd.facts.gpu_vram_mib, nd.facts.gpu_compute_cap = parse_gpu_facts(raw or b"")

    def _get_node(self, name: str | None) -> dict | None:
        if not name:
            return None
        try:
            return self.kube.get("node", name)
        except NotFound:
            return None

    @staticmethod
    def _draining(node: dict, claim: dict | None) -> bool:
        """The Node is being deleted or its NodeClaim carries an interruption notice."""
        return bool(_meta(node).get("deletionTimestamp")) or _interrupted(claim)

    def _node_state(self, run: BenchmarkRun, anchor: dict | None) -> tuple[_Node | None, bool]:
        """(node, draining) of the running attempt. node None: anchor or Node are gone."""
        if anchor is None or pods.is_finished(anchor):
            return None, False
        node = self._get_node(pods.pod_node(anchor))
        if node is None:
            return None, False
        nd = self._nodes.get(run.name)
        if nd is None or nd.attempt != run.status.attempt or nd.facts.node_name != _meta(node)["name"]:
            nd = self._build_node(run, node, anchor)
            self._nodes[run.name] = nd
        nd.claim = self._get_claim(nd.facts.node_claim) or self._claim_of(node, anchor)
        nd.gpu_count = _quantity(
            node.get("status", {}).get("allocatable", {}).get(GPU_RESOURCE, 0)
        )  # the plugin may register late
        nd.disk_pressure = _disk_pressure(node)
        nd.not_ready = _not_ready(node)
        return nd, self._draining(node, nd.claim)

    # ----------------------------------------------------------------------- Running

    def _running(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        anchor = live.get(pods.anchor_name(run.name, run.status.attempt))
        nd, draining = self._node_state(run, anchor)
        if nd is None:
            return self._attempt_lost(run, cfg, live, self._nodes.get(run.name))
        if run.spec.database is not None and not self._database_ready(run):
            if draining:
                return self._attempt_lost(run, cfg, live, nd)
            return self._database_up(run, cfg, live)
        if any(t.state == TaskState.RUNNING for t in run.status.tasks):
            return self._watch_step(run, cfg, live, nd, draining)
        if draining:
            return self._attempt_lost(run, cfg, live, nd)  # the node goes away: no new step on it
        self._delete_step_pods(live)  # no task is Running: every step Pod left is recorded (see the method)
        run = self._precheck(run, nd)
        run = self._ladder_failures(run, nd)
        step = self._plan(run, nd, _step_index(run.status.current_step) + 1)
        if step is None:
            return self._to_releasing(run, cfg, live)
        if self._wait_for_disk(run, nd):
            return
        self._start_step(run, cfg, live, nd, step)

    def _wait_for_disk(self, run: BenchmarkRun, nd: _Node) -> bool:
        """True while the node has DiskPressure (the kubelet would reject the Pod) and the wait of at most
        `DISK_PRESSURE_WAIT` is not used up; the run is looked at again soon."""
        if not nd.disk_pressure:
            self._disk_wait.pop(run.name, None)
            return False
        since = self._disk_wait.setdefault(run.name, self._now())
        if self._now() - since >= DISK_PRESSURE_WAIT:
            return False
        self._enqueue_later(run.name, DISK_PRESSURE_RECHECK_SECONDS)
        return True

    def _relaunch_rejected(
        self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, step: Step, pod: dict
    ) -> bool:
        """The kubelet rejected the step Pod (DiskPressure): wait for the pressure to clear, then create it once
        more. True when handled; False when it is time to record the rejection (already relaunched once, or the
        pressure outlasted the wait)."""
        name = _meta(pod)["name"]
        if (_meta(pod).get("annotations") or {}).get(RELAUNCH_ANNOTATION):
            return False  # the marker lives on the recreated Pod, so it survives a controller restart
        if self._wait_for_disk(run, nd):
            return True
        if nd.disk_pressure:
            return False
        self.kube.delete("pod", name, grace_seconds=0)
        self._launch(run, cfg, live, nd, step, annotations={**_markers(pod), RELAUNCH_ANNOTATION: "1"})
        return True

    def _probe_tasks(self, run: BenchmarkRun, nd: _Node) -> list[TaskDef]:
        """The probe step's tasks: the selected parallel tasks plus `lscpu` (always: the variant comes from it)
        and `gpu_facts` on GPU nodes, minus what the precheck refuses."""
        if run.spec.database is not None:  # the client node is not what is benchmarked
            return []
        selected = {t.name: t for t in self._selected(run)}
        wanted = {"lscpu": selected.get("lscpu") or tasks.get_task("lscpu")}
        if nd.facts.has_nvidia_gpu:
            wanted["gpu_facts"] = selected.get("gpu_facts") or tasks.get_task("gpu_facts")
        wanted |= {t.name: t for t in selected.values() if t.parallel}
        return [t for t in wanted.values() if tasks.precheck(t, nd.facts) is None]

    def _finished(self, run: BenchmarkRun) -> dict[str, TaskOutcome]:
        return {t.name: _outcome_of(t) for t in run.status.tasks if t.state == TaskState.DONE}

    def _plan(self, run: BenchmarkRun, nd: _Node, index: int) -> Step | None:
        """Step number `index` of the attempt given what is finished (none is in flight)."""
        if index == 1:
            done_here = {
                t.name for t in run.status.tasks if t.state == TaskState.DONE and t.attempt == run.status.attempt
            }
            probes = [t for t in self._probe_tasks(run, nd) if t.name not in done_here]
            if step := steps.probe_step(probes, index=1):
                return step
        finished = self._finished(run)
        remaining = [t for t in self._selected(run) if not t.parallel and t.name not in finished]
        return steps.next_step(remaining, finished, nd.facts, index=index, database=run.spec.database)

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

    def _start_step(
        self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, step: Step, *, fallback: bool = False
    ) -> None:
        """Store the step in the status with its tasks Running (one patch), then launch it."""
        now = self._now()
        run = self._update(run, lambda status: self._mark_running(status, step, now, fallback))
        self._launch(run, cfg, live, nd, step)

    @staticmethod
    def _mark_running(status: RunStatus, step: Step, now: datetime, fallback: bool) -> None:
        status.current_step, status.step = step.name, steps.to_status(step)
        for c in step.containers:
            found = _find(status, c.key)
            if found is None:
                found = TaskStatus(c.key)
                status.tasks.append(found)
            found.state, found.attempt, found.start, found.end = TaskState.RUNNING, status.attempt, now, None
            found.exit_code = found.error_msg = found.commit = None
            found.published, found.gpu_fallback = False, fallback

    def _launch(
        self,
        run: BenchmarkRun,
        cfg: Config,
        live: dict[str, dict],
        nd: _Node,
        step: Step,
        annotations: dict[str, str] | None = None,
    ) -> None:
        """Sign URLs, heartbeat, create the Pod. Every part is repeatable. URLs that do not fit a Secret cannot
        work on a retry either: the step's tasks end 256."""
        try:
            self._sign_step(run, step)
        except UrlsTooLarge as e:
            log.error("run %s step %s: %s", run.name, step.name, e)
            return self._fail_running(run, nd, live, f"cannot sign the upload URLs of the step: {e}")
        for c in step.containers:  # only this attempt's manifest may count at the end
            self.storage.clear_task(crd.run_key(run), c.key)
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
            cpu_millicores=self._step_cpu_millicores(nd, floor=_cpu_refusals(annotations) >= CPU_REFUSALS_FLOOR),
        )
        if annotations:
            _meta(pod).setdefault("annotations", {}).update(annotations)
        self._create("pod", pod)

    def _step_cpu_millicores(self, nd: _Node, *, floor: bool) -> int:
        """The step Pod's CPU request: the node's allocatable CPU minus what the Pods on it request, so the
        benchmark gets nearly all of the CPU weight kubelet derives from requests (see `pods.step_cpu_millicores`).
        `floor`: the floor, after the kubelet refused the computed request twice; also when the node's Pods
        cannot be listed (RBAC): the launch is never blocked."""
        if floor or not nd.facts.allocatable_cpu_millicores:
            return pods.CPU_FLOOR_M
        try:
            node_pods = self.kube.list_node_pods(nd.facts.node_name)
        except Forbidden:
            log.warning(
                "node %s: the Pods on it cannot be listed (forbidden), step Pod requests the floor", nd.facts.node_name
            )
            return pods.CPU_FLOOR_M
        return pods.step_cpu_millicores(nd.facts.allocatable_cpu_millicores, node_pods)

    def _relaunch_refused(
        self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, step: Step, pod: dict
    ) -> bool:
        """The kubelet refused the step Pod at admission (CPU request, DiskPressure): True when it was handled
        by creating it again (or waiting for that)."""
        if pods.is_out_of_cpu(pod):
            return self._relaunch_out_of_cpu(run, cfg, live, nd, step, pod)
        return pods.is_rejected(pod) and self._relaunch_rejected(run, cfg, live, nd, step, pod)

    def _relaunch_out_of_cpu(
        self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, step: Step, pod: dict
    ) -> bool:
        """The kubelet refused the step Pod for its CPU request (Pods appeared on the node since it was computed):
        create it again with the request recomputed from the node's current Pods; a second refusal gives the floor
        request, a third is final (False). The count lives in one annotation of the recreated Pod, so it survives
        a controller restart."""
        name = _meta(pod)["name"]
        refusals = _cpu_refusals(_meta(pod).get("annotations")) + 1
        if refusals >= CPU_REFUSALS_MAX:
            return False
        log.warning("run %s: step Pod %s refused for its CPU request (%d), recreating it", run.name, name, refusals)
        self.kube.delete("pod", name, grace_seconds=0)
        self._launch(run, cfg, live, nd, step, annotations={**_markers(pod), CPU_REFUSALS_ANNOTATION: str(refusals)})
        return True

    def _watch_step(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node, draining: bool) -> None:
        step = steps.from_status(run.status.step)
        if step is None:
            log.error("run %s: the step in flight cannot be read back from the status", run.name)
            return self._fail_running(run, nd, live, "the step in flight cannot be read back from the status")
        pod = live.get(pods.step_pod_name(run.name, run.status.attempt, step))
        if pod is None:
            if draining:
                self._attempt_lost(run, cfg, live, nd)  # evicted outright: nothing is left to wait for
            else:
                self._launch(run, cfg, live, nd, step)  # the Pod was lost: the same step again
        elif {s["name"] for s in pod["spec"]["containers"]} != {pods.container_name(c.key) for c in step.containers}:
            log.error("run %s: Pod %s does not match the stored step", run.name, _meta(pod)["name"])
            self._fail_running(run, nd, live, "the step Pod does not match the stored step")
        elif self._relaunch_refused(run, cfg, live, nd, step, pod):
            return
        elif pods.is_finished(pod):
            self._finish_step(run, cfg, live, nd, step, pod, draining)
        elif pods.overdue(pod, self._now()):
            log.warning("run %s: step Pod %s outlived its deadline, force-deleting it", run.name, _meta(pod)["name"])
            wedged = {c.key for c in step.containers if pods.container_name(c.key) in pods.unfinished_containers(pod)}
            self._finish_step(run, cfg, live, nd, step, pod, draining, wedged=wedged)
        elif stuck := pods.stuck_containers(pod, self._now(), cfg.settings.deadlines.image_pull_minutes):
            if pods.unfinished_containers(pod) <= stuck.keys():  # the others are done: nothing left to wait for
                messages = {
                    c.key: stuck[pods.container_name(c.key)]
                    for c in step.containers
                    if pods.container_name(c.key) in stuck
                }
                self._finish_step(run, cfg, live, nd, step, pod, draining, stuck=messages)
            else:
                self._resign_if_due(run, cfg)
        else:
            self._resign_if_due(run, cfg)

    def _delete_step_pods(self, live: dict[str, dict]) -> None:
        """Delete every step and flush Pod, so the kubelet can garbage-collect the images of the finished steps
        (it keeps an image while any container, exited ones included, uses it). Only called while no task is
        `Running`: the outcomes of the Pods' tasks are then in `status.tasks` and `status.json` is written, and
        nothing reads a Pod after that. Idempotent: a Pod that is gone is ignored (`Kube.delete`), so it also
        collects a Pod the controller did not get to delete before it stopped. Best effort: a failing delete is
        logged and the next reconcile retries it, so it never blocks the next step. Pods of any other role (the
        anchor, `dbaas`) stay."""
        for name, pod in live.items():
            if (_meta(pod).get("labels") or {}).get(pods.LABEL_ROLE) in (PodRole.STEP, PodRole.FLUSH):
                try:
                    self.kube.delete("pod", name)
                except Exception:
                    log.warning(
                        "cannot delete the finished Pod %s, retrying on the next reconcile", name, exc_info=True
                    )

    def _fail_running(self, run: BenchmarkRun, nd: _Node | None, live: dict[str, dict], message: str) -> None:
        """The tasks `Running` cannot be tied to a step: delete every Pod but the anchor, then end them 256."""
        for name, pod in live.items():
            if (_meta(pod).get("labels") or {}).get(pods.LABEL_ROLE) != PodRole.ANCHOR:
                self.kube.delete("pod", name)
        running = [t for t in run.status.tasks if t.state == TaskState.RUNNING]
        for t in running:
            self.storage.abort_open(crd.run_key(run), t.name)
        results = [_Result(steps.task_of(t.name), t.name, ExitCode.HARNESS_ERROR, message) for t in running]
        self._record(run, nd, results, end_step=True)

    def _finish_step(
        self,
        run: BenchmarkRun,
        cfg: Config,
        live: dict[str, dict],
        nd: _Node,
        step: Step,
        pod: dict,
        draining: bool,
        stuck: dict[str, str] | None = None,
        wedged: set[str] | None = None,
    ) -> None:
        """The step Pod ended (or, with `stuck`, only containers stuck waiting are left: their keys and the
        `error_msg` of their tasks; with `wedged`, the Pod outlived its deadline and these containers never
        ended: sc-run's uploaded result counts when it verifies, else they are `-4 DEADLINE_EXCEEDED`)."""
        pod_name = _meta(pod)["name"]
        stuck, wedged = stuck or {}, wedged or set()
        outcomes = pods.classify(pod, step.containers)
        for key, message in stuck.items():
            outcomes[key] = TaskOutcome(key, ExitCode.NEVER_FINISHED, message, from_sc_run=False)
        if draining and any(
            o.exit_code == ExitCode.NEVER_FINISHED and o.error_msg in (Reason.EVICTED, Reason.CANCELLED)
            for o in outcomes.values()
        ):
            return self._attempt_lost(run, cfg, live, nd)
        if self._wait_for_reclaim(run, nd, outcomes):
            return  # a later pass sees the Node gone or draining (then the step is lost with it) or the wait over
        retry = steps.retry_without_gpu(step, outcomes)
        retried = {c.key for c in retry.containers} if retry else set()  # run again below, not recorded now

        flushes = sorted(n for n in live if n.startswith(f"{pod_name}-flush"))
        flush_wedged = bool(flushes) and pods.overdue(live[flushes[-1]], self._now())
        if flushes and not pods.is_finished(live[flushes[-1]]) and not flush_wedged:
            return  # a flush Pod is still uploading
        key = crd.run_key(run)
        verdicts: dict[str, Verification | None] = {}
        for c in step.containers:
            if c.key in retried:
                verdicts[c.key] = None  # its uploads are reused by the new Pod
            elif c.key in stuck:
                verdicts[c.key] = None  # aborted with the Pod's deletion: the image may still arrive in a flush
            elif outcomes[c.key].container_start_error:
                self.storage.abort_open(key, c.key)  # nothing ran: nothing to verify
                verdicts[c.key] = None
            else:
                verdicts[c.key] = self.storage.finish_task(key, c.key)
        for c in step.containers:
            if c.key in wedged:
                outcomes[c.key] = self._wedged_outcome(key, c, verdicts[c.key])
        failing = [c for c in step.containers if verdicts[c.key] is not None and not verdicts[c.key].ok]
        if failing and len(flushes) < pods.FLUSH_STEPS and not flush_wedged:
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
            if c.key in retried:
                continue
            outcome, verdict = outcomes[c.key], verdicts[c.key]
            code, message = outcome.exit_code, outcome.error_msg
            if c in failing:  # the Pods are over: salvage what the uploaded parts allow
                verdict = self.storage.salvage(key, c.key, plans)
                if code == 0:
                    code, message = ExitCode.NEVER_FINISHED, Reason.OUTPUTS_LOST.value
            if c.task.images and c.key == c.task.name and code != 0:  # the benchmark after a successful probe
                message = f"benchmark failed on {steps.vllm_label(c.image)} after successful probe ({message or code})"
            results.append(_Result(c.task, c.key, code, message, outcome, verdict, c.image, pod_name))
        if stuck or retry:
            self.kube.delete("pod", pod_name)  # right before recording: nothing may flush from it before that
            for stuck_key in stuck:
                self.storage.abort_open(key, stuck_key)
        if wedged:
            self.kube.delete("pod", pod_name, grace_seconds=0)
        if flush_wedged:
            self.kube.delete("pod", flushes[-1], grace_seconds=0)
        is_probe = step.index == 1  # also the `-cpu` fallback of the probe step
        if is_probe:
            nd.facts.boot_time = next((o.btime for o in outcomes.values() if o.btime), nd.facts.boot_time)
        fallback_step = replace(retry, name=f"{step.name}-cpu") if retry else None
        run = self._record(run, nd, results, end_step=retry is None, next_step=fallback_step, fallback=True)
        if is_probe:
            self._learn_probe_facts(run, nd)
            self._write_run_json(run, nd)
        if fallback_step:
            return self._launch(run, cfg, live, nd, fallback_step)
        self._heartbeat(run)
        self._running(run, cfg, live)

    def _wait_for_reclaim(self, run: BenchmarkRun, nd: _Node, outcomes: dict[str, TaskOutcome]) -> bool:
        """True while a task that ended cancelled or evicted (the SIGTERM of a shutdown) on a spot node that does
        not look reclaimed yet is held back: the OS shutdown can end the container before the Node is deleted or
        the NodeClaim is marked, and recording the outcome now would make an interrupted task final. The hold
        lasts `RECLAIM_WAIT` after the task's end (`RECLAIM_WAIT_NOT_READY` while the Node's Ready condition is not
        True); the run is looked at again soon."""
        if nd.facts.capacity_type != CapacityType.SPOT:
            return False
        ends = [
            o.end
            for o in outcomes.values()
            if o.exit_code == ExitCode.NEVER_FINISHED and o.error_msg in (Reason.EVICTED, Reason.CANCELLED) and o.end
        ]
        if not ends:
            return False
        remaining = max(ends) + (RECLAIM_WAIT_NOT_READY if nd.not_ready else RECLAIM_WAIT) - self._now()
        if remaining <= timedelta(0):
            return False
        self._enqueue_later(run.name, min(RECLAIM_RECHECK_SECONDS, remaining.total_seconds()))
        return True

    def _wedged_outcome(self, key, container, verdict: Verification | None) -> TaskOutcome:
        """A container that never ended: its `result.json` when the uploads verify, else `-4`."""
        doc = self.storage.read_json(key, container.key, "result.json") if verdict and verdict.ok else None
        if doc and "exit_code" in doc:
            return pods.outcome_from_message(container.key, doc, container, "DeadlineExceeded")
        return TaskOutcome(container.key, ExitCode.NEVER_FINISHED, Reason.DEADLINE_EXCEEDED.value, from_sc_run=False)

    # ----------------------------------------------------------------------- losing the node

    def _attempt_lost(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict], nd: _Node | None) -> None:
        """Anchor or Node gone (spot reclaim, expiry, manual delete). Close the attempt, then either run the rest
        on a new node or end the unfinished tasks as `-4`. Several passes: the outcome is fixed in the first
        (the Node reacts to the Pods leaving), the in-flight tasks are settled in the pass that finds no step
        Pod that might still upload."""
        key = crd.run_key(run)
        attempt = self._attempt(run)
        outcome = attempt.outcome
        node = self._get_node(attempt.node)
        claim = self._get_claim(attempt.node_claim) or (nd.claim if nd else None)
        if outcome is None:
            healthy = node is not None and not self._draining(node, claim)  # only the anchor ended
            outcome = (
                AttemptOutcome.NODE_LOST if healthy else spot.classify_node_loss(run, nd.facts if nd else None, claim)
            )
            log.warning("run %s attempt %d lost its node: %s", run.name, run.status.attempt, outcome.value)
            run = self._update(run, lambda status: self._close_attempt(status, outcome))
            if outcome == AttemptOutcome.SPOT_INTERRUPTED:
                spot.record_interruption(self.kube, run.spec.vendor, run.spec.sku, self._now())
        interrupted = outcome == AttemptOutcome.SPOT_INTERRUPTED
        reason = Reason.SPOT_INTERRUPTED if interrupted else Reason.NODE_LOST

        # sc-run may still be flushing in a step Pod while the anchor ends or drains: wait until none is left
        # (a vanished Node ends them for sure).
        still_up = [n for n, p in live.items() if not pods.is_finished(p)]
        for name in still_up:
            self.kube.delete("pod", name)
        if any(live[n]["metadata"]["labels"]["inspector.sparecores.com/role"] != PodRole.ANCHOR for n in still_up) and (
            node is not None
        ):
            return

        retry = (
            interrupted
            and bool(self._unfinished(run))
            and len(run.status.attempts) < cfg.settings.spot.max_spot_attempts + EXTRA_ATTEMPTS
        )
        running = [t.name for t in run.status.tasks if t.state == TaskState.RUNNING]
        verdicts: dict[str, Verification] = {}
        if retry:
            for name in running:  # they run again: drop their open uploads
                self.storage.abort_open(key, name)
        else:
            plans = self._current_plans(run)
            verdicts = {name: self.storage.salvage(key, name, plans) for name in running}
        self._nodes.pop(run.name, None)
        if not retry:
            results = []
            for t in self._unfinished(run):
                result = _Result(steps.task_of(t.name), t.name, ExitCode.NEVER_FINISHED, reason.value)
                result.verification = verdicts.get(t.name)
                results.append(result)
            run = self._record(run, nd, results)

        def mutate(status: RunStatus) -> None:
            status.node = status.current_step = status.step = None
            if retry:
                for t in status.tasks:
                    if t.state == TaskState.RUNNING:
                        t.state, t.attempt, t.start, t.gpu_fallback = TaskState.PENDING, None, None, False
                status.phase = RunPhase.PENDING
            else:
                status.phase = RunPhase.PUBLISHING

        run = self._update(run, mutate)
        self._write_run_json(run, nd)
        self._delete_secret(run)  # last: the salvage needed its URLs list
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

        def close(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.COMPLETED)
            status.current_step = status.step = None

        run = self._update(run, close)
        self._write_run_json(run, self._nodes.pop(run.name, None))
        self._delete_anchors(live)
        self._delete_secret(run)
        self._update(run, lambda status: setattr(status, "phase", RunPhase.PUBLISHING))

    # ----------------------------------------------------------------------- cancel

    def _cancel(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        run = self._ensure_tasks(run)  # a run cancelled before its first attempt still needs its tasks
        key = crd.run_key(run)
        self._delete_run_claims(run, live)  # a node that is still booting is not consolidated by Karpenter
        for pod_name in live:
            self.kube.delete("pod", pod_name)
        for t in run.status.tasks:  # cancelled outputs are not kept: drop the open uploads
            if t.state == TaskState.RUNNING:
                self.storage.abort_open(key, t.name)
        nd = self._nodes.pop(run.name, None)
        results = [
            _Result(steps.task_of(t.name), t.name, ExitCode.NEVER_FINISHED, Reason.CANCELLED.value)
            for t in self._unfinished(run)
            if not self._clearing(run)  # no database was created: nothing to record
        ]
        run = self._record(run, nd, results)

        def mutate(status: RunStatus) -> None:
            self._close_attempt(status, AttemptOutcome.CANCELLED)
            status.phase = RunPhase.CANCELLED
            status.node = status.current_step = status.step = None

        run = self._update(run, mutate)
        self._write_run_json(run, nd)
        self._delete_secret(run)
