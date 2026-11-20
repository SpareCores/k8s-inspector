"""The run state machine: a watch-driven reconciler for BenchmarkRuns.

Owner: runner agent. Implements docs/explanation/run-model.md, state.md ("How a restarted controller knows
what is ongoing"), outputs.md "In the controller", spot.md "How k8s-inspector reacts".

Threading model (see the package docstring): a single worker thread drains `self._queue` of run names, so
`reconcile` never runs concurrently with itself and needs no locks. Producers: the BenchmarkRun and Pod watch
threads (one event -> the run named by the `inspector.sparecores.com/run` label), a 5-minute full re-LIST
resync, an hourly re-sign timer per running run, and `enqueue` from the CLI/planner. The publisher thread
writes only `published`/`commit` of tasks through `crd.update_status`.

`reconcile(name)` is idempotent and level-triggered: it reads the BenchmarkRun, its Pods and (when needed)
the Node/NodeClaim, decides the one next action and does it, then returns. It never blocks waiting for a Pod.

Reconcile by phase (all names deterministic, `AlreadyExists` = already done):
- Pending: choose capacity (`spot.capacity_for_next_attempt`), render + create the anchor
  (`pods.render_anchor`), attempt += 1, record the attempt, phase Provisioning.
- Provisioning: anchor Running+ready on a Node created after the anchor -> record NodeFacts (region, zone,
  capacity type, price, node timings, node_os...) in status, phase Running. Node older than the anchor ->
  delete anchor and its NodeClaim, retry. Pending longer than `deadlines.provision_*` or NodeClaim
  condition/Event with a capacity or quota error -> tasks end `-1 SCARPENTER_<ICE|QUOTA|QUOTA_ZERO|
  NO_OFFERING|BOOT_FAILURE>: <message>`, phase Failed.
- Running: no step Pod yet -> pick the step (`steps.probe_step` first, then `steps.next_step`), `plan_uploads`
  + `presign` -> `apply_secret(urls)`, `exec_heartbeat`, create the step Pod. Step Pod finished ->
  `pods.classify`, `storage.finish_task` per container, flush step (max 2) when not ok, then
  `write_status_json`, update `status.tasks`, `Storage.write_run_json`. Pull stuck > limit -> delete,
  `-4 IMAGE_PULL_TIMEOUT`. A `retry_without_gpu` step is created once. After the probe step: parse `lscpu` into
  `NodeFacts.lscpu` (and GPU facts) for `steps.next_step`; the variant number itself is assigned by the
  publisher at publish time (needs the git tree) and written to `status.variant`; re-evaluating due tasks
  against a variant is autoPlan work, deferred. No step left -> phase Releasing.
- Anchor gone or Node gone while Running (spot, expiry, cancel): `spot.classify_node_loss`, salvage the
  in-flight task (`storage.salvage`) as `-4`, close the attempt, start the next attempt with the remaining
  tasks, or end the remaining tasks as `-4` when budgets are exhausted.
- Releasing: honour `spec.hold_after`, delete the anchor, phase Publishing.
- Publishing: the publisher marks tasks published; when all are, delete the BenchmarkRun after
  `publish.keep_run_hours`. Deleted BenchmarkRun (cancel): unfinished tasks recorded `-4 CANCELLED`, anchor
  deleted.
- Every `uploads.resign_every_minutes` while a step runs: `storage.plans_from_urls_json` ->
  `presign` -> `apply_secret`.
"""

import queue
import threading

from k8s_inspector.kube import Kube
from k8s_inspector.storage import Storage


class Runner:
    def __init__(self, kube: Kube, storage: Storage, cfg_provider, namespace: str) -> None:
        """`cfg_provider`: zero-argument callable returning the current `Config` (settings are re-read from the
        ConfigMap every cycle, so a callable is passed instead of a snapshot)."""
        self.kube = kube
        self.storage = storage
        self._cfg = cfg_provider
        self._queue: queue.Queue[str] = queue.Queue()
        raise NotImplementedError

    def enqueue(self, run_name: str) -> None:
        """Ask for a reconcile of one run. Thread-safe; duplicates are harmless."""
        raise NotImplementedError

    def reconcile(self, run_name: str) -> None:
        """One reconcile pass of one run (see module docstring). Exceptions are logged by the worker loop and
        the run is re-queued with backoff; a bug in one run must not stop the others."""
        raise NotImplementedError

    def cancel(self, run_name: str) -> None:
        """Mark the run cancelled (delete the BenchmarkRun; `reconcile` finishes the bookkeeping)."""
        raise NotImplementedError

    def run_forever(self, stop: threading.Event) -> None:
        """Start the watch threads (BenchmarkRuns, labelled Pods), the resync/re-sign timer and the worker loop
        in this thread until `stop` is set. On start and after every watch end: LIST, enqueue all runs."""
        raise NotImplementedError
