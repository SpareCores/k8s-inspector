"""The sweeper: destroys the Pulumi stacks of DBaaS runs that are gone (decision K11, docs/explanation/dbaas.md).

A run that was deleted by hand, or whose finalizer was removed, can leave its database behind. The state of every stack
`k8s-dbaas-<run>` is in the bucket, so the sweeper lists it: a stack older than `dbaas.sweepAfterHours` whose
BenchmarkRun does not exist is destroyed by a `dbaas-down-<run>` Job that belongs to no run. The sweeper deletes that
Job when it is over
(a failed one is created again next cycle, so the destroy is retried until it succeeds). It also reports every stack it
sees, for the metric `k8s_inspector_dbaas_instances`.
"""

import contextlib
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, dbaas_jobs
from k8s_inspector.kube import AlreadyExists, Kube
from k8s_inspector.model import Config
from k8s_inspector.runner_dbaas import job_state

log = logging.getLogger(__name__)

INTERVAL_SECONDS = 600


@dataclass(frozen=True)
class Stack:
    run: str
    vendor: str
    modified: datetime  # the last change of its state file
    orphan: bool  # no BenchmarkRun of that name exists


def stacks(kube: Kube, storage) -> list[Stack]:
    """Every stack in the bucket, with whether its BenchmarkRun still exists."""
    runs = {run.name for run in crd.list_runs(kube)}
    found = []
    for key, modified in storage.list_objects(f"{dbaas_jobs.STATE_PREFIX}.pulumi/stacks/{dbaas_jobs.STATE_PROJECT}/"):
        run = dbaas_jobs.run_of_state_key(key)
        if run:
            found.append(Stack(run, dbaas_jobs.vendor_of_run(run), modified, run not in runs))
    return found


def sweep(kube: Kube, cfg: Config, storage, now: datetime) -> list[Stack]:
    """One cycle: finish the sweeper's earlier Jobs, start a destroy for every orphan stack that is old enough. Returns
    the stacks seen. Without a `dbaas.vendors` section DBaaS is not set up, and nothing is listed."""
    if not cfg.settings.dbaas.vendors:
        return []
    existing = {job["metadata"]["name"]: job for job in kube.list("job", labels={dbaas_jobs.LABEL_SWEEP: "true"})}
    for name, job in existing.items():
        state = job_state(job)
        if state == "running":
            continue
        run = name.removeprefix("dbaas-down-")
        if state == "complete":
            dbaas_jobs.clean_state(storage, run)
            log.info("swept the leaked database of run %s", run)
        else:
            log.warning("the sweep of run %s failed, trying again", run)
        kube.delete("job", name)
    seen = stacks(kube, storage)
    limit = timedelta(hours=cfg.settings.dbaas.sweep_after_hours)
    for stack in seen:
        job = dbaas_jobs.down_job_name(stack.run)
        if stack.orphan and now - stack.modified >= limit and job not in existing:
            log.warning(
                "run %s is gone but its database stack is %s old, destroying it", stack.run, now - stack.modified
            )
            with contextlib.suppress(AlreadyExists):
                kube.create("job", dbaas_jobs.down_job(stack.run, stack.vendor, cfg, None))
    return seen


def run_forever(kube: Kube, cfg_provider, storage, stop: threading.Event, on_stacks=lambda stacks: None) -> None:
    """Every `INTERVAL_SECONDS`: `sweep`; exceptions are logged, never fatal. `on_stacks` gets the stacks seen."""
    while not stop.is_set():
        try:
            on_stacks(sweep(kube, cfg_provider(), storage, datetime.now(UTC)))
        except Exception:
            log.exception("DBaaS sweep failed")
        deadline = time.monotonic() + INTERVAL_SECONDS
        while not stop.is_set() and time.monotonic() < deadline:
            stop.wait(1.0)
