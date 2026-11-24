"""The sweeper: destroys the Pulumi stacks of DBaaS runs that are gone and inventories the databases of every vendor
(decision K11, docs/explanation/dbaas.md).

A run that was deleted by hand, or whose finalizer was removed, can leave its database behind. The state of every
stack `k8s-dbaas-<run>` is in the bucket, so the sweeper lists it, whatever the configuration says: a stack whose
first update is older than `dbaas.sweepAfterHours` and whose BenchmarkRun does not exist is destroyed by a
`dbaas-down-<run>` Job that belongs to no run. The sweeper deletes that Job when it is over (a failed one is created
again next cycle, so the destroy is retried until it succeeds). A stack of a vendor the configuration does not know at
all is reported and logged, not destroyed: the Job would have no credentials.
It reports every stack it sees, for the metric `k8s_inspector_dbaas_instances`.

Every `INVENTORY_INTERVAL` it also runs `dbaas-inventory-<vendor>` for each configured vendor. That Job lists the
vendor's databases that carry the `sc_inspector_run` tag, which finds a database the state never recorded; the
controller exports their number and the age of the oldest (`k8s_inspector_dbaas_tagged_*`, alert-only). A failed or
unreadable inventory drops the vendor's last result; `k8s_inspector_dbaas_inventory_last_success_timestamp_seconds`
tells when the vendor's last one worked.
"""

import contextlib
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, dbaas_jobs
from k8s_inspector.kube import AlreadyExists, Kube, NotFound
from k8s_inspector.model import Config

log = logging.getLogger(__name__)

INTERVAL_SECONDS = 600
INVENTORY_INTERVAL = timedelta(minutes=30)


@dataclass(frozen=True)
class Stack:
    run: str
    vendor: str | None  # None: no configured vendor owns the run name
    created: datetime  # when the stack was first updated: the oldest of its Pulumi history files, which no retry resets
    orphan: bool  # no BenchmarkRun of that name exists


@dataclass(frozen=True)
class Tagged:
    """What the inventory Job of a vendor found: databases carrying the `sc_inspector_run` tag."""

    count: int
    oldest_created: datetime | None
    at: datetime  # when the Job that found them was collected


def known_vendors(cfg: Config) -> set[str]:
    return set(cfg.settings.vendors) | set(cfg.settings.dbaas.vendors)


def stacks(kube: Kube, cfg: Config, storage) -> list[Stack]:
    """Every stack in the bucket, with whether its BenchmarkRun still exists. A stack's age is that of its first
    update (the oldest history file; the state file's own time when it has none), so neither a destroy that keeps
    failing nor a run of the same name created again makes a stack look young."""
    runs = {run.name: run for run in crd.list_runs(kube)}
    vendors = known_vendors(cfg)
    found = []
    for key, modified in storage.list_objects(f"{dbaas_jobs.STATE_PREFIX}.pulumi/stacks/{dbaas_jobs.STATE_PROJECT}/"):
        name = dbaas_jobs.run_of_state_key(key)
        if name:
            run = runs.get(name)
            vendor = run.spec.vendor if run else dbaas_jobs.vendor_of_run(name, vendors)
            history = [at for _, at in storage.list_objects(dbaas_jobs.state_history_prefix(name))]
            found.append(Stack(name, vendor, min(history, default=modified), run is None))
    return found


def _finish_sweeps(kube: Kube, storage, runs: set[str]) -> dict[str, dict]:
    """Delete the finished down Jobs that belong to no live run (and the state a successful one left); return every
    such Job by name."""
    existing = {}
    for job in kube.list("job", labels={dbaas_jobs.LABEL_DBAAS: "down"}):
        name = job["metadata"]["name"]
        run = (job["metadata"].get("annotations") or {}).get(dbaas_jobs.ANNOTATION_RUN, "")
        belongs_to_run = dbaas_jobs.LABEL_SWEEP not in job["metadata"].get("labels", {}) and run in runs
        if belongs_to_run:
            continue
        existing[name] = job
        state = dbaas_jobs.job_state(job)
        if state == "running":
            continue
        if state == "complete":
            dbaas_jobs.clean_state(storage, run)
            log.info("destroyed the leaked database of run %s", run)
        else:
            log.warning("the destroy of run %s failed, trying again", run)
        kube.delete("job", name)  # a failed one stays in `existing`: its retry waits for the next cycle
    return existing


def sweep(kube: Kube, cfg: Config, storage, now: datetime) -> list[Stack]:
    """One cycle: finish the sweeper's earlier Jobs, start a destroy for every orphan stack that is old enough. Returns
    the stacks seen."""
    runs = {run.name for run in crd.list_runs(kube)}
    existing = _finish_sweeps(kube, storage, runs)
    seen = stacks(kube, cfg, storage)
    limit = timedelta(hours=cfg.settings.dbaas.sweep_after_hours)
    for stack in seen:
        if not (stack.orphan and now - stack.created >= limit):
            continue
        if stack.vendor is None:
            log.warning(
                "run %s is gone and its database stack is %s old, but no configured vendor owns it: not destroying it",
                stack.run,
                now - stack.created,
            )
        elif dbaas_jobs.down_job_name(stack.run) not in existing:
            log.warning(
                "run %s is gone but its database stack is %s old, destroying it", stack.run, now - stack.created
            )
            with contextlib.suppress(AlreadyExists):
                kube.create("job", dbaas_jobs.down_job(stack.run, stack.vendor, cfg, sweep=True))
    return seen


def _tagged(message: dict, now: datetime) -> Tagged:
    """The result message of an inventory Job; a naive timestamp counts as UTC. Raises on anything unreadable."""
    oldest = message.get("oldest_created_at")
    created = datetime.fromisoformat(oldest) if oldest else None
    if created is not None and created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    return Tagged(int(message["count"]), created, now)


def inventory(kube: Kube, cfg: Config, now: datetime, started: dict[str, datetime]) -> dict[str, Tagged | None]:
    """One inventory cycle: collect the finished `dbaas-inventory-<vendor>` Jobs, start one for every configured vendor
    that has not had one for `INVENTORY_INTERVAL` (`started`: when, per vendor; updated). Returns what the finished
    Jobs found: `None` for a vendor whose Job failed or reported something unreadable. A finished Job is always
    deleted."""
    found: dict[str, Tagged | None] = {}
    for vendor in cfg.settings.dbaas.vendors:
        name = dbaas_jobs.inventory_job_name(vendor)
        try:
            job = kube.get("job", name)
        except NotFound:
            if now - started.get(vendor, datetime.min.replace(tzinfo=UTC)) >= INVENTORY_INTERVAL:
                started[vendor] = now
                with contextlib.suppress(AlreadyExists):
                    kube.create("job", dbaas_jobs.inventory_job(vendor, cfg))
            continue
        state = dbaas_jobs.job_state(job)
        if state == "running":
            continue
        message = dbaas_jobs.job_message(kube, name)
        error = message.get("error", "no result")
        tagged = None
        if state == "complete" and "count" in message:
            try:
                tagged = _tagged(message, now)
            except (ValueError, TypeError) as e:
                error = f"unreadable result: {e}"
        if tagged is None:
            log.warning("the database inventory of vendor %s failed: %s", vendor, error)
        found[vendor] = tagged
        kube.delete("job", name)
    return found


def run_forever(
    kube: Kube,
    cfg_provider,
    storage,
    stop: threading.Event,
    on_stacks=lambda stacks: None,
    on_inventory=lambda tagged, last_success: None,
) -> None:
    """Every `INTERVAL_SECONDS`: `sweep` and `inventory`; exceptions are logged, never fatal. `on_stacks` gets the
    stacks seen, `on_inventory` the latest good result of every configured vendor (none for a vendor whose latest
    inventory failed) and when each vendor's last inventory worked."""
    started: dict[str, datetime] = {}
    latest: dict[str, Tagged] = {}
    last_success: dict[str, datetime] = {}
    while not stop.is_set():
        try:
            cfg, now = cfg_provider(), datetime.now(UTC)
            on_stacks(sweep(kube, cfg, storage, now))
            for vendor, tagged in inventory(kube, cfg, now, started).items():
                if tagged is None:
                    latest.pop(vendor, None)
                else:
                    latest[vendor], last_success[vendor] = tagged, tagged.at
            latest = {vendor: tagged for vendor, tagged in latest.items() if vendor in cfg.settings.dbaas.vendors}
            on_inventory(latest, dict(last_success))
        except Exception:
            log.exception("DBaaS sweep failed")
        deadline = time.monotonic() + INTERVAL_SECONDS
        while not stop.is_set() and time.monotonic() < deadline:
            stop.wait(1.0)
