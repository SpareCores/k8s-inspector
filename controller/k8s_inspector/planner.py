"""Planner: which SKUs get a BenchmarkRun. P1 = forced runs only; autoPlan is a stub for later.

Owner: planner agent. Implements docs/how-to/run-forced.md, docs/reference/configuration.md (planner,
vendors, batching), state.md "What git still records", exit-codes.md "Planner retry" and the planning rows of
feature-mapping.md. The lease is the BenchmarkRun name: creating a second run for a SKU raises `RunExists`.
"""

import threading
from dataclasses import dataclass
from datetime import timedelta

from k8s_inspector.kube import Kube
from k8s_inspector.model import BenchmarkRun, CapacityMode, Config


@dataclass(frozen=True)
class PlannedRun:
    vendor: str
    sku: str
    tasks: tuple[str, ...]  # due task names
    reason: str  # for logs and `plan --dry-run`


def force_run(
    kube: Kube,
    cfg: Config,
    vendor: str,
    sku: str,
    *,
    tasks: list[str] | None = None,
    force: bool = True,
    capacity: CapacityMode | None = None,
    hold_after: timedelta = timedelta(0),
) -> BenchmarkRun:
    """`k8s-inspector run`: validate (vendor `enabled`, tasks known via `tasks.select_tasks`), build the
    RunSpec (run id from `crd.new_run_id`, capacity default from the vendor setting) and `crd.create_run`.
    Raise `ValueError` for validation errors and `crd.RunExists` when the SKU already has a run."""
    raise NotImplementedError


def plan_cycle(kube: Kube, cfg: Config, *, dry_run: bool = False) -> list[PlannedRun]:
    """One autoPlan cycle: for each enabled vendor with autoPlan, respect `max_in_flight`/`max_usd_per_hour`
    (count BenchmarkRuns), fetch eligibility (`keeper.block_reasons`), read git state of the checkout
    (due tasks: hash change, `RERUN_`, backoff for -1/-4, `start_retries`, cap 3 -> -5), apply `batching`,
    create runs unless `dry_run`. STUB in P1: returns [] while neither `planner.autoPlan` nor the vendor's
    override is true."""
    raise NotImplementedError


def run_forever(kube: Kube, cfg_provider, stop: threading.Event) -> None:
    """Every `planner.interval_minutes`: `plan_cycle`; exceptions are logged, never fatal."""
    raise NotImplementedError
