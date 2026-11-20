"""Planner: which SKUs get a BenchmarkRun. P1 = forced runs only; autoPlan is a stub for later.

Owner: planner agent. Implements docs/how-to/run-forced.md, docs/reference/configuration.md (planner,
vendors, batching), state.md "What git still records", exit-codes.md "Planner retry" and the planning rows of
feature-mapping.md. The lease is the BenchmarkRun name: creating a second run for a SKU raises `RunExists`.
"""

import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, keeper, pods, tasks
from k8s_inspector.kube import Kube
from k8s_inspector.model import BenchmarkRun, CapacityMode, Config, PodRole, RunPhase, RunSpec, TaskDef

log = logging.getLogger(__name__)

# Phases in which a run holds (or is about to hold) a node. A run in any other phase (Failed, Cancelled) still
# counts while its anchor Pod exists, until the runner deletes the anchor and releases the node.
ACTIVE_PHASES = frozenset({RunPhase.PENDING, RunPhase.PROVISIONING, RunPhase.RUNNING, RunPhase.RELEASING})
PRICE_ANNOTATION = "scarpenter.sparecores.com/price"


@dataclass(frozen=True)
class PlannedRun:
    vendor: str
    sku: str
    tasks: tuple[str, ...]  # due task names
    reason: str  # for logs and `plan --dry-run`


def spend_per_hour(runs: list[BenchmarkRun]) -> float:
    """Sum of the node prices (USD/hour) of the given runs; a missing or unparsable price counts as 0."""
    total = 0.0
    for run in runs:
        try:
            total += float(run.status.price_per_hour or 0)
        except ValueError:
            log.warning("unparsable price %r", run.status.price_per_hour, extra={"run": run.name})
    return total


def holding_runs(kube: Kube, runs: list[BenchmarkRun], vendor: str) -> list[BenchmarkRun]:
    """Runs of `vendor` that hold or are acquiring a node: in an active phase, or (Failed, Cancelled) with an
    anchor Pod still alive."""
    anchored = {
        _labels(pod).get(crd.LABEL_RUN) for pod in kube.list("pod", labels={pods.LABEL_ROLE: PodRole.ANCHOR.value})
    }
    return [r for r in runs if r.spec.vendor == vendor and (r.status.phase in ACTIVE_PHASES or r.name in anchored)]


def _labels(pod: dict) -> dict:
    return pod.get("metadata", {}).get("labels") or {}


def live_nodeclaims(kube: Kube, cfg: Config, vendor: str) -> int:
    """Live NodeClaims of `vendor` in the bench NodePool. Only the pool label is there from creation: the vendor
    comes from the label after launch, before it from `spec.requirements` (`pods.claim_vendor`). An instance can
    outlive its anchor (provision deadline passed while Karpenter still launches; drain after release)."""
    claims = kube.list("nodeclaim", labels={pods.NODEPOOL_LABEL: cfg.settings.bench_pool})
    return sum(1 for claim in claims if pods.claim_vendor(claim) == vendor)


def check_vendor_budget(kube: Kube, cfg: Config, vendor: str, runs: list[BenchmarkRun]) -> None:
    """Raise `ValueError` when one more run of `vendor` would pass `maxInFlight` (0 pauses the vendor), or when
    `maxUsdPerHour` is set and the holding runs already cost the cap, or one of them has no price (a run still in
    Pending or Provisioning: wait; a run with a node: unknown spend, refused, with a warning). In flight =
    max(`holding_runs`, live bench NodeClaims), a lower bound ("at least"): leftover NodeClaims without a holding
    run add to the count but not to the spend. Shared by `force_run` and autoPlan. Not atomic: two
    concurrent callers can both pass the check."""
    settings = cfg.settings
    vendor_settings = settings.vendors[vendor]
    limit = vendor_settings.max_in_flight
    if limit is None:
        limit = settings.defaults.max_in_flight
    active = holding_runs(kube, runs, vendor)
    in_flight = max(len(active), live_nodeclaims(kube, cfg, vendor))
    if in_flight >= limit:
        raise ValueError(f"vendor {vendor} is at its maxInFlight limit ({in_flight} of {limit} instances in flight)")
    cap = vendor_settings.max_usd_per_hour
    if cap is None:
        return
    unpriced = [r for r in active if not r.status.price_per_hour]
    for run in unpriced:
        if run.status.node:
            log.warning("run %s has a node but no price annotation", run.name, extra={"run": run.name})
    no_node_yet = [r.name for r in unpriced if r.status.phase in (RunPhase.PENDING, RunPhase.PROVISIONING)]
    no_price = [r.name for r in unpriced if r.name not in no_node_yet]
    if no_price:
        raise ValueError(
            f"vendor {vendor} has maxUsdPerHour {cap}: the node of {', '.join(no_price)} has no price annotation "
            f"{PRICE_ANNOTATION} (the operator must check SCarpenter), so the spend is unknown"
        )
    if no_node_yet:
        raise ValueError(
            f"vendor {vendor} has maxUsdPerHour {cap}: wait until {', '.join(no_node_yet)} has a price "
            "before starting a run"
        )
    if spend_per_hour(active) >= cap:
        raise ValueError(f"vendor {vendor} is at its maxUsdPerHour cap ({cap} USD/hour)")


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
    """`k8s-inspector run`: validate, build the RunSpec (run id from `crd.new_run_id`, capacity default from the
    vendor setting) and `crd.create_run`. Raise `ValueError` for validation errors and `crd.RunExists` when the
    SKU already has a run.

    Validation: the vendor is `enabled`; every task name is in the catalogue (`tasks.select_tasks`; empty =
    every server task; DBaaS tasks need a database and are refused); the vendor is below `maxInFlight` and
    `maxUsdPerHour`. Without `force` Keeper must also find every task eligible (`keeper.block_reasons`; an
    unknown SKU is blocked, an unreachable Keeper fails closed). With `force` the SKU is not checked here: a
    SKU SCarpenter cannot offer ends in `-1 SCARPENTER_NO_OFFERING` (Keeper does not know OCI SKUs)."""
    settings = cfg.settings
    vendor_settings = settings.vendors.get(vendor)
    if vendor_settings is None or not vendor_settings.enabled:
        raise ValueError(f"vendor {vendor} is not enabled in the ConfigMap")
    if not sku.strip():
        raise ValueError("SKU must not be empty")

    selected = _select(tasks or [])
    names = [task.name for task in selected]
    if not force:
        _check_keeper(cfg, vendor, sku, names)
    check_vendor_budget(kube, cfg, vendor, crd.list_runs(kube))

    spec = RunSpec(
        vendor=vendor,
        sku=sku,
        run_id=crd.new_run_id(datetime.now(UTC)),
        tasks=names,
        force=force,
        capacity=capacity or vendor_settings.capacity,
        hold_after=hold_after,
    )
    run = crd.create_run(kube, spec)
    log.info("created run", extra={"run": run.name, "vendor": vendor, "sku": sku, "tasks": len(names)})
    return run


def _select(names: list[str]) -> list[TaskDef]:
    selected = tasks.select_tasks(names)
    dbaas = [task.name for task in selected if task.dbaas]
    if dbaas:
        raise ValueError(f"DBaaS tasks cannot run on a server SKU: {dbaas}")
    return selected


def _check_keeper(cfg: Config, vendor: str, sku: str, names: list[str]) -> None:
    try:
        blocked = keeper.block_reasons(cfg.env, vendor, [sku], names)[sku]
    except Exception as e:  # any Keeper failure (HTTP, network, key file, bad JSON) is a clean error, not a traceback
        raise ValueError(f"cannot check eligibility with Keeper: {e} (use --force to skip the check)") from e
    if blocked:
        details = "; ".join(f"{name}: {', '.join(reasons)}" for name, reasons in sorted(blocked.items()))
        raise ValueError(f"Keeper blocks tasks for {vendor}/{sku} ({details}); use --force to skip the check")


def plan_cycle(kube: Kube, cfg: Config, *, dry_run: bool = False) -> list[PlannedRun]:
    """One autoPlan cycle: for each enabled vendor with autoPlan, respect `max_in_flight`/`max_usd_per_hour`
    (`check_vendor_budget`), fetch eligibility (`keeper.block_reasons`), read git state of the checkout
    (due tasks: hash change, `RERUN_`, backoff for -1/-4, `start_retries`, cap 3 -> -5), apply `batching`,
    create runs unless `dry_run`. STUB in P1 (forced runs only, roadmap): plans nothing; it warns when a
    vendor asks for autoPlan so the setting is not silently ignored."""
    settings = cfg.settings
    for vendor, vendor_settings in settings.vendors.items():
        auto = settings.auto_plan if vendor_settings.auto_plan is None else vendor_settings.auto_plan
        if vendor_settings.enabled and auto:
            log.warning("autoPlan is not implemented yet, vendor %s is only run on demand", vendor)
    return []


def run_forever(kube: Kube, cfg_provider, stop: threading.Event) -> None:
    """Every `planner.interval_minutes`: `plan_cycle`; exceptions are logged, never fatal. `cfg_provider` is a
    zero-argument callable returning the current `Config`."""
    while not stop.is_set():
        interval_minutes = 5
        try:
            cfg = cfg_provider()
            interval_minutes = cfg.settings.planner_interval_minutes
            for planned in plan_cycle(kube, cfg):
                log.info("planned run", extra={"vendor": planned.vendor, "sku": planned.sku, "reason": planned.reason})
        except Exception:
            log.exception("planning cycle failed")
        stop.wait(interval_minutes * 60)
