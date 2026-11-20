"""Planner: which SKUs get a BenchmarkRun, forced (`force_run`) or by itself (`plan_cycle`, autoPlan).

Owner: planner agent. Implements docs/how-to/run-forced.md, docs/reference/configuration.md (planner,
vendors), state.md "What git still records", exit-codes.md "Planner retry" and the planning rows of
feature-mapping.md. The lease is the BenchmarkRun name: creating a second run for a SKU raises `RunExists`.
"""

import logging
import threading
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, keeper, pods, publisher, tasks
from k8s_inspector.kube import Kube
from k8s_inspector.model import BenchmarkRun, CapacityMode, Config, ExitCode, PodRole, RunPhase, RunSpec, TaskDef

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
    every server task, except the `keeper_gated` ones, which run only when named; DBaaS tasks need a database
    and are refused); the vendor is below `maxInFlight` and `maxUsdPerHour`. Without `force` Keeper must also find
    every task eligible (`keeper.block_reasons`; an unknown SKU is blocked, an unreachable Keeper fails closed).
    With `force` the SKU is not checked here: a SKU SCarpenter cannot offer ends in `-1 SCARPENTER_NO_OFFERING`."""
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

    return _create_run(kube, cfg, vendor, sku, names, force=force, capacity=capacity, hold_after=hold_after)


def _create_run(
    kube: Kube,
    cfg: Config,
    vendor: str,
    sku: str,
    names: list[str],
    *,
    force: bool = False,
    capacity: CapacityMode | None = None,
    hold_after: timedelta = timedelta(0),
) -> BenchmarkRun:
    spec = RunSpec(
        vendor=vendor,
        sku=sku,
        run_id=crd.new_run_id(datetime.now(UTC)),
        tasks=names,
        force=force,
        capacity=capacity or cfg.settings.vendors[vendor].capacity,
        hold_after=hold_after,
    )
    run = crd.create_run(kube, spec)
    log.info("created run", extra={"run": run.name, "vendor": vendor, "sku": sku, "tasks": ",".join(names)})
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


# `schedule/<task>/meta.json` of every SKU of a vendor: {sku: {task: meta}} (`Publisher.read_schedule_metas`)
ScheduleReader = Callable[[str], dict[str, dict[str, dict]]]

# Wait after a failure before the task is due again (exit-codes.md "Planner retry"): a `-4` by its `start_retries`
# (the third `-4` is `-5`, final), a `-1` flat; no capacity or quota at all (`QUOTA_ZERO`, `NO_OFFERING`) only daily.
RETRY_AFTER_NEVER_FINISHED = (timedelta(hours=2), timedelta(hours=6))
RETRY_AFTER_NO_NODE = timedelta(hours=2)
RETRY_AFTER_NO_NODE_DAILY = timedelta(hours=24)
DAILY_NO_NODE_REASONS = ("SCARPENTER_QUOTA_ZERO", "SCARPENTER_NO_OFFERING")


def due_reason(task: TaskDef, meta: dict | None, now: datetime) -> str | None:
    """Why `task` is due on a SKU whose `schedule/<task>/meta.json` is `meta` (None: never run), else None.
    Port of sc-inspector `lib.should_start`: a changed (or `RERUN_`-prefixed) hash, no result yet; a finished
    result with the current hash is final (success, a task's own exit code, `-3`, `-6`, `256`, `-5`) except the
    retried failures `-1` and `-4`, which wait for their backoff. `now` is naive UTC like the meta times."""
    if meta is None:
        return "never run"
    if meta.get("task_hash") != tasks.task_hash(task):
        return "task hash changed"
    code, end = meta.get("exit_code"), meta.get("end")
    if code is None or end is None:
        return "no result"
    code = int(code)
    if code == ExitCode.NO_NODE:
        daily = str(meta.get("error_msg") or "").startswith(DAILY_NO_NODE_REASONS)
        wait = RETRY_AFTER_NO_NODE_DAILY if daily else RETRY_AFTER_NO_NODE
    elif code == ExitCode.NEVER_FINISHED:
        retries = min(max(int(meta.get("start_retries") or 1), 1), len(RETRY_AFTER_NEVER_FINISHED))
        wait = RETRY_AFTER_NEVER_FINISHED[retries - 1]
    else:
        return None
    if now - datetime.fromisoformat(end).replace(tzinfo=None) >= wait:
        return f"retry after exit code {code}"
    return None


def start_order(vendor: str, sku: str) -> tuple[int, str]:
    """Legacy `lib.instance_start_order_key`: crc32 of `vendor/sku` mixes the sizes (price order would exhaust the
    quota of a new vendor on the small SKUs first); the SKU name breaks ties."""
    return zlib.crc32(f"{vendor}/{sku}".encode()), sku


def plan_cycle(kube: Kube, cfg: Config, schedule_reader: ScheduleReader, *, dry_run: bool = False) -> list[PlannedRun]:
    """One autoPlan cycle. For each enabled vendor with autoPlan whose results are published (`publish`; without it
    git would never record them and the SKU would be run again and again) and that is below its caps
    (`check_vendor_budget`): take the vendor's ACTIVE SKUs from Keeper in `start_order`, skip those that already
    have a BenchmarkRun (the lease), and create a run (not forced) for the first SKU with due tasks (`due_reason`
    on the git schedule metas) that Keeper does not block, until the caps are reached: usually one run per vendor
    and cycle. With `dry_run` nothing is created and the first such SKU of each vendor is returned.

    Fails closed: when Keeper or git cannot be read the cycle plans nothing (for git: nothing for that vendor)
    and logs why."""
    settings = cfg.settings
    candidates = []
    for vendor, vendor_settings in settings.vendors.items():
        auto = settings.auto_plan if vendor_settings.auto_plan is None else vendor_settings.auto_plan
        if not (vendor_settings.enabled and auto):
            continue
        if not publisher.vendor_allowed(settings, vendor):
            log.warning("autoPlan of vendor %s needs publish: results would never reach git", vendor)
            continue
        try:
            check_vendor_budget(kube, cfg, vendor, crd.list_runs(kube))
        except ValueError as e:
            log.info("autoPlan skips vendor %s: %s", vendor, e)
            continue
        candidates.append(vendor)
    if not candidates:
        return []

    server_tasks = [task for task in tasks.all_tasks() if not task.dbaas]
    try:
        blocked = keeper.active_servers_block_reasons(cfg.env, [task.name for task in server_tasks])
    except Exception:
        log.exception("autoPlan skips the cycle: Keeper is not reachable")
        return []

    now = datetime.now(UTC).replace(tzinfo=None)
    planned = []
    for vendor in candidates:
        try:
            metas = schedule_reader(vendor)
        except Exception:
            log.exception("autoPlan skips vendor %s: cannot read the sc-inspector-data state", vendor)
            continue
        runs = crd.list_runs(kube)
        leased = {run.name for run in runs}
        skus = sorted(blocked.get(vendor, {}), key=lambda sku: start_order(vendor, sku))
        for sku in skus:
            if crd.run_name(vendor, sku) in leased or not publisher.publish_allowed(settings, vendor, sku):
                continue
            due = {}
            for task in server_tasks:
                if task.name in blocked[vendor][sku]:
                    continue
                reason = due_reason(task, metas.get(sku, {}).get(task.name), now)
                if reason:
                    due[task.name] = reason
            if not due:
                continue
            if not dry_run:
                try:
                    check_vendor_budget(kube, cfg, vendor, runs)
                except ValueError as e:
                    log.info("autoPlan stops for vendor %s: %s", vendor, e)
                    break
                try:
                    runs.append(_create_run(kube, cfg, vendor, sku, list(due)))
                except crd.RunExists:
                    continue
            reason = ", ".join(f"{name}: {why}" for name, why in due.items())
            planned.append(PlannedRun(vendor, sku, tuple(due), reason))
            if dry_run:
                break
    return planned


def run_forever(kube: Kube, cfg_provider, schedule_reader: ScheduleReader, stop: threading.Event) -> None:
    """Every `planner.interval_minutes`: `plan_cycle`; exceptions are logged, never fatal. `cfg_provider` is a
    zero-argument callable returning the current `Config`."""
    while not stop.is_set():
        interval_minutes = 5
        try:
            cfg = cfg_provider()
            interval_minutes = cfg.settings.planner_interval_minutes
            for planned in plan_cycle(kube, cfg, schedule_reader):
                log.info("planned run", extra={"vendor": planned.vendor, "sku": planned.sku, "reason": planned.reason})
        except Exception:
            log.exception("planning cycle failed")
        stop.wait(interval_minutes * 60)
