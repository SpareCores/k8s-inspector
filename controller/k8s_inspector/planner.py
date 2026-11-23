"""Planner: which SKUs get a BenchmarkRun, forced (`force_run`) or by itself (`plan_cycle`, autoPlan).

Owner: planner agent. Implements docs/how-to/run-forced.md, docs/reference/configuration.md (planner,
vendors), state.md "What git still records", exit-codes.md "Planner retry" and the planning rows of
feature-mapping.md. The lease is the BenchmarkRun name: creating a second run for a SKU raises `RunExists`.
"""

import base64
import gzip
import json
import logging
import threading
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, keeper, launch, pods, publisher, tasks
from k8s_inspector.kube import Kube, NotFound
from k8s_inspector.model import (
    BenchmarkRun,
    CapacityMode,
    CapacityType,
    Config,
    ExitCode,
    PodRole,
    RunPhase,
    RunSpec,
    TaskDef,
)

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


KEEPER_CACHE_SECONDS = 30 * 60


class KeeperCache:
    """The bulk Keeper answer (`keeper.active_servers_block_reasons`), kept for `KEEPER_CACHE_SECONDS`: the ACTIVE
    server list and block reasons change rarely, a cycle runs every few minutes. `plan_cycle` without a cache
    object asks Keeper every time."""

    def __init__(self) -> None:
        self._asked = 0.0
        self._answer: dict | None = None

    def get(self, env, task_names: list[str]) -> dict:
        if self._answer is None or time.monotonic() - self._asked >= KEEPER_CACHE_SECONDS:
            self._answer = keeper.active_servers_block_reasons(env, task_names)
            self._asked = time.monotonic()
        return self._answer


# `schedule/<task>/meta.json` of every SKU of a vendor: {sku: {task: meta}} (`Publisher.read_schedule_metas`);
# a meta of None is an unreadable file, its SKU is skipped
ScheduleReader = Callable[[str], dict[str, dict[str, dict | None]]]

# Wait after a failure before the task is due again (exit-codes.md "Planner retry"): a `-4` by its `start_retries`
# (the third `-4` is `-5`, final), a `-1` flat, a permanent launch failure
# (`launch.PERMANENT`) after 30 days. A `-1` of a temporary reason (`launch.is_temporary`; no longer written, but
# older metas have them) is ignored: the launch ConfigMap and SCarpenter's offered-types marks pace the retries.
RETRY_AFTER_NEVER_FINISHED = (timedelta(hours=2), timedelta(hours=6))
RETRY_AFTER_NO_NODE = timedelta(hours=2)
GPU_PROBE_TASK = "nvidia_smi"  # Keeper blocks it for a SKU without an NVIDIA GPU


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
        error_msg = meta.get("error_msg")
        if launch.is_temporary(error_msg):
            return "temporary launch failure"
        reason = launch.reason_of_error(error_msg)
        wait = launch.RETRY_AFTER_PERMANENT if reason in launch.PERMANENT else RETRY_AFTER_NO_NODE
    elif code == ExitCode.NEVER_FINISHED:
        retries = min(max(int(meta.get("start_retries") or 1), 1), len(RETRY_AFTER_NEVER_FINISHED))
        wait = RETRY_AFTER_NEVER_FINISHED[retries - 1]
    else:
        return None
    if now - datetime.fromisoformat(end).replace(tzinfo=None) >= wait:
        return f"retry after exit code {code}"
    return None


# The newest hardware variant of each SKU (`Publisher.read_newest_variants`): {sku: (N, names of the tasks with a
# `data/<vendor>/<sku>/<N>/<task>/meta.json`)}
VariantReader = Callable[[str], dict[str, tuple[int, frozenset[str]]]]
NEW_VARIANT = "missing from the newest hardware variant"


SCARPENTER_NAMESPACE = "scarpenter-system"  # keep in sync with deploy/controller/rbac.yaml
OFFERED_MAX_AGE = timedelta(minutes=30)  # an older offered-types ConfigMap is stale: SCarpenter stopped writing it


def _until(offerings: list) -> datetime | None:
    """The latest `until` (RFC3339, with timezone) among the unavailable offerings. An absent, damaged or
    timezone-less one is ignored (an older SCarpenter writes none)."""
    untils = []
    for offering in offerings:
        try:
            until = datetime.fromisoformat(offering["until"])
        except KeyError, TypeError, ValueError:
            continue
        if until.tzinfo and not offering.get("available"):
            untils.append(until)
    return max(untils, default=None)


def read_offered(
    kube: Kube, cfg: Config, vendor: str, now: datetime | None = None
) -> tuple[dict[str, dict], bool] | None:
    """The usable content of SCarpenter's offered-types ConfigMap `scarpenter-offered-<benchClass>-<vendor>` in
    `SCARPENTER_NAMESPACE` (gzipped JSON in `binaryData["types.json.gz"]`, else plain `data["types.json"]`; the
    optional `zonesOmitted` field is ignored, nothing reads zones): `({apiReference: entry}, truncated)`, where
    an entry is `{available, capacityTypes, reason, until}` and `until` is the latest of its unavailable offerings
    (`offerings[].until`, else None). Entries without an `apiReference` are ignored; of two with the same one the
    available wins and `until` is the latest of both. `now` (default the current time) judges staleness.

    None (fail open, the reason is logged) when the ConfigMap is absent, unreadable (a corrupt gzip too), malformed or
    stale (`generatedAt` missing, without timezone or older than `OFFERED_MAX_AGE`), or has a malformed entry."""
    name = f"scarpenter-offered-{cfg.settings.bench_class}-{vendor}"
    try:
        configmap = kube.get_configmap_in(SCARPENTER_NAMESPACE, name)
        if gz := (configmap.get("binaryData") or {}).get("types.json.gz"):  # base64 in the API object
            data = json.loads(gzip.decompress(base64.b64decode(gz)))
        else:  # an older SCarpenter writes plain JSON
            data = json.loads(configmap["data"]["types.json"])
        generated = datetime.fromisoformat(data["generatedAt"])
        if generated.tzinfo is None:
            raise ValueError(f"generatedAt {data['generatedAt']!r} has no timezone")
        truncated = data.get("truncated") is True  # absent: false
        entries = {}  # a malformed entry fails the whole read
        for entry in data["types"].values():
            api_reference = entry.get("apiReference")
            if not api_reference:
                continue
            available, capacity_types, offerings = (
                entry["available"],
                entry["capacityTypes"],
                entry.get("offerings", []),
            )
            if not (isinstance(available, bool) and isinstance(capacity_types, list) and isinstance(offerings, list)):
                raise ValueError(f"malformed entry for {api_reference!r}")
            old = entries.get(api_reference)
            untils = [u for u in (old and old["until"], _until(offerings)) if u]
            if old is None or (available and not old["available"]):
                entries[api_reference] = old = {
                    "available": available,
                    "capacityTypes": capacity_types,
                    "reason": entry.get("reason", "unavailable"),
                }
            old["until"] = max(untils, default=None)
    except NotFound:
        log.debug("offered types of vendor %s unusable: ConfigMap %s not found", vendor, name)
        return None
    except Exception as e:  # unreadable, bad JSON or a missing key: one reason, fail open
        log.warning("offered types of vendor %s unusable: cannot use ConfigMap %s: %r", vendor, name, e)
        return None
    if (now or datetime.now(UTC)) - generated > OFFERED_MAX_AGE:
        log.warning(
            "offered types of vendor %s unusable: ConfigMap %s is stale (generated %s)", vendor, name, generated
        )
        return None
    return entries, truncated


def offered_until(kube: Kube, cfg: Config, vendor: str, sku: str, now: datetime | None = None) -> datetime | None:
    """When SCarpenter's marks on the SKU's unavailable offerings expire (`read_offered`), else None."""
    offered = read_offered(kube, cfg, vendor, now)
    return offered[0].get(sku, {}).get("until") if offered else None


def is_offered(
    kube: Kube, cfg: Config, vendor: str, sku: str, on_demand_only: bool, now: datetime | None = None
) -> bool:
    """SCarpenter lists the SKU as available (with `on-demand` among its capacity types when `on_demand_only`).
    False when the listing cannot be read or the SKU is not in it."""
    offered = read_offered(kube, cfg, vendor, now)
    entry = offered[0].get(sku) if offered else None
    return bool(
        entry and entry["available"] and (not on_demand_only or CapacityType.ON_DEMAND in entry["capacityTypes"])
    )


def not_offered(kube: Kube, cfg: Config, vendor: str, skus: list[str]) -> dict[str, str]:
    """The SKUs (of `skus`) that SCarpenter's offered-types ConfigMap (`read_offered`) does not offer for the
    vendor's capacity mode, with the reason. A SKU is not offered when its entry is missing, `available` is false
    (whatever its offerings' `until`: SCarpenter rewrites the listing when a mark expires) or it has no capacity
    type, or when the mode is OnDemand and `on-demand` is not in its `capacityTypes` (SpotFirst takes either
    type).

    Fails open: when `read_offered` returns nothing, nothing is filtered. In a truncated list a missing entry
    proves nothing, so only entries with `available` false count."""
    offered = read_offered(kube, cfg, vendor)
    if offered is None:
        return {}
    entries, truncated = offered
    if truncated:
        log.warning(
            "offered types of vendor %s are truncated: autoPlan skips only SKUs it lists as unavailable", vendor
        )
    on_demand_only = cfg.settings.vendors[vendor].capacity == CapacityMode.ON_DEMAND
    skipped = {}
    for sku in skus:
        if sku not in entries:
            if not truncated:
                skipped[sku] = "not listed"
            continue
        entry = entries[sku]
        if not entry["available"]:
            skipped[sku] = entry["reason"]
        elif not entry["capacityTypes"]:
            skipped[sku] = "no capacity types"
        elif on_demand_only and CapacityType.ON_DEMAND not in entry["capacityTypes"]:
            skipped[sku] = f"no {CapacityType.ON_DEMAND} capacity"
    return skipped


def start_order(vendor: str, sku: str) -> tuple[int, str]:
    """Legacy `lib.instance_start_order_key`: crc32 of `vendor/sku` mixes the sizes (price order would exhaust the
    quota of a new vendor on the small SKUs first); the SKU name breaks ties."""
    return zlib.crc32(f"{vendor}/{sku}".encode()), sku


def plan_cycle(
    kube: Kube,
    cfg: Config,
    schedule_reader: ScheduleReader,
    *,
    dry_run: bool = False,
    keeper_cache: KeeperCache | None = None,
    variant_reader: VariantReader | None = None,
) -> list[PlannedRun]:
    """One autoPlan cycle. For each enabled vendor with autoPlan whose results are published (`publish`; without it
    git would never record them and the SKU would be run again and again) and that is below its caps
    (`check_vendor_budget`): take the vendor's ACTIVE SKUs from Keeper in `start_order`, skip those that already
    have a BenchmarkRun (the lease) and those whose names are no label values (the anchor could not select them),
    and create a run (not forced) for the first SKU with due tasks (`due_reason` on the git schedule metas) that
    Keeper does not block, until the caps are reached: usually one run per vendor and cycle. GPU tasks are dropped
    for a SKU whose `nvidia_smi` Keeper blocks (no GPU); a SKU with only GPU tasks due is not planned. A vendor
    whose publishing is stalled (`publisher.publishing_stalled`) is skipped: new runs would only pile up
    unpublished. A vendor that is `launch.paused` (BOOT_FAILURE on several SKUs, no node ready since) is skipped
    too. SKUs SCarpenter does not offer (`not_offered`) and SKUs waiting for a launch retry (`launch.deferred`,
    which also drops the entries of SKUs Keeper no longer lists, except in a dry run) are skipped.
    When a run's node lands on new hardware, `pick_variant` creates a new variant directory that only gets the
    tasks that ran. So a task Keeper allows that the SKU's newest variant has no `meta.json` for (`variant_reader`)
    is due too, once per variant: the launch ConfigMap records `{sku: N}` when such a run is created
    (`launch.record_variant`), so a failed rerun, or a node landing on another variant, starts no second one; the
    normal due rules take over. The planner cannot choose the hardware.
    With `dry_run` nothing is created and the first such SKU of each vendor is returned.

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
        runs = crd.list_runs(kube)
        try:
            check_vendor_budget(kube, cfg, vendor, runs)
        except ValueError as e:
            log.info("autoPlan skips vendor %s: %s", vendor, e)
            continue
        if publisher.publishing_stalled(settings, vendor, runs):
            log.warning("autoPlan skips vendor %s: finished tasks stay unpublished, publishing is failing", vendor)
            continue
        if launch.paused(kube, vendor, datetime.now(UTC)):
            log.warning(
                "autoPlan skips vendor %s: nodes of %d SKUs did not come up within %s and none has since",
                vendor, launch.BREAKER_LIMIT, launch.BREAKER_WINDOW,
            )  # fmt: skip
            continue
        candidates.append(vendor)
    if not candidates:
        return []

    server_tasks = [task for task in tasks.all_tasks() if not task.dbaas]
    try:
        blocked = (keeper_cache or KeeperCache()).get(cfg.env, [task.name for task in server_tasks])
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
        try:
            newest = variant_reader(vendor) if variant_reader else {}
            started = launch.read_variants(kube, vendor) if variant_reader else {}
        except Exception:
            log.exception("autoPlan: no check for new hardware variants of vendor %s this cycle", vendor)
            newest, started = {}, {}
        runs = crd.list_runs(kube)
        leased = {run.name for run in runs}
        skus = sorted(blocked.get(vendor, {}), key=lambda sku: start_order(vendor, sku))
        if not skus:
            log.info("autoPlan: Keeper lists no ACTIVE servers for vendor %s", vendor)
        skipped = not_offered(kube, cfg, vendor, skus) if skus else {}
        waiting = launch.deferred(kube, vendor, skus, datetime.now(UTC), prune=not dry_run)
        if skipped:
            examples = ", ".join(f"{sku} ({why})" for sku, why in list(skipped.items())[:3])
            log.info(
                "autoPlan skips %d SKUs of vendor %s not offered by SCarpenter, e.g. %s", len(skipped), vendor, examples
            )
        bad_names = [sku for sku in skus if not pods.is_label_value(sku)]
        if bad_names:
            log.warning(
                "autoPlan skips %d SKUs of vendor %s whose names are not label values, e.g. %s",
                len(bad_names), vendor, bad_names[0],
            )  # fmt: skip
        for sku in skus:
            if crd.run_name(vendor, sku) in leased or not publisher.publish_allowed(settings, vendor, sku):
                continue
            if sku in bad_names or sku in skipped or sku in waiting:
                continue
            if None in metas.get(sku, {}).values():  # unreadable meta.json: the publisher logged which
                continue
            no_gpu = GPU_PROBE_TASK in blocked[vendor][sku]
            variant, present = newest.get(sku, (None, frozenset()))
            check_variant = variant is not None and started.get(sku, -1) < variant
            new_variant = False
            due = {}
            for task in server_tasks:
                if task.name in blocked[vendor][sku] or (task.gpu and no_gpu):
                    continue
                try:
                    reason = due_reason(task, metas.get(sku, {}).get(task.name), now)
                except AttributeError, TypeError, ValueError:  # a damaged meta.json field: unreadable, skip the SKU
                    log.warning("autoPlan skips %s/%s: damaged schedule/%s/meta.json", vendor, sku, task.name)
                    due = None
                    break
                if not reason and check_variant and task.name not in present:
                    reason, new_variant = NEW_VARIANT, True
                if reason:
                    due[task.name] = reason
            if due is None:
                continue
            if all(tasks.get_task(name).gpu for name in due):  # nothing due, or only GPU tasks
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
                if new_variant:
                    launch.record_variant(kube, vendor, sku, variant)
            reason = ", ".join(f"{name}: {why}" for name, why in due.items())
            planned.append(PlannedRun(vendor, sku, tuple(due), reason))
            if dry_run:
                break
    return planned


def run_forever(
    kube: Kube,
    cfg_provider,
    schedule_reader: ScheduleReader,
    stop: threading.Event,
    variant_reader: VariantReader | None = None,
) -> None:
    """Every `planner.interval_minutes`: `plan_cycle`; exceptions are logged, never fatal. `cfg_provider` is a
    zero-argument callable returning the current `Config`."""
    keeper_cache = KeeperCache()
    while not stop.is_set():
        interval_minutes = 5
        try:
            cfg = cfg_provider()
            interval_minutes = cfg.settings.planner_interval_minutes
            for planned in plan_cycle(
                kube, cfg, schedule_reader, keeper_cache=keeper_cache, variant_reader=variant_reader
            ):
                log.info("planned run", extra={"vendor": planned.vendor, "sku": planned.sku, "reason": planned.reason})
        except Exception:
            log.exception("planning cycle failed")
        stop.wait(interval_minutes * 60)
