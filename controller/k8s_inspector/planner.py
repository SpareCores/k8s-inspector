"""Planner: which SKUs get a BenchmarkRun, forced (`force_run`) or by itself (`plan_cycle`, autoPlan).

Owner: planner agent. Implements docs/how-to/run-forced.md, docs/reference/configuration.md (planner,
vendors), state.md "What git still records", exit-codes.md "Planner retry" and the planning rows of
feature-mapping.md. The lease is the BenchmarkRun name: creating a second run for a SKU raises `RunExists`.
"""

import base64
import gzip
import json
import logging
import re
import threading
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from k8s_inspector import crd, keeper, launch, pods, popularity, publisher, render, spot, tasks
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
    regions: tuple[str, ...] = ()  # the regions the run is confined to (`RunSpec.regions`), empty: any
    score: str = ""  # the popularity components that ranked the SKU (`popularity.rank`), for logs


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
    """Server runs of `vendor` that hold or are acquiring a node: in an active phase, or (Failed, Cancelled) with an
    anchor Pod still alive. DBaaS runs are not counted: they have their own limit, `dbaasMaxInFlight`
    (`dbaas_plan.check_in_flight`)."""
    anchored = {
        _labels(pod).get(crd.LABEL_RUN) for pod in kube.list("pod", labels={pods.LABEL_ROLE: PodRole.ANCHOR.value})
    }
    return [
        r
        for r in runs
        if r.spec.vendor == vendor
        and r.spec.database is None
        and (r.status.phase in ACTIVE_PHASES or r.name in anchored)
    ]


def _labels(pod: dict) -> dict:
    return pod.get("metadata", {}).get("labels") or {}


def live_nodeclaims(kube: Kube, cfg: Config, vendor: str) -> int:
    """Live NodeClaims of `vendor` in the bench NodePool. Only the pool label is there from creation: the vendor
    comes from the label after launch, before it from `spec.requirements` (`pods.claim_vendor`). An instance can
    outlive its anchor (provision deadline passed while Karpenter still launches; drain after release)."""
    claims = kube.list("nodeclaim", labels={pods.NODEPOOL_LABEL: cfg.settings.bench_pool})
    return sum(1 for claim in claims if pods.claim_vendor(claim) == vendor)


def check_vendor_budget(
    kube: Kube, cfg: Config, vendor: str, runs: list[BenchmarkRun], estimates: dict[str, float] | None = None
) -> float | None:
    """Raise `ValueError` when one more run of `vendor` would pass `maxInFlight` (0 pauses the vendor; unset: no
    limit), or when `maxUsdPerHour` is set and the holding runs already cost the cap, or one of them has no price (a
    run still in Pending or Provisioning: wait; a run with a node: unknown spend, refused, with a warning). In
    flight = max(`holding_runs`, live bench NodeClaims), a lower bound ("at least"): leftover NodeClaims without a
    holding run add to the count but not to the spend. `estimates` ({run name: USD/hour}) are the planner's
    estimates for runs it created in this cycle: such a run counts with its estimate instead of being waited for.
    Returns what is left under `maxUsdPerHour` (None without a cap). Used by `force_run` (without `force`) and
    autoPlan. Not atomic: two concurrent callers can both pass the check."""
    settings = cfg.settings
    vendor_settings = settings.vendors[vendor]
    limit = vendor_settings.max_in_flight
    if limit is None:
        limit = settings.defaults.max_in_flight
    active = holding_runs(kube, runs, vendor)
    client_nodes = sum(
        1
        for r in runs
        if r.spec.vendor == vendor and r.spec.database and r.status.node and r.status.phase in ACTIVE_PHASES
    )
    in_flight = max(
        len(active), live_nodeclaims(kube, cfg, vendor) - client_nodes
    )  # a DBaaS client is not a server run
    if in_flight >= limit:
        raise ValueError(f"vendor {vendor} is at its maxInFlight limit ({in_flight} of {limit} instances in flight)")
    cap = vendor_settings.max_usd_per_hour
    if cap is None:
        return None
    estimates = estimates or {}
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
    if waiting := [name for name in no_node_yet if name not in estimates]:
        raise ValueError(
            f"vendor {vendor} has maxUsdPerHour {cap}: wait until {', '.join(waiting)} has a price "
            "before starting a run"
        )
    spend = spend_per_hour(active) + sum(estimates[name] for name in no_node_yet)
    if spend >= cap:
        raise ValueError(f"vendor {vendor} is at its maxUsdPerHour cap ({cap} USD/hour)")
    return cap - spend


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
    and are refused). Without `force` the vendor must be below `maxInFlight` (0 pauses it) and `maxUsdPerHour`, and
    Keeper must find every task eligible (`keeper.block_reasons`; an unknown SKU is blocked, an unreachable Keeper
    fails closed). With `force` neither is checked (the run still counts toward both limits for autoPlan) and the
    SKU is not checked here: a SKU SCarpenter cannot offer ends in `-1 SCARPENTER_NO_OFFERING`."""
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
    regions: tuple[str, ...] = (),
) -> BenchmarkRun:
    spec = RunSpec(
        vendor=vendor,
        sku=sku,
        run_id=crd.new_run_id(datetime.now(UTC)),
        tasks=names,
        force=force,
        capacity=capacity or cfg.settings.vendors[vendor].capacity,
        hold_after=hold_after,
        regions=list(regions),
    )
    run = crd.create_run(kube, spec)
    log.info(
        "created run",
        extra={"run": run.name, "vendor": vendor, "sku": sku, "tasks": ",".join(names), "regions": ",".join(regions)},
    )
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
OFFERED_UNUSABLE = "offered_types_unusable"  # `reason` of `on_vendor_skipped`


class KeeperCache:
    """The bulk Keeper answer (`keeper.active_servers_block_reasons`), kept for `KEEPER_CACHE_SECONDS`: the ACTIVE
    server list and block reasons change rarely, a cycle runs every few minutes. `plan_cycle` without a cache
    object asks Keeper every time."""

    def __init__(self) -> None:
        self._asked = 0.0
        self._answer: dict | None = None
        self._cost_data: dict[tuple[str, str], tuple[float, tuple]] = {}

    def get(self, env, task_names: list[str]) -> dict:
        if self._answer is None or time.monotonic() - self._asked >= KEEPER_CACHE_SECONDS:
            self._answer = keeper.active_servers_block_reasons(env, task_names)
            self._asked = time.monotonic()
        return self._answer

    def server_cost_data(self, env, vendor: str, sku: str) -> tuple[dict | None, dict | None]:
        """`(server facts, on-demand prices by region)` of one SKU (`keeper.server_facts`, `keeper.ondemand_prices`),
        kept like the bulk answer. Raises `RuntimeError` when Keeper cannot be reached (nothing is kept then)."""
        asked, data = self._cost_data.get((vendor, sku), (0.0, ()))
        if not data or time.monotonic() - asked >= KEEPER_CACHE_SECONDS:
            data = (keeper.server_facts(env, vendor, sku), keeper.ondemand_prices(env, vendor, sku))
            self._cost_data[(vendor, sku)] = (time.monotonic(), data)
        return data


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
    if now - _naive(end) >= wait:
        return f"retry after exit code {code}"
    return None


# The newest hardware variant of each SKU (`Publisher.read_newest_variants`): {sku: (N, names of the tasks with a
# `data/<vendor>/<sku>/<N>/<task>/meta.json`)}
VariantReader = Callable[[str], dict[str, tuple[int, frozenset[str]]]]
# When hardware variant N of a SKU first appeared (`Publisher.read_first_seen(vendor, sku, N)`), None when unknown.
FirstSeenReader = Callable[[str, str, int], datetime | None]
NEW_VARIANT = "missing from the newest hardware variant"


def _naive(timestamp: str) -> datetime:
    """A meta time as naive UTC, parsed like in `due_reason`."""
    return datetime.fromisoformat(timestamp).replace(tzinfo=None)


def fill_reason(task: TaskDef, meta: dict, first_seen: datetime, now: datetime) -> str | None:
    """Why `task` is due for the newest variant, which first appeared at `first_seen` and has no result of it, on a
    SKU whose `schedule/<task>/meta.json` is `meta`. The latest attempt (`render.latest_attempt`) decides: one from
    before `first_seen` says nothing about the new hardware, so the task is filled; a later one is final except
    for the retries of `due_reason` (`-4`, a start failure, with their `-5` cap). An attempt without `start` is
    not filled."""
    attempt = render.latest_attempt(meta)
    if not attempt.get("start"):
        return None
    if _naive(attempt["start"]) < first_seen:
        return NEW_VARIANT
    return due_reason(task, attempt, now)


def _read_first_seen(reader: FirstSeenReader | None, vendor: str, sku: str, variant: int) -> datetime | None:
    try:
        return reader(vendor, sku, variant) if reader else None
    except Exception:
        log.exception("autoPlan: cannot read variant.json of %s/%s/%d, no variant fill", vendor, sku, variant)
        return None


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


def _regions(quota: dict) -> dict[str, dict]:
    """`{region: {limit, headroom}}` of a quota entry's `regions`; empty when absent (a global bucket, or unknown)
    or damaged (the entry then counts as without regions: its `maxHeadroom` decides)."""
    try:
        return {
            str(r["region"]): {"limit": float(r["limit"]), "headroom": float(r["headroom"])}
            for r in quota.get("regions") or []
        }
    except KeyError, TypeError, ValueError:
        return {}


def _zones(offerings: list) -> dict[str, list[str]]:
    """The zone names of each capacity type: the union over its available offering groups (a type has several
    groups, the unavailable ones among them), else over all its groups. A group without a list of zones is left
    out."""
    groups: dict[str, list[dict]] = {}
    for offering in offerings:
        if isinstance(offering, dict) and isinstance(offering.get("zones"), list):
            groups.setdefault(offering.get("capacityType"), []).append(offering)
    result = {}
    for capacity_type, found in groups.items():
        used = [o for o in found if o.get("available")] or found
        result[capacity_type] = sorted({str(zone) for o in used for zone in o["zones"]})
    return result


def _in_region(zone: str, region: str) -> bool:
    """The zone belongs to the region: it is the region (Alicloud, UpCloud), or the region plus a letter (AWS
    `us-east-1a`) or a dash and a letter (GCP `europe-west1-b`). `europe-west10-a` and `us-east-1-bos-1a` are not."""
    return re.fullmatch(f"{re.escape(region)}(-?[a-z])?", zone) is not None


def _offered_types(offerings: list) -> set[str]:
    """The capacity types of the entry's offering groups, available or not, except a group SCarpenter marks
    `NotLaunchable` (it can never launch). Empty when the entry lists no group (an older SCarpenter)."""
    return {o.get("capacityType") for o in offerings if isinstance(o, dict) and o.get("reason") != "NotLaunchable"}


def _quotas(offerings: list) -> dict[str, list[dict]]:
    """The quota entries of each capacity type, `{capacityType: [{bucket, unit, need, maxLimit, maxHeadroom,
    regions}]}` (`regions` from `_regions`): merged by bucket over its available offering groups (a type has several
    groups, the unavailable ones among them, each listing only the regions of its own zones), else over all its
    groups, like `_zones`. `regions` is the union of the groups' regions (the values are account-level facts; on a
    conflict the smaller limit and headroom win). `maxLimit` and `maxHeadroom` are the maxima over each group's own
    regions, so the maxima of the groups are the maxima over the union; `need` should agree, else the largest
    wins. A group without `quotas` or with a damaged one is left out
    (no quota model: the planner behaves as without this field)."""
    groups: dict[str, list[tuple[bool, list[dict]]]] = {}
    for offering in offerings:
        try:
            entries = [
                {
                    "bucket": str(q["bucket"]),
                    "unit": str(q.get("unit", "units")),
                    "need": float(q["need"]),
                    "maxLimit": float(q["maxLimit"]),
                    "maxHeadroom": float(q["maxHeadroom"]),
                    "regions": _regions(q),
                }
                for q in offering["quotas"]
            ]
        except KeyError, TypeError, ValueError:
            continue
        if entries:
            groups.setdefault(offering.get("capacityType"), []).append((bool(offering.get("available")), entries))
    result = {}
    for capacity_type, found in groups.items():
        used = [entries for available, entries in found if available] or [entries for _, entries in found]
        merged: dict[str, dict] = {}
        for entries in used:
            for entry in entries:
                _merge_quota(merged.setdefault(entry["bucket"], {**entry, "regions": {}}), entry)
        result[capacity_type] = list(merged.values())
    return result


def _merge_quota(into: dict, entry: dict) -> None:
    """Fold one group's quota entry of the same bucket into `into` (see `_quotas`)."""
    into["need"] = max(into["need"], entry["need"])
    into["maxLimit"] = max(into["maxLimit"], entry["maxLimit"])
    into["maxHeadroom"] = max(into["maxHeadroom"], entry["maxHeadroom"])
    for region, values in entry["regions"].items():
        known = into["regions"].get(region)
        into["regions"][region] = (
            values if known is None else {k: min(known[k], values[k]) for k in ("limit", "headroom")}
        )


class Offered(NamedTuple):
    entries: dict[str, dict]  # {apiReference: entry}
    truncated: bool
    generated_at: datetime  # when SCarpenter wrote the listing (`generatedAt`)


def read_offered(kube: Kube, cfg: Config, vendor: str, now: datetime | None = None) -> Offered | None:
    """The usable content of SCarpenter's offered-types ConfigMap `scarpenter-offered-<benchClass>-<vendor>` in
    `SCARPENTER_NAMESPACE` (gzipped JSON in `binaryData["types.json.gz"]`, else plain `data["types.json"]`; the
    optional `zonesOmitted` field is ignored, nothing reads zones): `Offered(entries, truncated, generated_at)`.
    `entries` is `{apiReference: entry}`; an entry is `{available, capacityTypes, reason, until, quotas}`.
    `until` is the latest of its unavailable offerings (`offerings[].until`, else None) and `quotas` the quota
    entries of its offering groups (`_quotas`), `offered_types` the capacity types of its offering groups
    (`_offered_types`). Entries without an `apiReference` are ignored; of two with the
    same one the available wins and `until` is the latest of both. `now` (default the current time) judges
    staleness.

    None (the reason is logged; it means neither "offered" nor "not offered") when the ConfigMap is absent,
    unreadable (a corrupt gzip too), malformed or stale (`generatedAt` missing, without timezone or older than
    `OFFERED_MAX_AGE`), or has a malformed entry."""
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
                    "quotas": _quotas(offerings),
                    "zones": _zones(offerings),
                    "offered_types": _offered_types(offerings),
                }
            old["until"] = max(untils, default=None)
    except NotFound:
        log.warning("offered types of vendor %s unusable: ConfigMap %s not found", vendor, name)
        return None
    except Exception as e:  # unreadable, bad JSON or a missing key: one reason
        log.warning("offered types of vendor %s unusable: cannot use ConfigMap %s: %r", vendor, name, e)
        return None
    if (now or datetime.now(UTC)) - generated > OFFERED_MAX_AGE:
        log.warning(
            "offered types of vendor %s unusable: ConfigMap %s is stale (generated %s)", vendor, name, generated
        )
        return None
    return Offered(entries, truncated, generated)


def offered_until(kube: Kube, cfg: Config, vendor: str, sku: str, now: datetime | None = None) -> datetime | None:
    """When SCarpenter's marks on the SKU's unavailable offerings expire (`read_offered`), else None."""
    offered = read_offered(kube, cfg, vendor, now)
    return offered[0].get(sku, {}).get("until") if offered else None


def is_offered(
    kube: Kube,
    cfg: Config,
    vendor: str,
    sku: str,
    on_demand_only: bool,
    now: datetime | None = None,
    spot_only: bool = False,
) -> bool:
    """SCarpenter lists the SKU as available (with `on-demand` among its capacity types when `on_demand_only`, with
    `spot` when `spot_only`). False when the listing cannot be read or the SKU is not in it."""
    offered = read_offered(kube, cfg, vendor, now)
    entry = offered[0].get(sku) if offered else None
    return bool(
        entry
        and entry["available"]
        and (not on_demand_only or CapacityType.ON_DEMAND in entry["capacityTypes"])
        and (not spot_only or CapacityType.SPOT in entry["capacityTypes"])
    )


class _Unread:
    """Type of `UNREAD`: `not_offered` was not given the listing."""


UNREAD = _Unread()


def not_offered(
    kube: Kube,
    cfg: Config,
    vendor: str,
    skus: list[str],
    now: datetime | None = None,
    offered: Offered | None | _Unread = UNREAD,
) -> dict[str, str] | None:
    """The SKUs (of `skus`) that SCarpenter's offered-types ConfigMap (`read_offered`) does not offer for the
    vendor's capacity mode, with the reason. A SKU is not offered when its entry is missing, `available` is false
    (whatever its offerings' `until`: SCarpenter rewrites the listing when a mark expires) or it has no capacity
    type, or when the mode is OnDemand and `on-demand` is not in its `capacityTypes` (SpotFirst takes either
    type), or SpotOnly and `spot` is not.

    Fails closed: None when `read_offered` returns nothing (the listing is missing, unreadable or stale), because
    then nothing is known about the SKUs. In a truncated list a missing entry proves nothing, so only entries with
    `available` false count. `offered` is the result of a `read_offered` the caller made already (None included:
    the listing is unusable); by default the listing is read here."""
    if offered is UNREAD:
        offered = read_offered(kube, cfg, vendor, now)
    if offered is None:
        return None
    entries, truncated, _ = offered
    if truncated:
        log.warning(
            "offered types of vendor %s are truncated: autoPlan skips only SKUs it lists as unavailable", vendor
        )
    mode = cfg.settings.vendors[vendor].capacity
    on_demand_only = mode == CapacityMode.ON_DEMAND
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
        elif mode == CapacityMode.SPOT_ONLY and CapacityType.SPOT not in entry["capacityTypes"]:
            skipped[sku] = f"no {CapacityType.SPOT} capacity"
    return skipped


# --------------------------------------------------------------------------- quota-aware planning

SPOT_BUSY = "spot quota busy"
ON_DEMAND_BUSY = "on-demand quota busy"
IN_FLIGHT = (RunPhase.PENDING, RunPhase.PROVISIONING, RunPhase.RUNNING)  # phases of a run that holds or takes quota

# One quota bucket in one region (None: the bucket as a whole, for a global bucket or a SKU without region facts).
QuotaKey = tuple[str, str | None]

# The big SKUs of each vendor deferred for a busy spot quota, kept across cycles by the controller (a restart resets
# them): (vendor, sku) -> (deferred since, the quota keys they ask to drain). Rebuilt every cycle.
QuotaWaits = dict[tuple[str, str], tuple[datetime, frozenset[QuotaKey]]]


def _never_fits(quotas: list[dict]) -> bool:
    """The limit of an entry is below what one instance needs: waiting does not help."""
    return any(q["maxLimit"] < q["need"] for q in quotas)


def spot_capable(kube: Kube, cfg: Config, vendor: str, sku: str, task_names: list[str], now: datetime) -> bool:
    """A new run of these tasks may use spot (`spot.capacity_for_next_attempt` without attempts): the vendor is not
    `OnDemand`, the tasks allow spot (`tasks.allows_spot`) and the SKU is not quarantined (`spot.is_quarantined`)."""
    settings = cfg.settings
    return (
        settings.vendors[vendor].capacity != CapacityMode.ON_DEMAND
        and tasks.allows_spot([tasks.get_task(name) for name in task_names], settings.spot.spot_max_task_minutes)
        and not spot.is_quarantined(kube, vendor, sku, settings, now)
    )


def _group(entry: dict, spot_ok: bool) -> list[dict]:
    """The quota entries one run of the SKU consumes. A run that may use spot (`spot_ok`) uses the spot group; when
    that never fits, or the SKU has no spot group, the on-demand group is used, except that an available spot
    offering without quotas consumes nothing known. A group that never fits is empty: waiting does not help, the
    planner behaves as without quotas."""
    groups = entry["quotas"]
    if spot_ok:
        spot_group = groups.get(CapacityType.SPOT)
        if spot_group and not _never_fits(spot_group):
            return spot_group
        if not spot_group and CapacityType.SPOT in entry["capacityTypes"]:
            return []
    on_demand = groups.get(CapacityType.ON_DEMAND, [])
    return [] if _never_fits(on_demand) else on_demand


@dataclass
class Decision:
    """What `QuotaPass.decide` says about one SKU."""

    quotas: list[dict] = field(default_factory=list)  # the quota entries the run consumes
    regions: tuple[str, ...] = ()  # the regions the run may land in (its pin), empty: any
    region: str | None = None  # where it is expected to land, for the quota accounting; None: unknown
    price: float | None = None  # estimated USD/hour of its node (Keeper), None: unknown
    deferred: str | None = None  # why the SKU waits for a later cycle
    reserved: bool = False  # deferred because another SKU's reservation holds the quota
    wait_keys: frozenset[QuotaKey] | None = None  # deferred for busy spot quota: the buckets to drain (a big SKU's)


def _key(quota: dict, region: str | None) -> QuotaKey:
    """The consumption key of a quota entry: per region when SCarpenter lists the region for it, else per bucket."""
    return quota["bucket"], region if region in quota["regions"] else None


def _landing(run: BenchmarkRun) -> str | None:
    """The region a run holds quota in: its node's, else the only region it may land in, else unknown."""
    return run.status.region or (run.spec.regions[0] if len(run.spec.regions) == 1 else None)


def _limit(quota: dict, region: str | None) -> float:
    return quota["regions"][region]["limit"] if region in quota["regions"] else quota["maxLimit"]


class QuotaPass:
    """The quota view of one vendor in one cycle. `entries` are the SKUs of the listing with `quotas`; a missing or
    truncated listing has none, so nothing here applies (a truncated one proves nothing about quota). A quota entry
    with `regions` is per region; one without (a global bucket, or unknown regions) is judged by `maxHeadroom`
    against everything consumed in the bucket. Per bucket and region (`QuotaKey`):

    - `consumed`: quota of the vendor's own runs that the listing's headroom does not show, plus the runs
      planned in this cycle. SCarpenter's headroom is `limit - used - reserved` as of the listing's `generatedAt`,
      and `reserved` starts with a launch, so it misses a run created after `generatedAt` and an anchor without a
      NodeClaim (or node) yet. A run with a NodeClaim that is older than `generatedAt` is in `reserved` or `used`
      already and is not counted again (a NodeClaim created just before `generatedAt` may not be in `reserved` yet).
      A run whose region is not known (its node has none yet and it is confined to several regions, or to none)
      counts under region None, against every region.
    - `held`: the keys that the vendor's own runs in flight consume. A reservation only matters for these: when
      others hold the quota, keeping SKUs back frees nothing.
    - `reserved`: the held keys that a SKU deferred longer than `limit` (the anti-starvation rule, `waits`) keeps for
      itself: no other run is placed there until the runs in flight drain. The longest waiter wins a key."""

    def __init__(
        self,
        kube: Kube,
        cfg: Config,
        vendor: str,
        runs: list[BenchmarkRun],
        offered: Offered | None,
        now,
        waits: dict[str, tuple[datetime, frozenset[QuotaKey]]] | None = None,
    ):
        self.kube, self.cfg, self.vendor, self.now = kube, cfg, vendor, now
        self.entries = {}
        self.consumed: dict[QuotaKey, float] = {}
        self.held: set[QuotaKey] = set()
        self.waits = waits or {}
        self.limit = timedelta(minutes=cfg.settings.spot.quota_reserve_after_minutes)
        self.reserved: dict[QuotaKey, tuple[datetime, str]] = {}  # key -> (waiting since, the SKU that reserves it)
        if offered is None or offered.truncated:
            return
        self.entries = {sku: entry for sku, entry in offered.entries.items() if entry["quotas"]}
        if not self.entries:
            return
        claimed = {
            pods.claim_run(c) for c in kube.list("nodeclaim", labels={pods.NODEPOOL_LABEL: cfg.settings.bench_pool})
        }
        for run in runs:
            if run.spec.vendor != vendor or run.status.phase not in IN_FLIGHT:
                continue
            quotas = self._run_quotas(run)
            unseen = (run.name not in claimed and not run.status.node) or run.created > offered.generated_at
            if unseen:
                self.consume(quotas, _landing(run))
            else:
                self.held.update(_key(q, _landing(run)) for q in quotas)
        if self.limit:
            for holder, (since, keys) in sorted(self.waits.items(), key=lambda item: (item[1][0], item[0])):
                if now - since > self.limit:
                    for key in keys & self.held:
                        self.reserved.setdefault(key, (since, holder))  # the longest waiter comes first

    def _run_quotas(self, run: BenchmarkRun) -> list[dict]:
        """The quota entries the run consumes with the capacity it will really use (`spot.capacity_for_next_attempt`:
        its spec and attempts decide, not the vendor setting), by the same rule as a new run (`_group`). A run that
        may use spot counts with its spot group only: the fall-back to on-demand when spot fills before the launch is
        not planned for (a residual race). A DBaaS client run counts, with its on-demand group, once its client SKU is
        known."""
        entry = self.entries.get(run.node_sku)
        if entry is None:
            return []
        spot_ok = False
        if run.spec.database is None:
            settings = self.cfg.settings
            try:
                selected = tasks.select_tasks(run.spec.tasks)
            except ValueError:  # a task of the run left the catalogue
                return []
            quarantined = spot.is_quarantined(self.kube, self.vendor, run.spec.sku, settings, self.now)
            spot_ok = spot.capacity_for_next_attempt(run, selected, settings, quarantined)[0] == CapacityType.SPOT
        return _group(entry, spot_ok)

    def consume(self, quotas: list[dict], region: str | None = None) -> None:
        for q in quotas:
            key = _key(q, region)
            self.consumed[key] = self.consumed.get(key, 0) + q["need"]
            self.held.add(key)

    def _room(self, quota: dict, region: str | None) -> float:
        """What is left of the entry for one more run in `region`: its regional headroom minus what is consumed
        there (and by runs without a region), else `maxHeadroom` minus everything consumed in the bucket."""
        bucket = quota["bucket"]
        if region in quota["regions"]:
            used = self.consumed.get((bucket, region), 0) + self.consumed.get((bucket, None), 0)
            return quota["regions"][region]["headroom"] - used
        return quota["maxHeadroom"] - sum(v for (b, _), v in self.consumed.items() if b == bucket)

    def _busy(self, quotas: list[dict], region: str | None) -> str | None:
        """Why one more instance in `region` does not fit the entries, else None."""
        for q in quotas:
            room = self._room(q, region)
            if q["need"] > room:
                where = f" in {region}" if region in q["regions"] else ""
                return f"bucket {q['bucket']}: need {q['need']:g} {q['unit']}, headroom {room:g}{where}"
        return None

    def _holder(self, sku: str, quotas: list[dict], region: str | None) -> str | None:
        """The SKU whose reservation keeps `sku` out of `region`, else None. Only a SKU that has waited longer than
        `sku` itself reserves against it, so two holders cannot block each other."""
        mine = self.waits[sku][0] if sku in self.waits else self.now
        for q in quotas:
            since, holder = self.reserved.get(_key(q, region), (mine, sku))
            if holder != sku and since < mine:
                return holder
        return None

    def _wait_keys(self, sku: str, quotas: list[dict], candidates: list[str | None]) -> frozenset[QuotaKey]:
        """The keys a SKU deferred for busy spot quota asks to drain, in ONE region: the one it chose before while
        that is still a candidate, else the one with the most room among those where the vendor's own runs hold the
        quota (only those drain), else the most room anywhere. Empty unless the SKU is big there (it needs more
        than half of an entry's limit): the others fit as soon as a few runs end."""
        old = {key[1] for key in self.waits[sku][1]} if sku in self.waits else set()
        held = [r for r in candidates if any(_key(q, r) in self.held for q in quotas)]  # only these can drain
        pool = [r for r in candidates if r in old] or held or candidates
        region = max(pool, key=lambda r: self._margin(quotas, r))
        if not any(q["need"] > _limit(q, region) / 2 for q in quotas):
            return frozenset()
        return frozenset(_key(q, region) for q in quotas)

    def _margin(self, quotas: list[dict], region: str | None) -> float:
        """How much room is left in `region` after one more run (the least of the entries)."""
        return min((self._room(q, region) - q["need"] for q in quotas), default=0)

    def decide(self, sku: str, task_names: list[str], market: dict | None = None) -> Decision:
        """Where and whether one new run of the SKU goes. The run consumes `_group` of the SKU. Without quota entries
        nothing is known and it is planned unconfined. Otherwise every region the entries list (all regional entries
        must list it, and the group's zones when they name regions) is a candidate, or just no region when no entry
        has `regions`. A region where the run does not fit, or that another SKU reserves, is out. The run is confined
        to all the others (Karpenter picks an available offering among them, so a region with an unavailable offering
        does not starve it) and counts against the cheapest of them by Keeper's price for the capacity type
        (`market`, `popularity.rank`'s data), else the one with the most room. With no candidate the SKU is deferred
        (busy spot quota: it would fall back to on-demand and cost several times as much). A group that never fits
        or has no quotas changes nothing."""
        entry = self.entries.get(sku)
        if entry is None:
            return Decision(price=_estimate(market, None, False))
        spot_ok = spot_capable(self.kube, self.cfg, self.vendor, sku, task_names, self.now)
        group = _group(entry, spot_ok)
        is_spot = bool(group) and group is entry["quotas"].get(CapacityType.SPOT)
        prices = (market or {}).get("spot" if is_spot else "ondemand", {})
        regional = [set(q["regions"]) for q in group if q["regions"]]
        candidates: list[str | None] = []
        if regional:
            zones = entry["zones"].get(CapacityType.SPOT if is_spot else CapacityType.ON_DEMAND, [])
            candidates = sorted(set.intersection(*regional))
            named = [r for r in candidates if any(_in_region(zone, r) for zone in zones)]
            candidates = named or candidates  # zones that carry region names narrow the set; others say nothing
        candidates = candidates or [None]
        busy = {region: self._busy(group, region) for region in candidates}
        fitting = [region for region in candidates if not busy[region]]
        if not fitting:
            why = busy[candidates[0]] + (f" (and {len(candidates) - 1} more regions)" if len(candidates) > 1 else "")
            keys = self._wait_keys(sku, group, candidates) if is_spot else None
            return Decision(deferred=f"{SPOT_BUSY if is_spot else ON_DEMAND_BUSY} ({why})", wait_keys=keys)
        free = [region for region in fitting if not self._holder(sku, group, region)]
        if not free:
            holder = self._holder(sku, group, fitting[0])
            return Decision(deferred=f"its quota is reserved for {holder}, deferred over {self.limit}", reserved=True)
        priced = [r for r in free if prices.get(r) is not None]
        region = (
            min(priced, key=lambda r: (prices[r], r))
            if priced
            else max(free, key=lambda r: (self._margin(group, r), r or ""))
        )
        regions = tuple(r for r in free if r is not None)
        return Decision(group, regions, region, _estimate(market, region, is_spot))


def _estimate(market: dict | None, region: str | None, spot: bool) -> float | None:
    """USD/hour to budget for a node in `region` (None: the cheapest region): the dearer of its spot and on-demand
    price for a run that may use spot, because spot can fall back to on-demand; None when Keeper has no price."""
    found = []
    for allocation in ("spot", "ondemand") if spot else ("ondemand",):
        by_region = (market or {}).get(allocation, {})
        found.append(by_region.get(region) if region else min(by_region.values(), default=None))
    return max((price for price in found if price is not None), default=None)


def start_order(vendor: str, sku: str) -> tuple[int, str]:
    """Legacy `lib.instance_start_order_key`: crc32 of `vendor/sku` mixes the sizes (price order would exhaust the
    quota of a new vendor on the small SKUs first); the SKU name breaks ties."""
    return zlib.crc32(f"{vendor}/{sku}".encode()), sku


def _due_tasks(
    vendor: str,
    sku: str,
    server_tasks: list[TaskDef],
    blocked: dict[str, list[str]],
    metas: dict[str, dict | None],
    newest: tuple[int | None, frozenset[str]],
    first_seen_reader: FirstSeenReader | None,
    now: datetime,
) -> dict[str, str] | None:
    """`{task name: why it is due}` of a SKU whose `schedule/<task>/meta.json` are `metas`, `blocked` the Keeper
    reasons by task and `newest` its newest variant and the tasks that variant has (`NEW_VARIANT`); empty when
    nothing is due, None when a meta is damaged (the SKU is skipped)."""
    no_gpu = GPU_PROBE_TASK in blocked
    variant, present = newest
    first_seen, first_seen_read = None, False  # read at most once, and only when a task needs it
    due = {}
    for task in server_tasks:
        if task.name in blocked or (task.gpu and no_gpu):
            continue
        meta = metas.get(task.name)
        try:
            reason = due_reason(task, meta, now)
            if not reason and variant is not None and task.name not in present:
                if not first_seen_read:
                    first_seen, first_seen_read = _read_first_seen(first_seen_reader, vendor, sku, variant), True
                if first_seen:
                    reason = fill_reason(task, meta, first_seen, now)
        except AttributeError, TypeError, ValueError:  # a damaged meta.json field: unreadable, skip the SKU
            log.warning("autoPlan skips %s/%s: damaged schedule/%s/meta.json", vendor, sku, task.name)
            return None
        if reason:
            due[task.name] = reason
    return due


def _started_skus(vendor: str, runs: list[BenchmarkRun], metas: dict[str, dict[str, dict | None]]) -> set[str]:
    """The SKUs with a BenchmarkRun, or with a result that k8s-inspector wrote (a `run_id` in a meta), which marks
    their series as started (`popularity.rank`)."""
    from_runs = {r.spec.sku for r in runs if r.spec.vendor == vendor and r.spec.database is None}
    from_results = {sku for sku, found in metas.items() if any(m and m.get("run_id") for m in found.values())}
    return from_runs | from_results


def plan_cycle(
    kube: Kube,
    cfg: Config,
    schedule_reader: ScheduleReader,
    *,
    dry_run: bool = False,
    keeper_cache: KeeperCache | None = None,
    variant_reader: VariantReader | None = None,
    first_seen_reader: FirstSeenReader | None = None,
    on_vendor_skipped: Callable[[str, str], None] | None = None,
    quota_waits: QuotaWaits | None = None,
    popularity_cache: popularity.PopularityCache | None = None,
) -> list[PlannedRun]:
    """One autoPlan cycle. For each enabled vendor with autoPlan whose results are published (`publish`; without it
    git would never record them and the SKU would be run again and again) and that is below its caps
    (`check_vendor_budget`): take the vendor's ACTIVE SKUs from Keeper in popularity order (`popularity.rank`;
    crc32 `start_order` without `popularity_cache` or data), skip those that already
    have a BenchmarkRun (the lease) and those whose names are no label values (the anchor could not select them),
    and create a run (not forced) for each SKU with due tasks (`due_reason` on the git schedule metas) that
    Keeper does not block and that fits (a SKU that does not fit is skipped, the next is tried), until `maxInFlight`,
    `maxUsdPerHour` or `maxNewRunsPerCycle` stops the cycle. A SKU whose quota entries list `regions` is confined to the
    regions where it fits (`QuotaPass.decide`; `RunSpec.regions`). GPU tasks are dropped
    for a SKU whose `nvidia_smi` Keeper blocks (no GPU); a SKU with only GPU tasks due is not planned. A vendor
    whose publishing is stalled (`publisher.publishing_stalled`) is skipped: new runs would only pile up
    unpublished. A vendor that is `launch.paused` (BOOT_FAILURE on several SKUs, no node ready since) is skipped
    too, and so is a vendor whose SCarpenter offered-types listing is missing, unreadable or stale (`not_offered`
    fails closed). SKUs SCarpenter does not offer (`not_offered`) and SKUs waiting for a launch retry
    (`launch.deferred`, which also drops the entries of SKUs Keeper no longer lists, except in a dry run) are skipped.
    Quota-aware (`QuotaPass`): a SKU whose listing entry has `quotas` is deferred to a later cycle while its quota
    is busy (`QuotaPass.decide`), and `quota_waits` (kept by the caller across cycles, rebuilt every cycle) lets a
    SKU deferred longer than `spot.quotaReserveAfterMinutes` reserve its buckets (`QuotaPass.reserved_for`).
    When a run's node lands on new hardware, `pick_variant` creates a new variant directory that only gets the
    tasks that ran. So a task Keeper allows that the SKU's newest variant `N` has no `meta.json` for
    (`variant_reader`) is due too (`NEW_VARIANT`) when its latest attempt on the SKU (`fill_reason`: `last_attempt`
    of `schedule/<task>/meta.json`, else the meta) started before `N`'s `first_seen` (`first_seen_reader`, from
    `variant.json`; none: no fill for the SKU). After an attempt since then only the retry bounds of `due_reason`
    on that attempt apply, wherever it landed. The planner cannot choose the hardware.
    With `dry_run` nothing is created and the first such SKU of each vendor is returned. `on_vendor_skipped(vendor,
    reason)` is called for a vendor skipped for its unusable offered-types listing (`OFFERED_UNUSABLE`; metrics).

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

    aware_now = datetime.now(UTC)
    now = aware_now.replace(tzinfo=None)
    quota_waits = {} if quota_waits is None else quota_waits
    if popularity_cache:
        popularity_cache.begin_cycle()
    planned = []
    for vendor in candidates:
        try:
            metas = schedule_reader(vendor)
        except Exception:
            log.exception("autoPlan skips vendor %s: cannot read the sc-inspector-data state", vendor)
            continue
        try:
            newest = variant_reader(vendor) if variant_reader and first_seen_reader else {}
        except Exception:
            log.exception("autoPlan: no check for new hardware variants of vendor %s this cycle", vendor)
            newest = {}
        runs = crd.list_runs(kube)
        leased = {run.name for run in runs}
        skus = sorted(blocked.get(vendor, {}), key=lambda sku: start_order(vendor, sku))
        if not skus:
            log.info("autoPlan: Keeper lists no ACTIVE servers for vendor %s", vendor)
        offered = read_offered(kube, cfg, vendor) if skus else None
        skipped = not_offered(kube, cfg, vendor, skus, offered=offered) if skus else {}
        if skipped is None:
            log.warning("autoPlan skips vendor %s: its SCarpenter offered-types listing is unusable", vendor)
            if on_vendor_skipped:
                on_vendor_skipped(vendor, OFFERED_UNUSABLE)
            continue
        waiting = launch.deferred(kube, vendor, skus, datetime.now(UTC), prune=not dry_run)
        if skipped:
            examples = ", ".join(f"{sku} ({why})" for sku, why in list(skipped.items())[:3])
            log.info(
                "autoPlan skips %d SKUs of vendor %s not offered by SCarpenter, e.g. %s", len(skipped), vendor, examples
            )
        old_waits = {sku: wait for (v, sku), wait in quota_waits.items() if v == vendor and sku in skus}
        quota = QuotaPass(kube, cfg, vendor, runs, offered, aware_now, old_waits)
        new_waits: dict[str, tuple[datetime, frozenset[QuotaKey]]] = {}  # the waits of this cycle replace the old ones
        bad_names = [sku for sku in skus if not pods.is_label_value(sku)]
        if bad_names:
            log.warning(
                "autoPlan skips %d SKUs of vendor %s whose names are not label values, e.g. %s",
                len(bad_names), vendor, bad_names[0],
            )  # fmt: skip
        spot_modes = (CapacityMode.SPOT_ONLY, CapacityMode.SPOT_UNTIL_EXHAUSTED)
        spot_state = spot.State.load(kube) if settings.vendors[vendor].capacity in spot_modes else None  # once a cycle
        ready: dict[str, dict[str, str]] = {}  # SKUs with due tasks, in start order: {sku: {task: why}}
        for sku in skus:
            if crd.run_name(vendor, sku) in leased or not publisher.publish_allowed(settings, vendor, sku):
                continue
            if sku in bad_names or sku in skipped or sku in waiting:
                continue
            if None in metas.get(sku, {}).values():  # unreadable meta.json: the publisher logged which
                continue
            due = _due_tasks(
                vendor, sku, server_tasks, blocked[vendor][sku], metas.get(sku, {}),
                newest.get(sku, (None, frozenset())), first_seen_reader, now,
            )  # fmt: skip
            if due and not all(tasks.get_task(name).gpu for name in due):  # nothing due, or only GPU tasks
                entry = offered.entries.get(sku) if offered else None
                if not spot.planning_skip(spot_state, settings, vendor, sku, list(due), entry, aware_now):
                    ready[sku] = due  # SpotOnly / SpotUntilExhausted: not skipped for spot
        scores: dict[str, popularity.Score] = {}
        markets: dict[str, dict | None] = {}
        if popularity_cache and ready:
            popularity_cache.warm(cfg.env, vendor, list(ready))
            markets = popularity_cache.markets(vendor)
            scores = popularity.rank(vendor, list(ready), markets, _started_skus(vendor, runs, metas))
            ready = dict(sorted(ready.items(), key=lambda item: scores[item[0]].key))
        order = list(ready)
        unreached: list[str] = []  # SKUs after a `break`: they keep their waits
        estimates: dict[str, float] = {}  # price estimates of the runs created in this cycle
        created = 0
        room: float | None = None  # USD/hour left under maxUsdPerHour
        try:
            room = check_vendor_budget(kube, cfg, vendor, runs)
        except ValueError as e:
            log.info("autoPlan stops for vendor %s: %s", vendor, e)
            order, unreached = [], list(ready)
        for i, sku in enumerate(order):
            due = ready[sku]
            market = markets.get(sku)
            decision = quota.decide(sku, list(due), market)
            if decision.deferred:
                log.info("autoPlan defers %s/%s: %s", vendor, sku, decision.deferred)
                if decision.wait_keys is not None:
                    since = old_waits[sku][0] if sku in old_waits else aware_now
                    new_waits[sku] = (since, decision.wait_keys)
                elif decision.reserved and sku in old_waits:  # still waiting: its timer goes on
                    new_waits[sku] = old_waits[sku]
                continue
            if room is not None and decision.price is not None and decision.price > room:
                log.info(
                    "autoPlan skips %s/%s: its node costs about %g USD/hour, %g are left under maxUsdPerHour",
                    vendor, sku, decision.price, room,
                )  # fmt: skip
                continue
            if not dry_run:
                try:
                    run = _create_run(kube, cfg, vendor, sku, list(due), regions=decision.regions)
                except crd.RunExists:
                    continue
                runs.append(run)
                created += 1
                estimates[run.name] = decision.price or 0.0
            quota.consume(decision.quotas, decision.region)
            reason = ", ".join(f"{name}: {why}" for name, why in due.items())
            score = scores[sku].note if sku in scores else ""
            planned.append(PlannedRun(vendor, sku, tuple(due), reason, decision.regions, score))
            if dry_run:
                unreached = order[i + 1 :]
                break
            if created >= settings.vendors[vendor].max_new_runs_per_cycle:
                log.info("autoPlan stops for vendor %s: %d new runs, its maxNewRunsPerCycle", vendor, created)
                unreached = order[i + 1 :]
                break
            if room is not None and decision.price is None:  # the spend of this run is unknown: wait for its price
                log.info("autoPlan stops for vendor %s: no price estimate for %s under maxUsdPerHour", vendor, sku)
                unreached = order[i + 1 :]
                break
            try:
                room = check_vendor_budget(kube, cfg, vendor, runs, estimates)
            except ValueError as e:
                log.info("autoPlan stops for vendor %s: %s", vendor, e)
                unreached = order[i + 1 :]
                break
        new_waits.update({sku: old_waits[sku] for sku in unreached if sku in old_waits})
        for key in [key for key in quota_waits if key[0] == vendor]:
            del quota_waits[key]
        quota_waits.update({(vendor, sku): wait for sku, wait in new_waits.items()})
    return planned


def run_forever(
    kube: Kube,
    cfg_provider,
    schedule_reader: ScheduleReader,
    stop: threading.Event,
    variant_reader: VariantReader | None = None,
    first_seen_reader: FirstSeenReader | None = None,
    keeper_cache: KeeperCache | None = None,
    on_vendor_skipped: Callable[[str, str], None] | None = None,
    popularity_cache: popularity.PopularityCache | None = None,
) -> None:
    """Every `planner.interval_minutes`: `plan_cycle`; exceptions are logged, never fatal. `cfg_provider` is a
    zero-argument callable returning the current `Config`. `keeper_cache` is shared with the runner."""
    keeper_cache = keeper_cache or KeeperCache()
    quota_waits: QuotaWaits = {}
    while not stop.is_set():
        interval_minutes = 5
        try:
            cfg = cfg_provider()
            interval_minutes = cfg.settings.planner_interval_minutes
            for planned in plan_cycle(
                kube,
                cfg,
                schedule_reader,
                keeper_cache=keeper_cache,
                variant_reader=variant_reader,
                first_seen_reader=first_seen_reader,
                on_vendor_skipped=on_vendor_skipped,
                quota_waits=quota_waits,
                popularity_cache=popularity_cache,
            ):
                log.info(
                    "planned run",
                    extra={
                        "vendor": planned.vendor,
                        "sku": planned.sku,
                        "reason": planned.reason,
                        "regions": ",".join(planned.regions),
                        "score": planned.score,
                    },
                )
        except Exception:
            log.exception("planning cycle failed")
        stop.wait(interval_minutes * 60)
