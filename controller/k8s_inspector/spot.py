"""Spot strategy decisions and per-SKU interruption memory.

Owner: spot agent (small; may be merged into runner by the runner agent if it stays tiny). Implements
docs/explanation/spot.md "The strategy" 3, 4, 6 and the classification of an interruption. State lives in
the ConfigMap `k8s-inspector-spot`: losing it is harmless.
"""

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector.kube import Conflict, Kube, NotFound
from k8s_inspector.model import (
    AttemptOutcome,
    BenchmarkRun,
    CapacityMode,
    CapacityType,
    NodeFacts,
    Settings,
    TaskDef,
    TaskState,
)
from k8s_inspector.tasks import allows_spot, get_task

log = logging.getLogger(__name__)

SPOT_CONFIGMAP = "k8s-inspector-spot"
RETAIN_DAYS = 90  # older interruptions are dropped when a new one is recorded
INTERRUPTION_ANNOTATION = "scarpenter.sparecores.com/interruption"  # SCarpenter: value `SpotInterrupted`
INTERRUPTED_CONDITION = "Interrupted"  # NodeClaim condition type
PATCH_RETRIES = 3


def _lost_minutes(run: BenchmarkRun) -> float:
    """Task time lost to interrupted attempts. The task in flight at an interruption started when the last
    task of that attempt finished (or when the attempt started), so the loss is the time from there to the
    attempt's end. Node boot of an attempt without a finished task counts too: a slight over-count."""
    total = timedelta()
    for attempt in run.status.attempts:
        if attempt.outcome != AttemptOutcome.SPOT_INTERRUPTED or attempt.start is None or attempt.end is None:
            continue
        done = [
            t.end
            for t in run.status.tasks
            if t.attempt == attempt.n and t.state == TaskState.DONE and t.end is not None
        ]
        total += attempt.end - max([attempt.start, *done])
    return total.total_seconds() / 60


def capacity_for_next_attempt(
    run: BenchmarkRun, tasks: list[TaskDef], settings: Settings, quarantined: bool, spot_released: bool = False
) -> list[CapacityType]:
    """Capacity types the next anchor allows. `SpotOnly`: `[spot]`, always (the planner does not plan a SKU that is
    quarantined or cannot run on spot). Otherwise `[spot, on-demand]` (spot preferred, falls back by itself) or
    `[on-demand]` when: vendor/run mode is `OnDemand`, `tasks.allows_spot` is False, the run used
    `max_spot_attempts` interrupted attempts or lost more than `max_lost_minutes`, the SKU is
    quarantined, or an attempt ended `LAUNCH_FAILED` (a NodeClaim failed to launch or boot: a spot shortage falls
    back by itself, so this is something else and spot is not tried again). Other `ProvisioningFailed` attempts
    (anchor disappeared, stale node) do not change the capacity. A `SpotUntilExhausted` run gets `[spot]` only
    instead of `[spot, on-demand]` until `spot_released` (the SKU is exhausted or has no usable spot offering, see
    `spot_impossible` and `is_exhausted`)."""
    if run.spec.capacity == CapacityMode.SPOT_ONLY:
        return [CapacityType.SPOT]
    interrupted = sum(1 for a in run.status.attempts if a.outcome == AttemptOutcome.SPOT_INTERRUPTED)
    on_demand_only = (
        run.spec.capacity == CapacityMode.ON_DEMAND
        or not allows_spot(tasks, settings.spot.spot_max_task_minutes)
        or interrupted >= settings.spot.max_spot_attempts
        or _lost_minutes(run) > settings.spot.max_lost_minutes
        or quarantined
        or any(a.outcome == AttemptOutcome.LAUNCH_FAILED for a in run.status.attempts)
    )
    if on_demand_only:
        return [CapacityType.ON_DEMAND]
    if run.spec.capacity == CapacityMode.SPOT_UNTIL_EXHAUSTED and not spot_released:
        return [CapacityType.SPOT]
    return [CapacityType.SPOT, CapacityType.ON_DEMAND]


def classify_node_loss(run: BenchmarkRun, facts: NodeFacts | None, node_claim: dict | None) -> AttemptOutcome:
    """`SPOT_INTERRUPTED` when the NodeClaim/Node carries an interruption reason or the lost node's capacity
    type was spot; else `NODE_LOST`."""
    if node_claim:
        annotations = node_claim.get("metadata", {}).get("annotations") or {}
        conditions = node_claim.get("status", {}).get("conditions") or []
        if annotations.get(INTERRUPTION_ANNOTATION) or any(
            c.get("type") == INTERRUPTED_CONDITION and c.get("status") == "True" for c in conditions
        ):
            return AttemptOutcome.SPOT_INTERRUPTED
    capacity = facts.capacity_type if facts and facts.capacity_type else run.status.capacity_type
    return AttemptOutcome.SPOT_INTERRUPTED if capacity == CapacityType.SPOT else AttemptOutcome.NODE_LOST


def _key(vendor: str, sku: str) -> str:
    """ConfigMap data key of a SKU (keys allow only `[-._a-zA-Z0-9]`)."""
    return re.sub(r"[^-._a-zA-Z0-9]", "_", f"{vendor}.{sku}")


def _times(configmap: dict, key: str) -> list[datetime]:
    try:
        return [datetime.fromisoformat(t) for t in json.loads(configmap.get("data", {}).get(key, "[]"))]
    except ValueError, TypeError:
        return []  # a damaged entry is harmless: spot is tried again


def _quarantined(times: list[datetime], settings: Settings, now: datetime) -> bool:
    since = now - timedelta(days=settings.spot.quarantine_days)
    return len([t for t in times if t >= since]) >= settings.spot.interruptions_to_quarantine


def is_quarantined(kube: Kube, vendor: str, sku: str, settings: Settings, now: datetime) -> bool:
    """`interruptions_to_quarantine` interruptions within `quarantine_days` for this SKU."""
    return State.load(kube).quarantined(vendor, sku, settings, now)


class State:
    """The ConfigMap `k8s-inspector-spot`, read once (the planner reads it once per cycle). Its keys: the SKU's
    interruption times (`<vendor>.<sku>`), its counted tries (`tries--...`) and its spot rejection (`rejected--...`).
    `readable` is False when the API could not be asked: a missing ConfigMap is an empty, readable state."""

    def __init__(self, data: dict[str, str], readable: bool = True) -> None:
        self.configmap = {"data": data}
        self.readable = readable

    @classmethod
    def load(cls, kube: Kube) -> State:
        try:
            return cls(kube.get("configmap", SPOT_CONFIGMAP).get("data") or {})
        except NotFound:
            return cls({})
        except Exception:
            log.warning("cannot read %s", SPOT_CONFIGMAP, exc_info=True)
            return cls({}, readable=False)

    def quarantined(self, vendor: str, sku: str, settings: Settings, now: datetime) -> bool:
        return _quarantined(_times(self.configmap, _key(vendor, sku)), settings, now)

    def tries(self, vendor: str, sku: str, settings: Settings, now: datetime) -> Tries | None:
        return _parse_tries(self.configmap["data"].get(_tries_key(vendor, sku)), settings, now)

    def tries_of_vendor(self, vendor: str, settings: Settings, now: datetime) -> dict[str, Tries]:
        """`{sku key: tries}` of the vendor (a key is the SKU with characters a ConfigMap key does not allow replaced
        by `_`). For the metrics, which only count."""
        prefix = _tries_key(vendor, "")
        found = {
            k[len(prefix) :]: _parse_tries(v, settings, now)
            for k, v in self.configmap["data"].items()
            if k.startswith(prefix)
        }
        return {sku: tries for sku, tries in found.items() if tries}

    def rejected(self, vendor: str, sku: str, now: datetime) -> bool:
        """A spot launch of the SKU was rejected less than `REJECTED_TTL` ago."""
        try:
            return now - datetime.fromisoformat(self.configmap["data"][_rejected_key(vendor, sku)]) < REJECTED_TTL
        except KeyError, ValueError, TypeError:
            return False


def _update_key(kube: Kube, key: str, change: Callable[[str | None], str | None]) -> bool:
    """Replace the value under `key` by `change(old value or None)` (the key goes when that is None; the ConfigMap
    is created when missing). Best effort, never raises; False when it did not work."""
    try:
        for _ in range(PATCH_RETRIES):
            try:
                configmap = kube.get("configmap", SPOT_CONFIGMAP)
            except NotFound:
                new = change(None)
                if new is not None:
                    body = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": SPOT_CONFIGMAP}}
                    kube.create("configmap", {**body, "data": {key: new}})
                return True
            old = (configmap.get("data") or {}).get(key)
            new = change(old)
            if new == old:
                return True
            patch = {"metadata": {"resourceVersion": configmap["metadata"]["resourceVersion"]}, "data": {key: new}}
            try:
                kube.merge_patch("configmap", SPOT_CONFIGMAP, patch)  # None removes the key
                return True
            except Conflict:
                continue
        log.warning("%s not updated: repeated conflicts", key)
    except Exception:
        log.warning("%s not updated", key, exc_info=True)
    return False


def record_interruption(kube: Kube, vendor: str, sku: str, now: datetime) -> None:
    """Append to the SKU's list in the ConfigMap (create it if missing); best effort, never raises."""
    horizon = now - timedelta(days=RETAIN_DAYS)

    def change(old: str | None) -> str:
        kept = [t for t in _times({"data": {"k": old or "[]"}}, "k") if t >= horizon]
        return json.dumps([t.isoformat() for t in [*kept, now]])

    if not _update_key(kube, _key(vendor, sku), change):
        log.warning("interruption of %s/%s not recorded", vendor, sku)


# --------------------------------------------------------------------------- SpotUntilExhausted: tries
#
# A try is a spot-only attempt that found no spot capacity while the spot quota had room. A week is cut into slots
# (`slot_hours` UTC hours of a weekday). Per SKU the ConfigMap keeps one small value under `tries--<vendor>.<sku>`:
# the first and last try, the number of tries and the set of slots they fell in. A SKU is retried only in a slot it
# has not tried yet, and is exhausted when the first try is `patience_days` old and the tries cover `coverage` percent
# of the slots. The value is forgotten when the last try is older than twice `patience_days` (read as no tries and
# replaced by the next write) and when a spot node launches.

TRIES_PREFIX = "tries--"
REJECTED_PREFIX = "rejected--"
REJECTED_TTL = timedelta(days=30)  # a SKU whose spot launch was rejected is not planned under SpotOnly this long
FULL_WEEK_PACE = timedelta(days=1)  # once every slot has a try, a SKU is due at most this often
CONFIDENCE = 0.95  # of the upper bound `unavailable_bound` reports

Slot = tuple[int, int]  # (UTC weekday, hour // slot_hours)


@dataclass(frozen=True)
class Tries:
    first: datetime
    last: datetime
    count: int
    slots: frozenset[Slot]


def _tries_key(vendor: str, sku: str) -> str:
    return TRIES_PREFIX + _key(vendor, sku)


def _rejected_key(vendor: str, sku: str) -> str:
    return REJECTED_PREFIX + _key(vendor, sku)


def _slot(time: datetime, settings: Settings) -> Slot:
    utc = time.astimezone(UTC)
    return utc.weekday(), utc.hour // settings.spot.slot_hours


def _parse_tries(raw: str | None, settings: Settings, now: datetime) -> Tries | None:
    """The stored value; None when there is none, it is damaged, was made with another `slot_hours`, or its last try
    is older than twice `patience_days`."""
    if not raw:
        return None
    try:
        doc = json.loads(raw)
        tries = Tries(
            datetime.fromisoformat(doc["first"]),
            datetime.fromisoformat(doc["last"]),
            int(doc["count"]),
            frozenset((int(d), int(h)) for d, h in doc["slots"]),
        )
        same_slots = doc["slot_hours"] == settings.spot.slot_hours
    except KeyError, ValueError, TypeError:
        return None
    expired = now - tries.last > timedelta(days=2 * settings.spot.patience_days)
    return None if expired or not same_slots else tries


def _dump_tries(tries: Tries, settings: Settings) -> str:
    return json.dumps(
        {
            "first": tries.first.isoformat(),
            "last": tries.last.isoformat(),
            "count": tries.count,
            "slots": sorted(tries.slots),
            "slot_hours": settings.spot.slot_hours,
        }
    )


def record_try(kube: Kube, vendor: str, sku: str, start: datetime, settings: Settings) -> Tries | None:
    """Count a try that started at `start`. Returns the tries now, None when the ConfigMap could not be changed (the
    try is then not counted)."""
    result: list[Tries] = []

    def change(raw: str | None) -> str:
        old = _parse_tries(raw, settings, start)
        new = Tries(
            min(old.first, start) if old else start,
            max(old.last, start) if old else start,
            (old.count if old else 0) + 1,
            (old.slots if old else frozenset()) | {_slot(start, settings)},
        )
        result[:] = [new]
        return _dump_tries(new, settings)

    return result[0] if _update_key(kube, _tries_key(vendor, sku), change) else None


def clear_tries(kube: Kube, vendor: str, sku: str) -> None:
    """A spot node launched: the SKU's tries and spot rejection are forgotten (the keys go)."""
    for key in (_tries_key(vendor, sku), _rejected_key(vendor, sku)):
        _update_key(kube, key, lambda old: None)


def record_rejected(kube: Kube, vendor: str, sku: str, now: datetime) -> bool:
    """A spot launch of the SKU was rejected: `SpotOnly` does not plan it for `REJECTED_TTL`."""
    return _update_key(kube, _rejected_key(vendor, sku), lambda old: now.isoformat())


def slots_per_week(settings: Settings) -> int:
    return 7 * -(-24 // settings.spot.slot_hours)


def unavailable_bound(tries: int) -> float:
    """Upper bound (at `CONFIDENCE`) of the share of tries that would find spot, after `tries` failed ones."""
    return 1 - (1 - CONFIDENCE) ** (1 / tries) if tries else 1.0


def describe_tries(sku: str, tries: Tries, settings: Settings) -> str:
    """`x8i.96xlarge: 41/56 slots tried, spot available <7% (95%)`."""
    return (
        f"{sku}: {len(tries.slots)}/{slots_per_week(settings)} slots tried, "
        f"spot available <{unavailable_bound(tries.count):.0%} ({CONFIDENCE:.0%})"
    )


def is_exhausted(tries: Tries | None, settings: Settings, now: datetime) -> bool:
    """The first counted try is `patience_days` old and counted tries fell in `coverage` percent of the week's slots."""
    if not tries:
        return False
    old_enough = now - tries.first >= timedelta(days=settings.spot.patience_days)
    covered = 100 * len(tries.slots) >= settings.spot.coverage * slots_per_week(settings)
    return old_enough and covered


def try_due(tries: Tries | None, settings: Settings, now: datetime) -> bool:
    """The SKU is offered for planning: the current slot is one it has not tried yet. Once every slot of the week has a
    try, a SKU is due once a day (`FULL_WEEK_PACE`): that is the churn of a SKU that stays unavailable and is not
    exhausted yet (`SpotOnly`, or `SpotUntilExhausted` before its patience is over)."""
    if not tries:
        return True
    if len(tries.slots) < slots_per_week(settings):
        return _slot(now, settings) not in tries.slots
    return now - tries.last >= FULL_WEEK_PACE


# --------------------------------------------------------------------------- SpotUntilExhausted / SpotOnly: planning


_announced: set[tuple[str, str, datetime]] = set()  # exhausted SKUs the planner has logged (this process)


def spot_impossible(entry: dict | None) -> str | None:
    """Why spot can never launch the SKU, from its entry in SCarpenter's offered listing (`planner.read_offered`):
    none of its offering groups, available or not, is a spot one (`offered_types`), or the spot limit is below what
    one instance needs. A spot offering without quotas is possible. None when spot is possible or not known (an entry
    without offering groups says nothing)."""
    if not entry or not entry["offered_types"]:
        return None
    if CapacityType.SPOT not in entry["offered_types"]:
        return "no spot offering"
    group = entry["quotas"].get(CapacityType.SPOT)
    if group and any(q["maxLimit"] < q["need"] for q in group):
        return "spot quota can never fit"
    return None


def quota_busy(group: list[dict], regions: tuple[str, ...] = ()) -> bool:
    """One of the spot quota entries has less headroom than one instance needs. An entry with per-region headroom is
    judged by the most headroom among `regions` (the regions the run is confined to; all of its regions when none),
    one without by its `maxHeadroom`."""
    for q in group:
        per_region = q.get("regions") or {}
        wanted = [r for r in (regions or per_region) if r in per_region]
        room = max((per_region[r]["headroom"] for r in wanted), default=q["maxHeadroom"])
        if q["need"] > room:
            return True
    return False


def spot_released(state: State, vendor: str, sku: str, entry: dict | None, settings: Settings, now: datetime) -> bool:
    """A `SpotUntilExhausted` SKU may use on-demand: spot is impossible or the SKU is exhausted."""
    return bool(spot_impossible(entry)) or is_exhausted(state.tries(vendor, sku, settings, now), settings, now)


def planning_skip(
    state: State | None,
    settings: Settings,
    vendor: str,
    sku: str,
    task_names: list[str],
    entry: dict | None,
    now: datetime,
) -> str | None:
    """Why the planner leaves the SKU for later under the vendor's spot-only capacity modes, else None (also for the
    other modes, whose `state` is None). `entry` is the SKU's entry of the offered listing, `state` the ConfigMap
    read once for the cycle. An unreadable ConfigMap skips the SKU: nothing is known of its tries or quarantine.

    - `SpotOnly`: skipped when spot is impossible, its spot launch was rejected (`record_rejected`), its due tasks do
      not allow spot or it is quarantined: it never runs on-demand.
    - `SpotUntilExhausted`: skipped while spot is possible, the SKU is not exhausted and the current slot is one it
      has tried already (`try_due`). Where on-demand is allowed at once (spot impossible, quarantined, tasks that do
      not allow spot) or the SKU is exhausted, the run is planned like a `SpotFirst` one."""
    mode = settings.vendors[vendor].capacity
    if state is None or mode not in (CapacityMode.SPOT_ONLY, CapacityMode.SPOT_UNTIL_EXHAUSTED):
        return None
    if not state.readable:
        return f"{SPOT_CONFIGMAP} is unreadable"
    impossible = spot_impossible(entry)
    spot_ok = allows_spot([get_task(name) for name in task_names], settings.spot.spot_max_task_minutes)
    quarantined = state.quarantined(vendor, sku, settings, now)
    if mode == CapacityMode.SPOT_ONLY:
        if state.rejected(vendor, sku, now):
            return "its spot launch was rejected"
        return (
            impossible
            or (None if spot_ok else "its tasks do not allow spot")
            or ("quarantined" if quarantined else None)
        )
    if impossible or not spot_ok or quarantined:
        return None
    tries = state.tries(vendor, sku, settings, now)
    if is_exhausted(tries, settings, now):
        if (vendor, sku, tries.first) not in _announced:  # once per SKU state, not every cycle
            _announced.add((vendor, sku, tries.first))
            log.info("%s: exhausted, on-demand allowed", describe_tries(sku, tries, settings))
        return None
    return None if try_due(tries, settings, now) else "spot was tried in this slot"
