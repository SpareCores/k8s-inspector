"""Spot strategy decisions and per-SKU interruption memory.

Owner: spot agent (small; may be merged into runner by the runner agent if it stays tiny). Implements
docs/explanation/spot.md "The strategy" 3, 4, 6 and the classification of an interruption. State lives in
the ConfigMap `k8s-inspector-spot`: losing it is harmless.
"""

import json
import logging
import re
from datetime import datetime, timedelta

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
from k8s_inspector.tasks import allows_spot

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
    run: BenchmarkRun, tasks: list[TaskDef], settings: Settings, quarantined: bool
) -> list[CapacityType]:
    """Capacity types the next anchor allows: `[spot, on-demand]` (spot preferred, falls back by itself) or
    `[on-demand]` when: vendor/run mode is `OnDemand`, `tasks.allows_spot` is False, the run used
    `max_spot_attempts` interrupted attempts or lost more than `max_lost_minutes`, the SKU is
    quarantined, or an attempt ended `LAUNCH_FAILED` (a NodeClaim failed to launch or boot: a spot shortage falls
    back by itself, so this is something else and spot is not tried again). Other `ProvisioningFailed` attempts
    (anchor disappeared, stale node) do not change the capacity."""
    interrupted = sum(1 for a in run.status.attempts if a.outcome == AttemptOutcome.SPOT_INTERRUPTED)
    on_demand_only = (
        run.spec.capacity == CapacityMode.ON_DEMAND
        or not allows_spot(tasks, settings.spot.spot_max_task_minutes)
        or interrupted >= settings.spot.max_spot_attempts
        or _lost_minutes(run) > settings.spot.max_lost_minutes
        or quarantined
        or any(a.outcome == AttemptOutcome.LAUNCH_FAILED for a in run.status.attempts)
    )
    return [CapacityType.ON_DEMAND] if on_demand_only else [CapacityType.SPOT, CapacityType.ON_DEMAND]


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
    except ValueError:
        return []  # a damaged entry is harmless: spot is tried again


def is_quarantined(kube: Kube, vendor: str, sku: str, settings: Settings, now: datetime) -> bool:
    """`interruptions_to_quarantine` interruptions within `quarantine_days` for this SKU."""
    try:
        configmap = kube.get("configmap", SPOT_CONFIGMAP)
    except NotFound:
        return False
    except Exception:
        log.warning("cannot read %s", SPOT_CONFIGMAP, exc_info=True)
        return False
    since = now - timedelta(days=settings.spot.quarantine_days)
    recent = [t for t in _times(configmap, _key(vendor, sku)) if t >= since]
    return len(recent) >= settings.spot.interruptions_to_quarantine


def record_interruption(kube: Kube, vendor: str, sku: str, now: datetime) -> None:
    """Append to the SKU's list in the ConfigMap (create it if missing); best effort, never raises."""
    key = _key(vendor, sku)
    try:
        for _ in range(PATCH_RETRIES):
            try:
                configmap = kube.get("configmap", SPOT_CONFIGMAP)
            except NotFound:
                body = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": SPOT_CONFIGMAP}}
                kube.create("configmap", {**body, "data": {key: json.dumps([now.isoformat()])}})
                return
            keep = [t for t in _times(configmap, key) if t >= now - timedelta(days=RETAIN_DAYS)]
            patch = {
                "metadata": {"resourceVersion": configmap["metadata"]["resourceVersion"]},
                "data": {key: json.dumps([t.isoformat() for t in [*keep, now]])},
            }
            try:
                kube.merge_patch("configmap", SPOT_CONFIGMAP, patch)
                return
            except Conflict:
                continue
        log.warning("interruption of %s/%s not recorded: repeated conflicts", vendor, sku)
    except Exception:
        log.warning("interruption of %s/%s not recorded", vendor, sku, exc_info=True)
