"""Spot strategy decisions and per-SKU interruption memory.

Owner: spot agent (small; may be merged into runner by the runner agent if it stays tiny). Implements
docs/explanation/spot.md "The strategy" 3, 4, 6 and the classification of an interruption. State lives in
the ConfigMap `k8s-inspector-spot`: losing it is harmless.
"""

from datetime import datetime

from k8s_inspector.kube import Kube
from k8s_inspector.model import AttemptOutcome, BenchmarkRun, CapacityType, NodeFacts, Settings, TaskDef

SPOT_CONFIGMAP = "k8s-inspector-spot"


def capacity_for_next_attempt(
    run: BenchmarkRun, tasks: list[TaskDef], settings: Settings, quarantined: bool
) -> list[CapacityType]:
    """Capacity types the next anchor allows: `[spot, on-demand]` (spot preferred, falls back by itself) or
    `[on-demand]` when: vendor/run mode is `OnDemand`, `tasks.allows_spot` is False, the run used
    `max_spot_attempts` interrupted attempts or lost more than `max_lost_minutes`, or the SKU is
    quarantined."""
    raise NotImplementedError


def classify_node_loss(run: BenchmarkRun, facts: NodeFacts | None, node_claim: dict | None) -> AttemptOutcome:
    """`SPOT_INTERRUPTED` when the NodeClaim/Node carries an interruption reason or the lost node's capacity
    type was spot; else `NODE_LOST`."""
    raise NotImplementedError


def is_quarantined(kube: Kube, vendor: str, sku: str, settings: Settings, now: datetime) -> bool:
    """`interruptions_to_quarantine` interruptions within `quarantine_days` for this SKU."""
    raise NotImplementedError


def record_interruption(kube: Kube, vendor: str, sku: str, now: datetime) -> None:
    """Append to the SKU's list in the ConfigMap (create it if missing); best effort, never raises."""
    raise NotImplementedError
