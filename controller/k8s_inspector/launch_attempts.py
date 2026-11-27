"""SCarpenter's `LaunchAttempts` Events: how each candidate offering of a NodeClaim's Create ended.

SCarpenter publishes one Event per Create that made a vendor call on the NodeClaim (reason `LaunchAttempts`, message
below). A claim can have several: Karpenter calls Create again on the same claim after a `Retryable` or `Fatal`
error. The Event outlives the NodeClaim until the Event TTL. Format v1:

    launch-attempts v1 tried=3 shown=3 [truncated] [launched=<region>|<zone>|<capacityType>|<vendor.type>]
    <region>|<zone>|<capacityType>|<vendor.type>|<class>|<code>|<message>

`launched=` is only on a success and counts in `tried`; the lines are the failed candidates. SCarpenter replaces
`|` in a message by `/`, so a line splits into 7 fields. An Event of another version is ignored.
"""

from k8s_inspector.model import LaunchEvent

VERSION = "v1"
MAX_FAILED = 50  # failed candidates of one attempt: bounds the size of the run status, as SCarpenter bounds a Create
LAUNCHED = "launched"
COMBINED = "(combined from similar events): "  # what Kubernetes puts before the message of an aggregated Event


def _outcome(region: str, zone: str, capacity_type: str, instance_type: str, outcome: str, code=None, message=None):
    return {
        "region": region,
        "zone": zone,
        "capacity_type": capacity_type,
        "instance_type": instance_type,
        "outcome": outcome,
        "code": code,
        "message": message,
    }


def parse(message: str) -> LaunchEvent | None:
    """The outcomes of one Event message (nameless, timeless); None for a message that is not a v1 one."""
    header, *lines = message.removeprefix(COMBINED).splitlines() or [""]
    words = header.split()
    if words[:2] != ["launch-attempts", VERSION]:
        return None
    fields = dict(w.split("=", 1) for w in words[2:] if "=" in w)
    try:
        tried = int(fields["tried"])
    except KeyError, ValueError:
        return None
    failed = [_outcome(*parts) for parts in (line.split("|", 6) for line in lines) if len(parts) == 7]
    won = fields.get("launched", "").split("|")
    launched = _outcome(*won, LAUNCHED) if len(won) == 4 else None
    return LaunchEvent("", None, tried, "truncated" in words, failed, launched)


def add(known: list[LaunchEvent], event: LaunchEvent) -> list[LaunchEvent]:
    """`known` plus `event`, whose failed candidates are cut to what is left of `MAX_FAILED` (that sets `truncated`).
    The launched candidate is never cut."""
    room = MAX_FAILED - sum(len(e.failed) for e in known)
    cut = len(event.failed) > room
    kept = event.failed[: max(room, 0)]
    return [*known, LaunchEvent(event.name, event.time, event.tried, event.truncated or cut, kept, event.launched)]


def summary(events: list[LaunchEvent]) -> dict:
    """The attempt's fields of `run.json`: the failed candidates of all Events in the order tried (Event time), then
    the launched one."""
    events = sorted(events, key=lambda e: e.time.timestamp() if e.time else 0)
    outcomes = [o for e in events for o in e.failed] + [e.launched for e in events if e.launched]
    return {
        "launch_attempts": outcomes,
        "launch_attempts_tried": sum(e.tried for e in events),
        "launch_attempts_truncated": any(e.truncated for e in events),
    }
