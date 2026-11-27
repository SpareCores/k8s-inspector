"""SCarpenter's `LaunchAttempts` Events: how each candidate offering of a NodeClaim's Create ended.

SCarpenter publishes one Event per Create on the NodeClaim (reason `LaunchAttempts`, message below). The Event
outlives the NodeClaim until the Event TTL. Format v1:

    launch-attempts v1 tried=3 shown=3 [truncated] [launched=<region>|<zone>|<capacityType>|<vendor.type>]
    <region>|<zone>|<capacityType>|<vendor.type>|<class>|<code>|<message>

`launched=` is only on a success and counts in `tried`; the lines are the failed candidates. SCarpenter replaces
`|` in a message by `/`, so a line splits into 7 fields. An Event of another version is ignored.
"""

from k8s_inspector.model import LaunchAttempts

VERSION = "v1"
MAX_OUTCOMES = 50  # of one attempt, as many as SCarpenter records per Create: bounds the size of the run status
LAUNCHED = "launched"


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


def parse(message: str) -> LaunchAttempts | None:
    """The outcomes of one Event message, the launched one last; None for a message that is not a v1 one."""
    header, *lines = message.splitlines() or [""]
    words = header.split()
    if words[:2] != ["launch-attempts", VERSION]:
        return None
    fields = dict(w.split("=", 1) for w in words[2:] if "=" in w)
    try:
        tried = int(fields["tried"])
    except KeyError, ValueError:
        return None
    failed = [line.split("|", 6) for line in lines]
    outcomes = [_outcome(*parts) for parts in failed if len(parts) == 7]
    won = fields.get("launched", "").split("|")
    if len(won) == 4:
        outcomes.append(_outcome(*won, LAUNCHED))
    return LaunchAttempts(tried=tried, truncated="truncated" in words, outcomes=outcomes)


def merge(known: LaunchAttempts | None, claim: str, parsed: LaunchAttempts) -> LaunchAttempts:
    """`known` plus the Event of `claim`, at most `MAX_OUTCOMES` outcomes in all (the rest sets `truncated`)."""
    known = known or LaunchAttempts()
    outcomes = [*known.outcomes, *parsed.outcomes]
    return LaunchAttempts(
        claims=[*known.claims, claim],
        tried=known.tried + parsed.tried,
        truncated=known.truncated or parsed.truncated or len(outcomes) > MAX_OUTCOMES,
        outcomes=outcomes[:MAX_OUTCOMES],
    )
