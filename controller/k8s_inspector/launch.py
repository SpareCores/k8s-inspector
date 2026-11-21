"""Launch failures: the class SCARPENTER reports, the reason k8s-inspector records, and the retry memory.

Implements docs/explanation/launch-retries.md. SCarpenter writes `<Class> (<Code>): <message>` on the NodeClaim
launch condition; `reason_of_class` maps the class to a reason (no keyword guessing). The reasons:

- temporary (ICE, QUOTA, DAILY_LIMIT, VENDOR): nothing is written to sc-inspector-data, the BenchmarkRun is
  deleted and the SKU waits for its backoff in the per-vendor ConfigMap `k8s-inspector-launch-<vendor>`;
- permanent (LAUNCH_REJECTED, BOOT_FAILURE after `BOOT_FAILURE_LIMIT` runs): a `-1 SCARPENTER_<reason>` schedule
  meta, final until the task hash changes and re-checked after 30 days (`planner.due_reason`).

The ConfigMap holds `{sku: {reason, since, count, next}}` as JSON under the key `skus`. Losing it only means an
earlier retry. Entries go when the SKU gets a node or is no longer in Keeper's ACTIVE list.
"""

import json
import logging
import re
from datetime import UTC, datetime, timedelta

from k8s_inspector.kube import Conflict, Kube, NotFound

log = logging.getLogger(__name__)

CLASS_REASONS = {
    "InsufficientCapacity": "ICE",
    "ZoneUnusable": "ICE",
    "QuotaExceeded": "QUOTA",
    "DailyLimit": "DAILY_LIMIT",
    "Retryable": "VENDOR",
    "Unsupported": "LAUNCH_REJECTED",
    "InvalidOffering": "LAUNCH_REJECTED",
    "Fatal": "LAUNCH_REJECTED",
}
# NO_OFFERING (no instance type for the SKU, or an anchor nobody schedules): SCarpenter's mark is usually short, but
# where the offered-types filter fails open nothing else stops the retries, so the wait is a flat 24 h.
TEMPORARY = ("ICE", "QUOTA", "DAILY_LIMIT", "VENDOR", "NO_OFFERING", "NO_LSCPU")
# Reasons of `-1` schedule metas written before the classes existed; they were temporary too.
LEGACY_TEMPORARY = ("QUOTA_ZERO", "LAUNCH_FAILED")
NO_NODE_MESSAGE = ": no node after "  # the old BOOT_FAILURE of a run that merely waited out its budget
PERMANENT = ("LAUNCH_REJECTED", "BOOT_FAILURE")
BOOT_FAILURE_LIMIT = 3  # runs that ended BOOT_FAILURE before it is final
REASONS = (*TEMPORARY, "BOOT_FAILURE")  # the reasons an entry of the ConfigMap can have
# Longest wait between two launches of a SKU, per reason; the wait doubles from 1 h with each failure
# (NO_OFFERING: `NO_OFFERING_WAIT` at once).
MAX_WAIT = {
    "ICE": timedelta(hours=24),
    "QUOTA": timedelta(hours=6),
    "VENDOR": timedelta(hours=6),
    "BOOT_FAILURE": timedelta(hours=6),
    "DAILY_LIMIT": timedelta(hours=6),  # without SCarpenter's `until`
    "NO_LSCPU": timedelta(hours=6),  # the publisher: a run ended without an lscpu result (not a launch failure)
}
NO_OFFERING_WAIT = timedelta(hours=24)
RETRY_AFTER_PERMANENT = timedelta(days=30)
PATCH_RETRIES = 3

# The class may be wrapped (`launching X: Retryable (RequestLimitExceeded): ...`): search for the token.
_CLASSIFIED = re.compile(rf"\b({'|'.join(CLASS_REASONS)}) \([^)]*\): ")
_ERROR = re.compile(r"^SCARPENTER_([A-Z_]+):")


def reason_of_class(message: str, default: str) -> str:
    """The reason of a NodeClaim message containing `<Class> (<Code>): <text>`; `default` when it has no known class
    (an older SCarpenter, or a failure that is not a launch call)."""
    found = _CLASSIFIED.search(message)
    return CLASS_REASONS.get(found.group(1), default) if found else default


def error_message(reason: str, message: str) -> str:
    """The `error_msg` of a `-1` task: `SCARPENTER_<reason>: <message>`."""
    return f"SCARPENTER_{reason}: {message}"


def reason_of_error(error_msg: str | None) -> str | None:
    """The reason of an `error_msg` made by `error_message`, else None."""
    found = _ERROR.match(str(error_msg or ""))
    return found.group(1) if found else None


def is_temporary(error_msg: str | None) -> bool:
    reason = reason_of_error(error_msg)
    return reason in (*TEMPORARY, *LEGACY_TEMPORARY) or (reason == "BOOT_FAILURE" and NO_NODE_MESSAGE in str(error_msg))


def wait_after(reason: str, count: int) -> timedelta:
    """Wait before the SKU is launched again after its `count`-th failure in a row."""
    if reason == "NO_OFFERING":
        return NO_OFFERING_WAIT
    return min(timedelta(hours=2 ** min(count - 1, 10)), MAX_WAIT[reason])


# ----------------------------------------------------------------------------- the ConfigMap


def configmap_name(vendor: str) -> str:
    return f"k8s-inspector-launch-{vendor}"


def _entries(configmap: dict) -> dict[str, dict]:
    """The entries of a ConfigMap's `skus`: only the dict ones, none when `skus` is not a JSON object."""
    skus = json.loads(configmap["data"]["skus"])
    return {sku: entry for sku, entry in skus.items() if isinstance(entry, dict)} if isinstance(skus, dict) else {}


def read(kube: Kube, vendor: str) -> dict[str, dict]:
    """The vendor's entries. A missing, damaged or unreadable ConfigMap has none, a damaged entry is dropped: the
    SKUs are launched as if they had not failed."""
    try:
        return _entries(kube.get("configmap", configmap_name(vendor)))
    except NotFound, KeyError, TypeError, ValueError:
        return {}
    except Exception:
        log.warning("cannot read %s", configmap_name(vendor), exc_info=True)
        return {}


def _change(kube: Kube, vendor: str, mutate) -> None:
    """Apply `mutate(entries)` to the vendor's entries (create the ConfigMap when missing). Best effort:
    never raises."""
    name = configmap_name(vendor)
    try:
        for _ in range(PATCH_RETRIES):
            try:
                configmap = kube.get("configmap", name)
            except NotFound:
                entries: dict[str, dict] = {}
                mutate(entries)
                if not entries:
                    return
                body = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name}}
                kube.create("configmap", {**body, "data": {"skus": json.dumps(entries)}})
                return
            try:
                entries = _entries(configmap)
            except KeyError, TypeError, ValueError:
                entries = {}
            before = json.dumps(entries)
            mutate(entries)
            if json.dumps(entries) == before:
                return
            patch = {
                "metadata": {"resourceVersion": configmap["metadata"]["resourceVersion"]},
                "data": {"skus": json.dumps(entries)},
            }
            try:
                kube.merge_patch("configmap", name, patch)
                return
            except Conflict:
                continue
        log.warning("%s not updated: repeated conflicts", name)
    except Exception:
        log.warning("%s not updated", name, exc_info=True)


def record_failure(kube: Kube, vendor: str, sku: str, reason: str, now: datetime, until: datetime | None = None) -> int:
    """Count a failed launch of the SKU (failures in a row of the same reason) and set when it may be tried again:
    after the backoff, or at `until` (when SCarpenter says when a daily limit resets). Returns the count (1 when
    the ConfigMap could not be read: the retry is then only earlier)."""
    count = 1

    def mutate(entries: dict[str, dict]) -> None:
        nonlocal count
        old = entries.get(sku) or {}
        if old.get("reason") != reason:
            old = {}  # a different failure starts counting again
        count = old["count"] + 1 if isinstance(old.get("count"), int) else 1
        wait = until if until and until > now else now + wait_after(reason, count)
        entries[sku] = {
            "reason": reason,
            "since": old.get("since") or now.isoformat(),
            "count": count,
            "next": wait.isoformat(),
        }

    _change(kube, vendor, mutate)
    return count


def clear(kube: Kube, vendor: str, skus: list[str]) -> None:
    """Remove the entries of `skus` (a node was launched, or they are gone from Keeper's list)."""

    def mutate(entries: dict[str, dict]) -> None:
        for sku in skus:
            entries.pop(sku, None)

    _change(kube, vendor, mutate)


def deferred(kube: Kube, vendor: str, active: list[str], now: datetime, *, prune: bool = True) -> set[str]:
    """The SKUs that must not be launched yet (`next` in the future). With `prune`, entries of SKUs not in
    `active` (Keeper's ACTIVE list) are removed; with an empty `active` (Keeper listed nothing) none are."""
    entries = read(kube, vendor)
    gone = [sku for sku in entries if prune and active and sku not in active]
    if gone:
        clear(kube, vendor, gone)
    return {sku for sku, e in entries.items() if sku not in gone and is_waiting(e, now)}


def is_waiting(entry: dict, now: datetime) -> bool:
    """The entry's `next` is still ahead."""
    return _time(entry.get("next")) > now


def _time(text: object) -> datetime:
    """`text` as an aware time; anything else (not a string, damaged, no timezone) is the distant past: a damaged
    entry blocks nothing."""
    try:
        time = datetime.fromisoformat(text)
    except TypeError, ValueError:
        time = None
    return time if time and time.tzinfo else datetime.min.replace(tzinfo=UTC)
