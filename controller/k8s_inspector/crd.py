"""BenchmarkRun custom resource: naming, (de)serialisation, create/get/list/status helpers.

Owner: kube/crd agent. Implements docs/reference/objects.md "BenchmarkRun" and state.md "The lease".
Status writes from several threads (reconciler, publisher) go through `update_status`, which is
read-modify-write with `resourceVersion` and retries on `Conflict`, so no caller needs a lock.
"""

import re
import secrets
import zlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from k8s_inspector.kube import AlreadyExists, Conflict, Kube, call_with_retries
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    BenchmarkRun,
    CapacityMode,
    CapacityType,
    RunKey,
    RunPhase,
    RunSpec,
    RunStatus,
    TaskState,
    TaskStatus,
)

API_VERSION = "inspector.sparecores.com/v1alpha1"
KIND = "BenchmarkRun"
LABEL_RUN = "inspector.sparecores.com/run"
LABEL_VENDOR = "inspector.sparecores.com/vendor"
ANNOTATION_CANCEL = "inspector.sparecores.com/cancel"

MAX_NAME_LENGTH = 63  # DNS-1123 label, also the limit for label values and Pod name prefixes
UPDATE_ATTEMPTS = 5


class RunExists(Exception):
    """A BenchmarkRun for this SKU exists (the lease is held)."""


# --------------------------------------------------------------------------- names and ids


def _slug(text: str) -> str:
    """Lowercase, every run of characters outside a-z0-9 becomes `-`, no leading or trailing `-`."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def run_name(vendor: str, sku: str) -> str:
    """`<vendor>-<sku slug>-<crc32 hex of "vendor/sku", 8 digits>`, a DNS-1123 name (max 63 chars, so the
    slug is truncated). Pure function of the SKU: `oci-vm-standard-e5-flex-2-16-3f9c1a7e`. Slug: lowercase,
    every run of non-alphanumerics becomes `-`."""
    checksum = f"{zlib.crc32(f'{vendor}/{sku}'.encode()):08x}"
    prefix = "-".join(part for part in (_slug(vendor), _slug(sku)) if part) or "run"
    prefix = prefix[: MAX_NAME_LENGTH - len(checksum) - 1].rstrip("-")
    return f"{prefix}-{checksum}"


def new_run_id(now: datetime) -> str:
    """`r<UTC yyyymmddThhmm>-<4 hex>` (output-contract.md)."""
    return f"r{now.astimezone(UTC):%Y%m%dt%H%M}-{secrets.token_hex(2)}"


def run_key(run: BenchmarkRun) -> RunKey:
    """Object storage identity of the run: vendor, sku, creation date (UTC) of the object, run id."""
    return RunKey(
        vendor=run.spec.vendor,
        sku=run.spec.sku,
        day=run.created.astimezone(UTC).date(),
        run_id=run.spec.run_id,
    )


# --------------------------------------------------------------------------- time helpers


def _format_time(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _format_duration(value: timedelta) -> str:
    """`0s`, `45s`, `90m`, `2h`: the largest unit that divides the duration evenly."""
    seconds = int(value.total_seconds())
    if seconds and seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds and seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _parse_duration(text: str) -> timedelta:
    """`2h`, `90m`, `45s`, `1h30m`."""
    if not re.fullmatch(r"(\d+[hms])+", text):
        raise ValueError(f"invalid duration {text!r}, expected e.g. 2h, 90m, 1h30m")
    units = {"h": 3600, "m": 60, "s": 1}
    return timedelta(seconds=sum(int(n) * units[u] for n, u in re.findall(r"(\d+)([hms])", text)))


# --------------------------------------------------------------------------- object <-> dataclasses


def to_manifest(spec: RunSpec, namespace: str) -> dict:
    """The BenchmarkRun object to create: name, labels (vendor), spec in camelCase (`runId`, `holdAfter` as
    "2h"-style string)."""
    name = run_name(spec.vendor, spec.sku)
    spec_dict = {
        "vendor": spec.vendor,
        "sku": spec.sku,
        "runId": spec.run_id,
        "tasks": list(spec.tasks),
        "force": spec.force,
        "capacity": spec.capacity.value,
        "holdAfter": _format_duration(spec.hold_after),
    }
    if spec.database is not None:
        spec_dict["database"] = spec.database
    return {
        "apiVersion": API_VERSION,
        "kind": KIND,
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {LABEL_VENDOR: spec.vendor, LABEL_RUN: name},
        },
        "spec": spec_dict,
    }


def _task_from_dict(d: dict) -> TaskStatus:
    return TaskStatus(
        name=d["name"],
        state=TaskState(d.get("state", TaskState.PENDING)),
        attempt=d.get("attempt"),
        start=_parse_time(d.get("start")),
        end=_parse_time(d.get("end")),
        exit_code=d.get("exitCode"),
        error_msg=d.get("errorMsg"),
        published=d.get("published", False),
        commit=d.get("commit"),
        gpu_fallback=d.get("gpuFallback", False),
    )


def _attempt_from_dict(d: dict) -> AttemptStatus:
    outcome = d.get("outcome")
    return AttemptStatus(
        n=d["n"],
        capacity_type=CapacityType(d.get("capacityType", CapacityType.ON_DEMAND)),
        node_claim=d.get("nodeClaim"),
        node=d.get("node"),
        start=_parse_time(d.get("start")),
        end=_parse_time(d.get("end")),
        outcome=AttemptOutcome(outcome) if outcome else None,
    )


def _status_from_dict(d: dict) -> RunStatus:
    capacity_type = d.get("capacityType")
    return RunStatus(
        phase=RunPhase(d.get("phase", RunPhase.PENDING)),
        progress=d.get("progress", "0/0"),
        current_step=d.get("currentStep"),
        step=d.get("step"),
        attempt=d.get("attempt", 0),
        capacity_type=CapacityType(capacity_type) if capacity_type else None,
        node=d.get("node"),
        region=d.get("region"),
        zone=d.get("zone"),
        price_per_hour=d.get("pricePerHour"),
        price_incomplete=bool(d.get("priceIncomplete", False)),
        variant=d.get("variant"),
        tasks=[_task_from_dict(t) for t in d.get("tasks") or []],
        attempts=[_attempt_from_dict(a) for a in d.get("attempts") or []],
        conditions=list(d.get("conditions") or []),
    )


def from_dict(obj: dict) -> BenchmarkRun:
    """Parse an object returned by the API (tolerates a missing `status`)."""
    meta = obj["metadata"]
    spec = obj["spec"]
    return BenchmarkRun(
        name=meta["name"],
        namespace=meta.get("namespace", ""),
        uid=meta.get("uid", ""),
        resource_version=meta.get("resourceVersion", ""),
        created=_parse_time(meta["creationTimestamp"]),
        spec=RunSpec(
            vendor=spec["vendor"],
            sku=spec["sku"],
            run_id=spec["runId"],
            tasks=list(spec.get("tasks") or []),
            force=spec.get("force", False),
            capacity=CapacityMode(spec.get("capacity", CapacityMode.SPOT_FIRST)),
            hold_after=_parse_duration(spec.get("holdAfter", "0s")),
            database=spec.get("database"),
        ),
        status=_status_from_dict(obj.get("status") or {}),
        cancel_requested=(meta.get("annotations") or {}).get(ANNOTATION_CANCEL) == "true",
    )


def _task_to_dict(task: TaskStatus) -> dict:
    d = {
        "name": task.name,
        "state": task.state.value,
        "start": _format_time(task.start),
        "end": _format_time(task.end),
        "exitCode": task.exit_code,
        "published": task.published,
        "commit": task.commit,
    }
    if task.attempt is not None:
        d["attempt"] = task.attempt
    if task.error_msg is not None:
        d["errorMsg"] = task.error_msg
    if task.gpu_fallback:
        d["gpuFallback"] = True
    return d


def _attempt_to_dict(attempt: AttemptStatus) -> dict:
    d = {
        "n": attempt.n,
        "capacityType": attempt.capacity_type.value,
        "start": _format_time(attempt.start),
        "end": _format_time(attempt.end),
        "outcome": attempt.outcome.value if attempt.outcome else None,
    }
    if attempt.node_claim is not None:
        d["nodeClaim"] = attempt.node_claim
    if attempt.node is not None:
        d["node"] = attempt.node
    return d


def status_to_dict(status: RunStatus) -> dict:
    """camelCase `status` as in objects.md, timestamps ISO-8601 UTC. Unset scalar fields are `None`, which a
    JSON merge patch turns into removing the field."""
    return {
        "phase": status.phase.value,
        "progress": status.progress,
        "currentStep": status.current_step,
        "step": status.step,
        "attempt": status.attempt,
        "capacityType": status.capacity_type.value if status.capacity_type else None,
        "node": status.node,
        "region": status.region,
        "zone": status.zone,
        "pricePerHour": status.price_per_hour,
        "priceIncomplete": status.price_incomplete,
        "variant": status.variant,
        "tasks": [_task_to_dict(t) for t in status.tasks],
        "attempts": [_attempt_to_dict(a) for a in status.attempts],
        "conditions": status.conditions,
    }


def _progress(status: RunStatus) -> str:
    finished = sum(1 for t in status.tasks if t.state == TaskState.DONE)
    return f"{finished}/{len(status.tasks)}"


# --------------------------------------------------------------------------- API helpers


def create_run(kube: Kube, spec: RunSpec) -> BenchmarkRun:
    """Create with `status.phase=Pending`. Raise `RunExists` on `AlreadyExists`. Transient API errors are retried
    here step by step: when a create's response was lost, the retry finds the run with our `run_id` and carries on."""
    name = run_name(spec.vendor, spec.sku)
    manifest = to_manifest(spec, kube.namespace)
    try:
        call_with_retries("create run", lambda: kube.create("benchmarkrun", manifest))
    except AlreadyExists as e:
        existing = get_run(kube, name)
        if existing.spec.run_id != spec.run_id:
            raise RunExists(name) from e
    # The status subresource ignores `status` on create, so write the initial status separately (idempotent).
    return call_with_retries("initialise run status", lambda: update_status(kube, name, lambda status: None))


def get_run(kube: Kube, name: str) -> BenchmarkRun:
    """Raise `kube.NotFound`."""
    return from_dict(kube.get("benchmarkrun", name))


def list_runs(kube: Kube) -> list[BenchmarkRun]:
    return [from_dict(obj) for obj in kube.list("benchmarkrun")]


def update_status(kube: Kube, name: str, mutate: Callable[[RunStatus], None]) -> BenchmarkRun:
    """GET, apply `mutate` to the parsed status in place, PATCH `status` with the object's resourceVersion;
    on `Conflict` start again (max 5 times). Also refreshes `progress` from `tasks` (finished/planned).
    Returns the updated run."""
    for _ in range(UPDATE_ATTEMPTS):
        run = get_run(kube, name)
        mutate(run.status)
        run.status.progress = _progress(run.status)
        body = {"metadata": {"resourceVersion": run.resource_version}, "status": status_to_dict(run.status)}
        try:
            return from_dict(kube.merge_patch("benchmarkrun", name, body, subresource_status=True))
        except Conflict:
            continue
    raise Conflict(f"BenchmarkRun {name}: status update still conflicting after {UPDATE_ATTEMPTS} attempts")


def request_cancel(kube: Kube, name: str) -> None:
    """Ask the serve runner to cancel the run (annotation). The runner does the cancelling, so a CLI and the
    runner never race on the same run. Raise `kube.NotFound`."""
    kube.merge_patch("benchmarkrun", name, {"metadata": {"annotations": {ANNOTATION_CANCEL: "true"}}})


def delete_run(kube: Kube, name: str) -> None:
    """Deleting cancels the run when tasks are unfinished (owner references clean up the Pods)."""
    kube.delete("benchmarkrun", name)
