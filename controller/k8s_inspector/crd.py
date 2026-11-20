"""BenchmarkRun custom resource: naming, (de)serialisation, create/get/list/status helpers.

Owner: kube/crd agent. Implements docs/reference/objects.md "BenchmarkRun" and state.md "The lease".
Status writes from several threads (reconciler, publisher) go through `update_status`, which is
read-modify-write with `resourceVersion` and retries on `Conflict`, so no caller needs a lock.
"""

from collections.abc import Callable
from datetime import datetime

from k8s_inspector.kube import Kube
from k8s_inspector.model import BenchmarkRun, RunKey, RunSpec, RunStatus

API_VERSION = "inspector.sparecores.com/v1alpha1"
KIND = "BenchmarkRun"
LABEL_RUN = "inspector.sparecores.com/run"
LABEL_VENDOR = "inspector.sparecores.com/vendor"


class RunExists(Exception):
    """A BenchmarkRun for this SKU exists (the lease is held)."""


def run_name(vendor: str, sku: str) -> str:
    """`<vendor>-<sku slug>-<crc32 hex of "vendor/sku", 8 digits>`, a DNS-1123 name (max 63 chars, so the
    slug is truncated). Pure function of the SKU: `oci-vm-standard-e5-flex-2-16-3f9c1a7e`. Slug: lowercase,
    every run of non-alphanumerics becomes `-`."""
    raise NotImplementedError


def new_run_id(now: datetime) -> str:
    """`r<UTC yyyymmddThhmm>-<4 hex>` (output-contract.md)."""
    raise NotImplementedError


def run_key(run: BenchmarkRun) -> RunKey:
    """Object storage identity of the run: vendor, sku, creation date (UTC) of the object, run id."""
    raise NotImplementedError


def to_manifest(spec: RunSpec, namespace: str) -> dict:
    """The BenchmarkRun object to create: name, labels (vendor), spec in camelCase (`runId`, `holdAfter` as
    "2h"-style string)."""
    raise NotImplementedError


def from_dict(obj: dict) -> BenchmarkRun:
    """Parse an object returned by the API (tolerates a missing `status`)."""
    raise NotImplementedError


def status_to_dict(status: RunStatus) -> dict:
    """camelCase `status` as in objects.md, timestamps ISO-8601 UTC."""
    raise NotImplementedError


def create_run(kube: Kube, spec: RunSpec) -> BenchmarkRun:
    """Create with `status.phase=Pending`. Raise `RunExists` on `AlreadyExists`."""
    raise NotImplementedError


def get_run(kube: Kube, name: str) -> BenchmarkRun:
    """Raise `kube.NotFound`."""
    raise NotImplementedError


def list_runs(kube: Kube) -> list[BenchmarkRun]:
    raise NotImplementedError


def update_status(kube: Kube, name: str, mutate: Callable[[RunStatus], None]) -> BenchmarkRun:
    """GET, apply `mutate` to the parsed status in place, PATCH `status` with the object's resourceVersion;
    on `Conflict` start again (max 5 times). Also refreshes `progress` from `tasks` (finished/planned).
    Returns the updated run."""
    raise NotImplementedError


def delete_run(kube: Kube, name: str) -> None:
    """Deleting cancels the run when tasks are unfinished (owner references clean up the Pods)."""
    raise NotImplementedError
