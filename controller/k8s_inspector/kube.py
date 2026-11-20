"""Thin lightkube wrapper. All Kubernetes I/O of the controller goes through `Kube`, as plain dicts.

Owner: kube/crd agent. Other modules never import lightkube. Kinds are the lowercase strings in `KINDS`;
`"benchmarkrun"` is a lightkube generic resource of `inspector.sparecores.com/v1alpha1`, `"nodeclaim"` of
`karpenter.sh/v1`. Errors are translated to the exceptions below so callers do not import lightkube.

Threading: `Kube` is used from several threads (watch threads, the reconciler, planner, publisher);
lightkube's sync `Client` is safe for that. `watch` is a blocking generator: run it in its own thread.
"""

from collections.abc import Iterator
from dataclasses import dataclass

KINDS = ("pod", "secret", "configmap", "node", "nodeclaim", "event", "benchmarkrun")


class NotFound(Exception):
    """404 from the API server."""


class AlreadyExists(Exception):
    """409 AlreadyExists on create: for BenchmarkRun it is the lease being held."""


class Conflict(Exception):
    """409 Conflict (stale resourceVersion) on update/patch."""


@dataclass(frozen=True)
class WatchEvent:
    type: str  # ADDED | MODIFIED | DELETED | BOOKMARK
    kind: str
    obj: dict


class Kube:
    def __init__(self, namespace: str) -> None:
        """Create the lightkube `Client` (in-cluster config, or kubeconfig when run locally)."""
        raise NotImplementedError

    def get(self, kind: str, name: str) -> dict:
        """Namespaced kinds in `self.namespace`; node and nodeclaim are cluster-scoped. Raise `NotFound`."""
        raise NotImplementedError

    def list(self, kind: str, labels: dict[str, str] | None = None) -> list[dict]:
        raise NotImplementedError

    def create(self, kind: str, manifest: dict) -> dict:
        """Raise `AlreadyExists`. Returns the created object."""
        raise NotImplementedError

    def delete(self, kind: str, name: str, grace_seconds: int | None = None) -> None:
        """Ignore `NotFound`."""
        raise NotImplementedError

    def merge_patch(self, kind: str, name: str, body: dict, subresource_status: bool = False) -> dict:
        """JSON merge patch (`status` subresource when asked). Include `metadata.resourceVersion` in `body`
        for optimistic concurrency; raise `Conflict` when it is stale."""
        raise NotImplementedError

    def apply_secret(self, manifest: dict) -> None:
        """Create or replace a Secret (urls Secret, re-signed hourly)."""
        raise NotImplementedError

    def watch(
        self, kind: str, labels: dict[str, str] | None = None, resource_version: str | None = None
    ) -> Iterator[WatchEvent]:
        """Blocking event stream; ends on server timeout (caller re-LISTs and re-watches)."""
        raise NotImplementedError

    def exec_heartbeat(self, pod: str, directory: str) -> None:
        """`kubectl exec <anchor> -- /sc-run heartbeat <directory>` (between steps)."""
        raise NotImplementedError
