"""Thin lightkube wrapper. All Kubernetes I/O of the controller goes through `Kube`, as plain dicts.

Owner: kube/crd agent. Other modules never import lightkube. Kinds are the lowercase strings in `KINDS`;
`"benchmarkrun"` is a lightkube generic resource of `inspector.sparecores.com/v1alpha1`, `"nodeclaim"` of
`karpenter.sh/v1`. Errors are translated to the exceptions below so callers do not import lightkube.

Threading: `Kube` is used from several threads (watch threads, the reconciler, planner, publisher);
lightkube's sync `Client` is safe for that. `watch` is a blocking generator: run it in its own thread.
"""

import logging
import random
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import httpx2
import lightkube
from lightkube import ALL_NS, ApiError
from lightkube.core.client import CascadeType, PatchType
from lightkube.generic_resource import create_global_resource, create_namespaced_resource
from lightkube.resources import batch_v1, core_v1

log = logging.getLogger(__name__)

KINDS = ("pod", "secret", "configmap", "job", "node", "nodeclaim", "event", "benchmarkrun")

# Must match deploy/crd/benchmarkruns.yaml.
CRD_GROUP = "inspector.sparecores.com"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "benchmarkruns"

BenchmarkRunResource = create_namespaced_resource(CRD_GROUP, CRD_VERSION, "BenchmarkRun", CRD_PLURAL)
NodeClaimResource = create_global_resource("karpenter.sh", "v1", "NodeClaim", "nodeclaims")

# kind -> (lightkube resource class, namespaced)
_RESOURCES = {
    "pod": (core_v1.Pod, True),
    "secret": (core_v1.Secret, True),
    "configmap": (core_v1.ConfigMap, True),
    "job": (batch_v1.Job, True),
    "event": (core_v1.Event, True),
    "node": (core_v1.Node, False),
    "nodeclaim": (NodeClaimResource, False),
    "benchmarkrun": (BenchmarkRunResource, True),
}

WATCH_SERVER_TIMEOUT_SECONDS = 240
EXEC_TIMEOUT_SECONDS = 30


class NotFound(Exception):
    """404 from the API server."""


class Forbidden(Exception):
    """403 from the API server (RBAC)."""


class AlreadyExists(Exception):
    """409 AlreadyExists on create: for BenchmarkRun it is the lease being held."""


class Conflict(Exception):
    """409 Conflict (stale resourceVersion) on update/patch."""


@dataclass(frozen=True)
class WatchEvent:
    type: str  # ADDED | MODIFIED | DELETED | BOOKMARK
    kind: str
    obj: dict


def _translate(error: ApiError) -> Exception:
    """The controller's exception for an API error, or the error itself when it has no counterpart."""
    code = error.status.code
    if code == 404:
        return NotFound(error.status.message)
    if code == 403:
        return Forbidden(error.status.message)
    if code == 409:
        if error.status.reason == "AlreadyExists":
            return AlreadyExists(error.status.message)
        return Conflict(error.status.message)
    return error


def _is_gone(error: httpx2.HTTPStatusError) -> bool:
    """410 Gone: the watch's resourceVersion expired. It arrives as an `ApiError` (error event in the stream)
    or as a plain HTTP status error (refused request)."""
    if isinstance(error, ApiError):
        return error.status.code == 410
    return error.response.status_code == 410


BACKOFF_START_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0


def backoff_delay(failures: int, error: Exception | None = None) -> float:
    """Seconds to wait after the `failures`-th failure in a row (1, 2, 4 ... 60, with jitter). A 429 or 503
    with a numeric `Retry-After` header waits at least that long."""
    delay = min(BACKOFF_START_SECONDS * 2 ** max(failures - 1, 0), BACKOFF_MAX_SECONDS)
    delay *= random.uniform(0.75, 1.25)  # noqa: S311 - jitter, not security
    response = getattr(error, "response", None)
    retry_after = response.headers.get("Retry-After", "") if response is not None else ""
    if retry_after.isdigit():
        delay = max(delay, min(float(retry_after), BACKOFF_MAX_SECONDS))
    return delay


def retry_until_ok(what: str, action: Callable[[], None], stop: threading.Event) -> bool:
    """Call `action` until it returns without raising, waiting with `backoff_delay` between attempts and
    logging each failure as a warning (API errors while the cluster or the CRD settles are transient).
    False when `stop` was set first."""
    failures = 0
    while not stop.is_set():
        try:
            action()
            return True
        except Exception as error:
            failures += 1
            delay = backoff_delay(failures, error)
            log.warning("%s failed (%d in a row), retrying in %.0fs", what, failures, delay, exc_info=True)
            stop.wait(delay)
    return False


ONE_SHOT_ATTEMPTS = 5


def is_transient(error: Exception) -> bool:
    """429, 5xx, "storage is (re)initializing" (the API server right after a CRD update) and connection errors."""
    if isinstance(error, ApiError):
        # `status.code` is None when the body is not a Status (or has no code): fall back to the HTTP status.
        code = error.status.code or error.response.status_code
        return code == 429 or code >= 500 or "initializing" in str(error.status.message)
    return isinstance(error, httpx2.TransportError) and not isinstance(error, httpx2.UnsupportedProtocol)


class TransientApiError(Exception):
    """The Kubernetes API kept failing transiently through every attempt; `__cause__` is the last error."""

    def __init__(self, attempts: int) -> None:
        super().__init__(f"Kubernetes API unavailable after {attempts} attempts")
        self.attempts = attempts


def call_with_retries[T](
    what: str, action: Callable[[], T], attempts: int = ONE_SHOT_ATTEMPTS, sleep: Callable[[float], None] = time.sleep
) -> T:
    """`action()`, retried up to `attempts` times in total after a transient API error, waiting with
    `backoff_delay` between attempts. Other errors are raised as they are; after the last transient one
    `TransientApiError` is raised from it."""
    for failures in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:
            if not is_transient(error):
                raise
            if failures == attempts:
                raise TransientApiError(attempts) from error
            delay = backoff_delay(failures, error)
            log.warning("%s failed (%d of %d), retrying in %.0fs: %s", what, failures, attempts, delay, error)
            sleep(delay)
    raise AssertionError("unreachable")  # attempts < 1


class Kube:
    def __init__(self, namespace: str, client: lightkube.Client | None = None) -> None:
        """Create the lightkube `Client` (in-cluster config, or kubeconfig when run locally). `client` is for
        tests."""
        self.namespace = namespace
        self._client = client if client is not None else lightkube.Client(namespace=namespace)

    def _target(self, kind: str) -> tuple[type, dict]:
        """Resource class and the `namespace` keyword for it (none for cluster-scoped kinds)."""
        resource, namespaced = _RESOURCES[kind]
        return resource, ({"namespace": self.namespace} if namespaced else {})

    def get(self, kind: str, name: str) -> dict:
        """Namespaced kinds in `self.namespace`; node and nodeclaim are cluster-scoped. Raise `NotFound`."""
        resource, ns = self._target(kind)
        try:
            return self._client.get(resource, name, **ns).to_dict()
        except ApiError as e:
            raise _translate(e) from e

    def get_configmap_in(self, namespace: str, name: str) -> dict:
        """A ConfigMap of another namespace (SCarpenter's offered-types). Raise `NotFound`."""
        try:
            return self._client.get(core_v1.ConfigMap, name, namespace=namespace).to_dict()
        except ApiError as e:
            raise _translate(e) from e

    def list(self, kind: str, labels: dict[str, str] | None = None) -> list[dict]:
        resource, ns = self._target(kind)
        try:
            return [obj.to_dict() for obj in self._client.list(resource, labels=labels, **ns)]
        except ApiError as e:
            raise _translate(e) from e

    def list_pod_events(self, pod_name: str, uid: str) -> list[dict]:
        """Events of one Pod (by name and UID, so a same-named earlier Pod's are excluded) in `self.namespace`."""
        fields = {"involvedObject.name": pod_name, "involvedObject.kind": "Pod", "involvedObject.uid": uid}
        try:
            return [e.to_dict() for e in self._client.list(core_v1.Event, namespace=self.namespace, fields=fields)]
        except ApiError as e:
            raise _translate(e) from e

    def list_node_pods(self, node_name: str) -> list[dict]:
        """Every Pod of every namespace that is bound to the node (`spec.nodeName` field selector)."""
        try:
            pods = self._client.list(core_v1.Pod, namespace=ALL_NS, fields={"spec.nodeName": node_name})
            return [pod.to_dict() for pod in pods]
        except ApiError as e:
            raise _translate(e) from e

    def create(self, kind: str, manifest: dict) -> dict:
        """Raise `AlreadyExists`. Returns the created object."""
        resource, ns = self._target(kind)
        try:
            return self._client.create(resource.from_dict(manifest), **ns).to_dict()
        except ApiError as e:
            raise _translate(e) from e

    def delete(self, kind: str, name: str, grace_seconds: int | None = None) -> None:
        """Ignore `NotFound`. A Job is deleted in the background, so its Pods go too (the API default orphans them)."""
        resource, ns = self._target(kind)
        cascade = CascadeType.BACKGROUND if kind == "job" else None
        try:
            self._client.delete(resource, name, grace_period=grace_seconds, cascade=cascade, **ns)
        except ApiError as e:
            if e.status.code != 404:
                raise _translate(e) from e

    def merge_patch(self, kind: str, name: str, body: dict, subresource_status: bool = False) -> dict:
        """JSON merge patch (`status` subresource when asked). Include `metadata.resourceVersion` in `body`
        for optimistic concurrency; raise `Conflict` when it is stale."""
        resource, ns = self._target(kind)
        if subresource_status:
            resource = resource.Status
        try:
            return self._client.patch(resource, name, body, patch_type=PatchType.MERGE, **ns).to_dict()
        except ApiError as e:
            raise _translate(e) from e

    def apply_secret(self, manifest: dict) -> None:
        """Create or replace a Secret (urls Secret, re-signed hourly)."""
        secret = core_v1.Secret.from_dict(manifest)
        try:
            self._client.create(secret, namespace=self.namespace)
        except ApiError as e:
            if not isinstance(_translate(e), AlreadyExists):
                raise _translate(e) from e
            try:
                self._client.replace(secret, namespace=self.namespace)
            except ApiError as e2:
                raise _translate(e2) from e2

    def watch(
        self, kind: str, labels: dict[str, str] | None = None, resource_version: str | None = None
    ) -> Iterator[WatchEvent]:
        """Blocking event stream that does not end by itself: lightkube reconnects when the server closes the
        stream, and when the resourceVersion has expired (410 Gone) this re-LISTs and yields every current
        object as `ADDED` before watching on. Consumers must therefore treat `ADDED` as "object seen" (level
        triggered) and cannot rely on a `DELETED` for objects removed while the watch was gone. Without
        `resource_version` the stream starts with that LIST. Other API errors propagate."""
        resource, ns = self._target(kind)
        while True:
            if resource_version is None:
                listing = self._client.list(resource, labels=labels, **ns)
                for obj in listing:
                    yield WatchEvent("ADDED", kind, obj.to_dict())
                resource_version = listing.resourceVersion
            try:
                for event_type, obj in self._client.watch(
                    resource,
                    labels=labels,
                    server_timeout=WATCH_SERVER_TIMEOUT_SECONDS,
                    resource_version=resource_version,
                    **ns,
                ):
                    yield WatchEvent(event_type, kind, obj.to_dict())
            except httpx2.HTTPStatusError as e:
                if not _is_gone(e):
                    if isinstance(e, ApiError):
                        raise _translate(e) from e
                    raise
                resource_version = None  # expired: LIST again

    def exec_heartbeat(self, pod: str, directory: str) -> None:
        """`kubectl exec <anchor> -- /sc-run heartbeat <directory>` (between steps). Raise `NotFound` when the
        Pod is gone; a failing command or a connection problem raises lightkube's own exception."""
        try:
            self._client.exec(
                pod,
                namespace=self.namespace,
                command=["/sc-run", "heartbeat", directory],
                raise_on_error=True,
                timeout=EXEC_TIMEOUT_SECONDS,
            )
        except ApiError as e:
            raise _translate(e) from e
