"""In-memory stand-ins for `Kube` and `Storage`, and a small cluster simulator, for the runner tests.

`FakeKube` keeps plain dicts per kind with the semantics the controller relies on: `AlreadyExists`, JSON merge
patch with a `resourceVersion` check, owner-reference cascade on delete, label selectors (`None` = exists).
`Cluster` plays Karpenter and the kubelet: it provisions a Node for a pending anchor and finishes step Pods the
way the test scripted.
"""

import copy
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, pods
from k8s_inspector.kube import AlreadyExists, Conflict, Forbidden, NotFound, WatchEvent
from k8s_inspector.model import Manifest, ManifestObject, UploadPlan, Verification
from k8s_inspector.storage import LARGE_STANDARD_FILES, STANDARD_FILES, Storage, object_key, run_json_key


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


def iso(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _merge(target: dict, patch: dict) -> None:
    """RFC 7386 JSON merge patch."""
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


class FakeKube:
    namespace = "inspector"

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.objects: dict[str, dict[str, dict]] = {}
        self.heartbeats: list[tuple[str, str]] = []
        self.created: list[tuple[str, str]] = []  # (kind, name) in order
        self.deleted: list[tuple[str, str]] = []
        self.node_pods_forbidden = False  # `list_node_pods` raises Forbidden (no RBAC for the cluster-wide list)
        self.events_forbidden = False  # `list_pod_events` raises Forbidden
        self.foreign_pods: list[dict] = []  # Pods of other namespaces (kube-system ...), seen by `list_node_pods` only
        self.force_deleted: list[str] = []  # Pods deleted with grace period 0
        self.secret_writes = 0
        self._counter = 0

    def _store(self, kind: str) -> dict[str, dict]:
        return self.objects.setdefault(kind, {})

    def _bump(self, obj: dict) -> None:
        self._counter += 1
        obj["metadata"]["resourceVersion"] = str(self._counter)

    def get(self, kind: str, name: str) -> dict:
        try:
            return copy.deepcopy(self._store(kind)[name])
        except KeyError:
            raise NotFound(f"{kind} {name}") from None

    def list(self, kind: str, labels: dict | None = None) -> list[dict]:
        found = []
        for obj in self._store(kind).values():
            have = obj["metadata"].get("labels") or {}
            if all(k in have and (v is None or have[k] == v) for k, v in (labels or {}).items()):
                found.append(copy.deepcopy(obj))
        return found

    def list_pod_events(self, pod_name: str, uid: str) -> list[dict]:
        if self.events_forbidden:
            raise Forbidden("events")
        return [
            copy.deepcopy(e)
            for e in self._store("event").values()
            if e["involvedObject"]["name"] == pod_name and e["involvedObject"].get("uid") == uid
        ]

    def list_node_pods(self, node_name: str) -> list[dict]:
        """The Pods bound to the node: stored ones plus `foreign_pods` (other namespaces, written by tests)."""
        if self.node_pods_forbidden:
            raise Forbidden(node_name)
        pods = [*self._store("pod").values(), *self.foreign_pods]
        return [copy.deepcopy(p) for p in pods if p["spec"].get("nodeName") == node_name]

    def create(self, kind: str, manifest: dict) -> dict:
        obj = copy.deepcopy(manifest)
        name = obj["metadata"]["name"]
        if name in self._store(kind):
            raise AlreadyExists(name)
        meta = obj["metadata"]
        meta.setdefault("uid", f"uid-{kind}-{name}")
        meta.setdefault("creationTimestamp", iso(self.clock.now))
        if kind == "pod":
            obj.setdefault("status", {"phase": "Pending"})
        self._bump(obj)
        self._store(kind)[name] = obj
        self.created.append((kind, name))
        return copy.deepcopy(obj)

    def delete(self, kind: str, name: str, grace_seconds: int | None = None) -> None:
        obj = self._store(kind).pop(name, None)
        if obj is None:
            return
        if grace_seconds == 0:
            self.force_deleted.append(name)
        self.deleted.append((kind, name))
        for child_kind, children in self.objects.items():
            for child in list(children.values()):
                owners = child["metadata"].get("ownerReferences") or []
                if any(o["name"] == name and o["kind"].lower() == kind for o in owners):
                    self.delete(child_kind, child["metadata"]["name"])

    def merge_patch(self, kind: str, name: str, body: dict, subresource_status: bool = False) -> dict:
        obj = self._store(kind).get(name)
        if obj is None:
            raise NotFound(name)
        patch = copy.deepcopy(body)
        wanted = (patch.get("metadata") or {}).pop("resourceVersion", None)
        if wanted is not None and wanted != obj["metadata"]["resourceVersion"]:
            raise Conflict(name)
        _merge(obj, patch)
        self._bump(obj)
        return copy.deepcopy(obj)

    def apply_secret(self, manifest: dict) -> None:
        secret = copy.deepcopy(manifest)
        import base64

        secret["data"] = {k: base64.b64encode(v.encode()).decode() for k, v in secret.pop("stringData", {}).items()}
        secret["metadata"].setdefault("uid", "uid-secret")
        self._bump(secret)
        self._store("secret")[secret["metadata"]["name"]] = secret
        self.secret_writes += 1

    def exec_heartbeat(self, pod: str, directory: str) -> None:
        if pod not in self._store("pod"):
            raise NotFound(pod)
        self.heartbeats.append((pod, directory))

    def watch(self, kind, labels=None, resource_version=None):
        for obj in self.list(kind, labels):
            yield WatchEvent("ADDED", kind, obj)

    # ---- test helpers

    def set_status(self, kind: str, name: str, status: dict) -> None:
        obj = self._store(kind)[name]
        obj["status"] = copy.deepcopy(status)
        self._bump(obj)

    def patch_obj(self, kind: str, name: str, patch: dict) -> None:
        obj = self._store(kind)[name]
        _merge(obj, patch)
        self._bump(obj)

    def run(self, name: str):
        return crd.get_run(self, name)


class FakeStorage(Storage):
    """Everything in memory. `incomplete[key] = n`: the first n `finish_task` calls of the key report missing
    files; `lost`: the key never verifies."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.status: dict[str, dict] = {}
        self.run_json: dict | None = None
        self.incomplete: dict[str, int] = {}
        self.lost: set[str] = set()
        self.planned: list[str] = []  # keys passed to plan_uploads, in order
        self.salvaged: list[str] = []
        self.aborted: list[str] = []
        self.cleared: list[str] = []  # keys passed to clear_task, in order
        self.signed = 0
        self.crash_after_status_json = False  # the process dies right after the next status.json

    def plan_uploads(self, run, containers):
        plans = []
        for c in containers:
            self.planned.append(c.key)
            for name in STANDARD_FILES:
                plan = UploadPlan(urls_key=f"{c.key}/{name}", object_key=object_key(run, c.key, name))
                if name in LARGE_STANDARD_FILES and not c.task.parallel:
                    plan = replace(plan, upload_id=f"up-{c.key}-{name}-{len(self.planned)}", part_size=16, n_parts=2)
                plans.append(plan)
        return plans

    def presign(self, plans):
        self.signed += 1
        return [
            replace(
                p,
                put_url=f"https://s3.test/{p.object_key}?sig={self.signed}",
                part_urls=tuple(f"https://s3.test/{p.object_key}?part={n}&sig={self.signed}" for n in range(p.n_parts)),
            )
            for p in plans
        ]

    def read_bytes(self, okey):
        return self.objects.get(okey)

    def finish_task(self, run, key):
        if key in self.lost:
            return Verification(key, False, None, ("manifest.json missing",))
        if self.incomplete.get(key, 0) > 0:
            self.incomplete[key] -= 1
            return Verification(key, False, None, ("manifest.json missing",))
        manifest = Manifest(key, True, (ManifestObject("stdout", "put", 10), ManifestObject("stderr", "put", 5)))
        return Verification(key, True, manifest)

    def clear_task(self, run, key):
        self.cleared.append(key)

    def salvage(self, run, key, plans):
        self.salvaged.append(key)
        return Verification(key, True, None)

    def abort_open(self, run, key):
        self.aborted.append(key)
        return 0

    def write_status_json(self, run, key, doc):
        self.status[key] = json.loads(json.dumps(doc))  # must be JSON
        if self.crash_after_status_json:
            self.crash_after_status_json = False
            raise RuntimeError("controller crashed")

    def write_run_json(self, run, doc):
        self.run_json = json.loads(json.dumps(doc))
        self.objects[run_json_key(run)] = json.dumps(doc).encode()


class Cluster:
    """Karpenter and the kubelet. `behaviour[key]` is a list of scripted results for that container key, used
    in order (default: success). Result dict keys: `exit_code`, `oom_kills`, `timed_out`, `cancelled`,
    `start_error`, `pull_stuck` (the Pod stays Pending in ErrImagePull, or in the waiting reason given as a string,
    with `message`), `btime`, `wedged` (the Pod stays Running: containerd does not report the exit), `rejected` (the
    kubelet refuses the Pod: DiskPressure)."""

    def __init__(self, kube: FakeKube, clock: Clock) -> None:
        self.kube, self.clock = kube, clock
        self.behaviour: dict[str, list[dict]] = {}
        self.capacity = "spot"
        self.node_labels: dict[str, str] = {}
        self.node_allocatable: dict[str, str] = {"memory": "15Gi", "cpu": "2"}
        self.claim_annotations: dict[str, str] = {
            "scarpenter.sparecores.com/price": "0.0337",
            "scarpenter.sparecores.com/nodeclass-hash": "h1",
        }
        self.nodes = 0
        self.claim_conditions: list[dict] | None = None  # override: claim with these conditions, no node
        self.provision_enabled = True

    def node_name(self, anchor: dict) -> str:
        return f"node-{anchor['metadata']['name']}"

    def provision(self, anchor: dict, *, node_created: datetime | None = None) -> None:
        name = self.node_name(anchor)
        when = node_created or self.clock.now + timedelta(seconds=30)
        selector = anchor["spec"]["nodeSelector"]
        vendor, sku = selector["scarpenter.sparecores.com/vendor"], selector["scarpenter.sparecores.com/api-reference"]
        labels = {
            "node.kubernetes.io/instance-type": f"{vendor}.{sku}",
            "scarpenter.sparecores.com/vendor": vendor,
            "scarpenter.sparecores.com/api-reference": sku,
            "karpenter.sh/nodepool": selector["karpenter.sh/nodepool"],
            "karpenter.sh/capacity-type": self.capacity,
            "topology.kubernetes.io/region": "eu-test-1",
            "topology.kubernetes.io/zone": "eu-test-1a",
            "kubernetes.io/arch": "amd64",
            **self.node_labels,
        }
        self.kube.create(
            "node",
            {
                "metadata": {"name": name, "labels": labels, "creationTimestamp": iso(when)},
                "status": {
                    "allocatable": self.node_allocatable,
                    "capacity": {"cpu": "2", "memory": "16Gi"},
                    "nodeInfo": {
                        "osImage": "Flatcar 4000",
                        "kubeletVersion": "v1.34.0",
                        "containerRuntimeVersion": "containerd://2.1.0",
                    },
                },
            },
        )
        conditions = [
            {"type": t, "status": "True", "lastTransitionTime": iso(when + timedelta(seconds=i * 10))}
            for i, t in enumerate(("Launched", "Registered", "Initialized"))
        ]
        self.kube.create(
            "nodeclaim",
            {
                "metadata": {
                    "name": f"claim-{name}",
                    "labels": {k: v for k, v in labels.items() if k in selector},
                    "annotations": dict(self.claim_annotations),
                    "creationTimestamp": iso(when),
                },
                "status": {"nodeName": name, "conditions": conditions},
            },
        )
        self.kube.patch_obj("pod", anchor["metadata"]["name"], {"spec": {"nodeName": name}})
        self.kube.set_status(
            "pod",
            anchor["metadata"]["name"],
            {
                "phase": "Running",
                "startTime": iso(self.clock.now),
                "containerStatuses": [{"name": "hold", "ready": True}],
            },
        )

    def fail_launch(
        self, anchor: dict, message: str, name: str | None = None, status: str = "False", reason: str | None = None
    ) -> None:
        """A NodeClaim whose `Launched` condition failed (Karpenter's `LaunchFailed` is status Unknown)."""
        selector = anchor["spec"]["nodeSelector"]
        self.kube.create(
            "nodeclaim",
            {
                "metadata": {
                    "name": name or f"claim-{anchor['metadata']['name']}",
                    "labels": {
                        k: selector[k]
                        for k in (
                            "karpenter.sh/nodepool",
                            "scarpenter.sparecores.com/vendor",
                            "scarpenter.sparecores.com/api-reference",
                        )
                    },
                    "creationTimestamp": iso(self.clock.now + timedelta(seconds=1)),
                },
                "status": {
                    "conditions": [
                        {"type": "Launched", "status": status, "message": message}
                        | ({"reason": reason} if reason else {})
                    ]
                },
            },
        )

    @staticmethod
    def container_keys(pod: dict) -> list[tuple[str, str]]:
        """(container name, task key) of a step Pod or flush Pod."""
        result = []
        for c in pod["spec"]["containers"]:
            args = c.get("command") or c.get("args")
            result.append((c["name"], args[args.index("--task") + 1]))
        return result

    def _terminated(self, key: str, spec: dict) -> dict:
        if spec.get("start_error"):
            return {"state": {"terminated": {"exitCode": 128, "reason": "StartError", "message": "no nvidia runtime"}}}
        message = {
            "task": key,
            "exit_code": spec.get("exit_code", 0),
            "start": iso(self.clock.now),
            "end": iso(self.clock.now + timedelta(minutes=1)),
            "timed_out": spec.get("timed_out", False),
            "cancelled": spec.get("cancelled", False),
            "oom_kills": spec.get("oom_kills", 0),
            "nice": spec.get("nice", -20),
            "upload_complete": True,
            "missing": 0,
            "btime": spec.get("btime", int(self.clock.now.timestamp()) - 120),
        }
        return {"state": {"terminated": {"exitCode": 0, "message": json.dumps(message)}}}

    def _stay(self, pod: dict, spec: dict) -> bool:
        name = pod["metadata"]["name"]
        if spec.get("out_of_cpu"):
            status = {
                "phase": "Failed",
                "reason": "OutOfcpu",
                "message": "Pod was rejected: Node didn't have enough resource: cpu, requested: 3000, used: 1",
            }
        elif spec.get("rejected"):
            status = {
                "phase": "Failed",
                "reason": "Evicted",
                "message": "Pod was rejected: The node had condition: [DiskPressure].",
            }
        else:
            statuses = [{"name": n, "state": {"running": {}}} for n, _ in self.container_keys(pod)]
            status = {"phase": "Running", "startTime": iso(self.clock.now), "containerStatuses": statuses}
        self.kube.set_status("pod", name, status)
        return True

    def finish(self, pod: dict) -> bool:
        """Script the end of one step or flush Pod. False when it stays Pending (`pull_stuck`)."""
        statuses, stuck = [], False
        for name, key in self.container_keys(pod):
            queue = self.behaviour.get(key, [])
            spec = queue.pop(0) if queue else {}
            if spec.get("wedged") or spec.get("rejected") or spec.get("out_of_cpu"):
                return self._stay(pod, spec)
            if spec.get("pull_stuck"):
                stuck = True
                waiting = {"reason": spec["pull_stuck"] if isinstance(spec["pull_stuck"], str) else "ErrImagePull"}
                if "message" in spec:
                    waiting["message"] = spec["message"]
                statuses.append({"name": name, "state": {"waiting": waiting}})
            else:
                statuses.append({"name": name, **self._terminated(key, spec)})
        if stuck:
            status = {"phase": "Pending", "startTime": iso(self.clock.now), "containerStatuses": statuses}
            self.kube.set_status("pod", pod["metadata"]["name"], status)
            return False
        self.kube.set_status("pod", pod["metadata"]["name"], {"phase": "Succeeded", "containerStatuses": statuses})
        return True

    def react(self) -> bool:
        """One round of cluster activity; True when something changed."""
        changed = False
        for pod in self.kube.list("pod"):
            meta = pod["metadata"]
            role = meta["labels"]["inspector.sparecores.com/role"]
            if pod["status"].get("phase") != "Pending" or pod["status"].get("containerStatuses"):
                continue
            if role == "anchor" and self.provision_enabled:
                self.provision(pod)
                changed = True
            elif role in ("step", "flush"):
                changed |= self.finish(pod)
        return changed


def anchor_name(run_name: str, attempt: int) -> str:
    return pods.anchor_name(run_name, attempt)
