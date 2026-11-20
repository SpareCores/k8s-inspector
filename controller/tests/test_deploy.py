"""The CRD schema in deploy/ must know every field the controller writes."""

from datetime import UTC, datetime
from pathlib import Path

import yaml

from k8s_inspector import crd
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    CapacityType,
    LaunchFailure,
    RunPhase,
    RunSpec,
    RunStatus,
    TaskState,
    TaskStatus,
)

CRD_PATH = Path(__file__).parents[2] / "deploy" / "crd" / "benchmarkruns.yaml"
NOW = datetime(2026, 11, 20, tzinfo=UTC)


def schema() -> dict:
    version = yaml.safe_load(CRD_PATH.read_text())["spec"]["versions"][0]
    return version["schema"]["openAPIV3Schema"]["properties"]


def unknown_keys(value: object, node: dict, path: str) -> list[str]:
    """Paths of dict keys in `value` that the schema `node` does not declare."""
    if isinstance(value, dict) and "properties" in node:
        found = []
        for key, item in value.items():
            if key not in node["properties"]:
                found.append(f"{path}.{key}")
            else:
                found += unknown_keys(item, node["properties"][key], f"{path}.{key}")
        return found
    if isinstance(value, list) and "items" in node:
        return [p for item in value for p in unknown_keys(item, node["items"], f"{path}[]")]
    return []


def test_every_status_key_the_controller_writes_is_in_the_crd_schema():
    status = RunStatus(
        phase=RunPhase.RUNNING,
        current_step="01-probes",
        step={
            "index": 1,
            "name": "01-probes",
            "gpuRuntime": True,
            "containers": [
                {"key": "k", "image": "i", "command": "c", "timeoutSeconds": 5, "env": {"A": "b"}},
            ],
        },
        attempt=1,
        capacity_type=CapacityType.SPOT,
        node="n",
        region="r",
        zone="z",
        price_per_hour="0.1",
        price_incomplete=True,
        variant=0,
        tasks=[
            TaskStatus(
                name="t",
                state=TaskState.DONE,
                attempt=1,
                start=NOW,
                end=NOW,
                exit_code=0,
                error_msg="e",
                published=True,
                commit="c",
                gpu_fallback=True,
            )
        ],
        attempts=[
            AttemptStatus(
                n=1,
                capacity_type=CapacityType.SPOT,
                node_claim="nc",
                node="n",
                start=NOW,
                end=NOW,
                outcome=AttemptOutcome.COMPLETED,
            )
        ],
        launch_failures=[LaunchFailure(node_claim="nc", message="m", time=NOW)],
        conditions=[{"type": "Published", "status": "True"}],
    )
    emitted = crd.status_to_dict(status)
    assert unknown_keys(emitted, schema()["status"], "status") == []
    assert emitted["tasks"][0].keys() >= {"errorMsg", "gpuFallback"}  # the sample really sets every optional key
    assert "node" in emitted["attempts"][0]


def test_every_spec_key_the_controller_writes_is_in_the_crd_schema():
    spec = RunSpec(vendor="v", sku="s", run_id="r", database={"vendor": "v", "id": "i", "engineVersion": "1"})
    manifest = crd.to_manifest(spec, "inspector")
    assert unknown_keys(manifest["spec"], schema()["spec"], "spec") == []


def test_rbac_is_least_privilege_for_what_the_controller_calls():
    docs = yaml.safe_load_all((CRD_PATH.parents[1] / "controller" / "rbac.yaml").read_text())
    granted = {
        (rule["apiGroups"][0], resource): set(rule["verbs"])
        for doc in docs
        if doc["kind"] == "Role" and doc["metadata"]["namespace"] == "inspector"
        for rule in doc["rules"]
        for resource in rule["resources"]
    }
    assert granted[("", "pods/exec")] == {"get", "create"}
    assert granted[("", "configmaps")] == {"get", "create", "patch"}  # spot.py merge-patches the history
    assert granted[("", "secrets")] == {"create", "get", "update", "delete"}  # no list/watch of other Secrets
    assert granted[("", "pods")] == {"create", "list", "watch", "delete"}
    assert granted[("inspector.sparecores.com", "benchmarkruns/status")] == {"patch"}
    cluster = {
        (rule["apiGroups"][0], resource): set(rule["verbs"])
        for doc in yaml.safe_load_all((CRD_PATH.parents[1] / "controller" / "rbac.yaml").read_text())
        if doc["kind"] == "ClusterRole"
        for rule in doc["rules"]
        for resource in rule["resources"]
    }
    assert cluster[("", "pods")] == {"list"}  # the Pods on the benchmark node (cpu requests), any namespace
    assert cluster[("", "nodes")] == {"get"}
    assert granted[("", "events")] == {"list"}  # Karpenter FailedScheduling event of the anchor
    assert ("batch", "jobs") not in granted


def test_rbac_reads_scarpenter_offered_types_in_its_namespace():
    docs = list(yaml.safe_load_all((CRD_PATH.parents[1] / "controller" / "rbac.yaml").read_text()))
    (role,) = [d for d in docs if d["kind"] == "Role" and d["metadata"]["namespace"] == "scarpenter-system"]
    assert [(r["resources"], r["verbs"]) for r in role["rules"]] == [(["configmaps"], ["get"])]
    (binding,) = [d for d in docs if d["kind"] == "RoleBinding" and d["metadata"]["namespace"] == "scarpenter-system"]
    assert binding["roleRef"]["name"] == role["metadata"]["name"]
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "k8s-inspector", "namespace": "inspector"}]
