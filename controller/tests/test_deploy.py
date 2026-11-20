"""The CRD schema in deploy/ must know every field the controller writes."""

from datetime import UTC, datetime
from pathlib import Path

import yaml

from k8s_inspector import crd
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    CapacityType,
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
        attempt=1,
        capacity_type=CapacityType.SPOT,
        node="n",
        region="r",
        zone="z",
        price_per_hour="0.1",
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
