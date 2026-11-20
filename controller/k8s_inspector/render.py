"""Render one finished task into the sc-inspector-data tree. Pure with respect to Kubernetes and S3: inputs
are passed in, output is files under a directory.

Owner: publisher agent. Implements docs/explanation/outputs.md "Publishing to git" step 3 and
docs/reference/output-contract.md "sc-inspector-data" + "meta.json". Ports sc-inspector `transform.py`
(`raw`, `fetch_geekbench_results` incl. the HTTP fallback), `parse.py` (dmidecode, openssl, stressnglongrun,
lstopo + svg styling), the `meta.json` writer (`lib.Meta`: naive UTC timestamps, no trailing newline) and
`select_schedule_meta` / `refresh_schedule_meta` (the `schedule/<task>/meta.json` union). Output must be
byte-identical to the legacy for the same inputs.
"""

from dataclasses import dataclass, field
from pathlib import Path

from k8s_inspector.model import RunKey, TaskDef


@dataclass
class RenderInput:
    """Everything the publisher read from object storage for one task."""

    run: RunKey
    task: TaskDef
    key: str  # output prefix (task name, or vllm-probe-<image>)
    status: dict  # status.json: the final meta.json content plus Kubernetes facts
    stdout: bytes = b""
    stderr: bytes = b""
    files: dict[str, bytes] = field(default_factory=dict)  # other objects of the task prefix by name


def meta_from_status(status: dict) -> dict:
    """The `meta.json` dict: unchanged legacy fields (`start, end, exit_code, error_msg, task_hash,
    version, kernel_version, stdout_bytes, stderr_bytes, outputs, start_retries, spot, region`) plus the
    added `run_id, image, zone, capacity_type, price, spot_attempts, node_os, kubelet, runtime,
    nodeclass_hash`."""
    raise NotImplementedError


def render_task(tree: Path, vendor: str, sku: str, variant: int, inp: RenderInput) -> list[Path]:
    """Write `data/<vendor>/<sku>/<variant>/<task>/` (meta.json, stdout/stderr via the `raw` transform --
    empty streams not written --, geekbench `results.json`, parsers' `parsed.*`/`lstopo.svg`) and refresh
    `schedule/<task>/meta.json`. Skip (return []) when the meta already in the tree has a newer `start`
    (stale duplicate). Returns the written paths."""
    raise NotImplementedError


def render_variant(tree: Path, vendor: str, sku: str, variant: int, lscpu_stdout: bytes) -> None:
    """Write `variant.json` when the variant dir has none (`variant.build_variant_json`, source `observed`)."""
    raise NotImplementedError


def render_timing(tree: Path, vendor: str, sku: str, variant: int, run: RunKey, timings: dict[str, str]) -> None:
    """`timing/<run_id>/<name>`: one ISO UTC line each (`api_start`, `api_end`, `machine_start`,
    `node_registered`, `node_ready`, `inspector_start`, `inspector_end`)."""
    raise NotImplementedError
