"""Render one finished task into the sc-inspector-data tree. Pure with respect to Kubernetes and S3: inputs
are passed in, output is files under a directory.

Implements docs/explanation/outputs.md "Publishing to git" step 3 and docs/reference/output-contract.md
"sc-inspector-data" + "meta.json". Ports sc-inspector `transform.py` (`raw`, `fetch_geekbench_results` incl. the
HTTP fallback), `parse.py` (dmidecode, openssl, stressnglongrun, lstopo + svg styling), the `meta.json` writer
(`lib.Meta`: naive UTC timestamps, compact JSON, no trailing newline) and `select_schedule_meta` /
`refresh_schedule_meta` (the `schedule/<task>/meta.json` union). Output is byte-identical to the legacy for the
same inputs (see tests/test_render.py, golden files under tests/fixtures/publish/).

Deviations from the legacy, on purpose:
- Transforms and parsers run only for tasks with `exit_code == 0` (and streams are written for any exit code except
  256, the harness error); the legacy ran the parsers in a separate daily workflow and crashed on bad output.
- `-4`/`-5` go to `schedule/<task>/meta.json` only, without outputs; a failure never replaces a good schedule meta;
  `stdout_bytes`/`stderr_bytes` are the sizes of the files written; files of the previous result that the new one
  does not write are removed; a command that did not start (`256 cannot start command`) is `-1`; `error_msg` is
  redacted (`redact_error_msg`).
- `last_attempt` in `schedule/<task>/meta.json` records the latest attempt of any outcome when the meta is not that
  attempt (the planner decides retries and variant fills from it).
- `start_retries` and the `-5` rule are applied here (the runner does not know git): see `_apply_start_retries`.
- Nothing is written when a transform or parser raises: all content is computed before the first write.
- Optional parser dependencies (`dmiparser`, `dateparser`, `fastnumbers`, `lxml`) are imported lazily, only for the
  tasks that need them; `lstopo` needs the hwloc `lstopo` binary on PATH.
"""

import copy
import csv
import io
import json
import logging
import re
import subprocess
import tempfile
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from k8s_inspector.model import (
    AttemptStatus,
    BenchmarkRun,
    CapacityType,
    ExitCode,
    NodeFacts,
    Reason,
    RunKey,
    TaskDef,
    TaskOutcome,
    TaskStatus,
    Verification,
)

NOT_FINAL_CODES = (ExitCode.NEVER_FINISHED, ExitCode.RETRIES_EXHAUSTED)  # never finished: no variant dir, no outputs
MAX_START_RETRIES = 3  # three `-4` in a row become `-5` (exit-codes.md)
START_FAILURE_PREFIX = "cannot start command:"  # sc-run's error_msg when the task's command does not start
ERROR_MSG_MAX = 200  # published error_msg length; a longer one keeps its start and end

LEGACY_META_FIELDS = (
    "start",
    "end",
    "exit_code",
    "error_msg",
    "task_hash",
    "version",
    "kernel_version",
    "stdout_bytes",
    "stderr_bytes",
    "outputs",
    "start_retries",
    "spot",
    "region",
)
ADDED_META_FIELDS = (
    "run_id",
    "image",
    "zone",
    "capacity_type",
    "price",
    "spot_attempts",
    "node_os",
    "kubelet",
    "runtime",
    "nodeclass_hash",
    "nice",
)
_META_DEFAULTS = {"outputs": [], "start_retries": 0}


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


log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- status.json <-> meta.json


def _iso(when: datetime | None) -> str | None:
    """Naive UTC ISO string as the legacy `Meta` writes it (microseconds only when non-zero)."""
    if when is None:
        return None
    if when.tzinfo is not None:
        when = when.astimezone(UTC).replace(tzinfo=None)
    return when.isoformat()


def _parse_dt(text: str | None) -> datetime | None:
    return None if not text else datetime.fromisoformat(text).replace(tzinfo=None)


def format_timing_utc(when: datetime) -> str:
    """Timing file content: ISO-8601 UTC with `Z`, whole seconds (legacy `lib.format_timing_utc`)."""
    when = when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)
    return when.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_status(
    run: BenchmarkRun,
    task: TaskStatus,
    *,
    task_hash: str,
    outcome: TaskOutcome | None = None,
    verification: Verification | None = None,
    node: NodeFacts | None = None,
    attempt: AttemptStatus | None = None,
    image: str | None = None,
    pod: str | None = None,
) -> dict:
    """The `status.json` dict of a finished task (the runner calls this, then `Storage.write_status_json`).

    "Final meta.json content plus Kubernetes facts". Timestamps are naive UTC ISO strings, as in meta.json.
    Keys, in this order:

    meta.json fields (`meta_from_status` copies them back):
      `start, end, exit_code, error_msg` (from `task`), `task_hash` (the caller computes `tasks.task_hash`),
      `version, kernel_version` (from `outcome`, else null), `stdout_bytes, stderr_bytes` (sizes of the `stdout`
      and `stderr` objects in the verified manifest, else null), `start_retries` (always 0 here: the publisher
      counts from git), `spot` (`capacity_type == "spot"`, null when unknown), `region`
      (`node.region`, else `run.status.region`), `run_id` (`run.spec.run_id`), `image` (Pod image reference with
      digest), `zone`, `capacity_type` (`spot` | `on-demand`), `price` (hourly price string), `spot_attempts`
      (spot attempts up to and including this task's attempt), `node_os`, `kubelet`, `runtime`, `nodeclass_hash`
      (from `node`), `nice` (from `outcome`, else null).
    Kubernetes facts (ignored by `meta_from_status`):
      `vendor, sku, key` (the output prefix = `task.name`), `pod`, `node` (node name), `node_claim`, `attempt`
      (attempt number), `reason` (`error_msg` when it is a `model.Reason` value, else null), `gpu_fallback`,
      `upload_ok` (`Storage.finish_task` verdict, null without one), `upload_problems` (its problem strings).

    `outputs` is not part of status.json: the publisher derives it while rendering.
    """
    status = run.status
    attempt_n = attempt.n if attempt else task.attempt
    capacity = (attempt.capacity_type if attempt else None) or (node.capacity_type if node else None)
    capacity = capacity or status.capacity_type
    manifest_sizes = (
        {o.name: o.size for o in verification.manifest.objects} if verification and verification.manifest else {}
    )
    spot_attempts = sum(
        1 for a in status.attempts if a.capacity_type == CapacityType.SPOT and (attempt_n is None or a.n <= attempt_n)
    )
    return {
        "start": _iso(task.start),
        "end": _iso(task.end),
        "exit_code": task.exit_code,
        "error_msg": task.error_msg,
        "task_hash": task_hash,
        "version": outcome.version if outcome else None,
        "kernel_version": outcome.kernel_version if outcome else None,
        "stdout_bytes": manifest_sizes.get("stdout"),
        "stderr_bytes": manifest_sizes.get("stderr"),
        "start_retries": 0,
        "spot": None if capacity is None else capacity == CapacityType.SPOT,
        "region": (node.region if node else None) or status.region,
        "run_id": run.spec.run_id,
        "image": image,
        "zone": (node.zone if node else None) or status.zone,
        "capacity_type": None if capacity is None else str(capacity),
        "price": (node.price_per_hour if node else None) or status.price_per_hour,
        "spot_attempts": spot_attempts,
        "node_os": node.node_os if node else None,
        "kubelet": node.kubelet if node else None,
        "runtime": node.runtime if node else None,
        "nodeclass_hash": node.nodeclass_hash if node else None,
        "nice": outcome.nice if outcome else None,
        "vendor": run.spec.vendor,
        "sku": run.spec.sku,
        "key": task.name,
        "pod": pod,
        "node": (node.node_name if node else None) or (attempt.node if attempt else None) or status.node,
        "node_claim": (attempt.node_claim if attempt else None) or (node.node_claim if node else None),
        "attempt": attempt_n,
        "reason": task.error_msg if task.error_msg in set(Reason) else None,
        "gpu_fallback": task.gpu_fallback,
        "upload_ok": verification.ok if verification else None,
        "upload_problems": list(verification.problems) if verification else [],
    }


def _canonical_meta(meta: dict, *, with_added: bool) -> dict:
    """Legacy fields in legacy order (defaults like the legacy `Meta`), then the added fields."""
    out = {name: meta.get(name, _META_DEFAULTS.get(name)) for name in LEGACY_META_FIELDS}
    if with_added:
        out.update({name: meta.get(name) for name in ADDED_META_FIELDS})
    else:
        out.update({name: meta[name] for name in ADDED_META_FIELDS if name in meta})
    if out["outputs"] is None:
        out["outputs"] = []
    return out


def meta_from_status(status: dict) -> dict:
    """The `meta.json` dict: unchanged legacy fields (`start, end, exit_code, error_msg, task_hash,
    version, kernel_version, stdout_bytes, stderr_bytes, outputs, start_retries, spot, region`) plus the
    added `run_id, image, zone, capacity_type, price, spot_attempts, node_os, kubelet, runtime,
    nodeclass_hash, nice`. Everything else in `status` (the Kubernetes facts, see `build_status`) is dropped; a
    missing key is null (`outputs` []; `start_retries` 0)."""
    return _canonical_meta(status, with_added=True)


_URL = re.compile(r"https?://[^\s\"'<>]+")
_ID = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"  # UUID: subscription, tenant, request ID
    r"|\bocid1\.[a-z0-9_.-]+"  # OCI OCID
    r"|\barn:aws[a-z-]*:\S+"  # AWS ARN (carries the account number)
    r"|/subscriptions/[^\s\"']+",  # Azure resource ID quoted without a URL
    re.IGNORECASE,
)
_GCP_PROJECT = re.compile(r"\bprojects/[a-z][-a-z0-9]+")
_ACCOUNT = re.compile(
    r"(\baccount\W{0,3})\d{12}\b", re.IGNORECASE
)  # AWS account number; other 12-digit numbers are sizes


def redact_error_msg(message: str | None, limit: int = ERROR_MSG_MAX) -> str | None:
    """`error_msg` as published to the public repo: URLs become `<url>`; UUIDs, OCIDs, ARNs, Azure resource IDs,
    GCP project paths and AWS account numbers (after "account") become `<id>`. A message longer than `limit`
    (separator included) keeps its start (the reason code) and its end."""
    if not message:
        return message
    message = _ID.sub("<id>", _URL.sub("<url>", message))
    message = _ACCOUNT.sub(r"\1<id>", _GCP_PROJECT.sub("projects/<id>", message))
    if len(message) > limit:
        sep = " ... "
        head = (limit - len(sep)) // 2
        message = f"{message[:head]}{sep}{message[-(limit - len(sep) - head) :]}"
    return message


def published_meta(status: dict) -> dict:
    """`meta_from_status` for git: a command that did not start (sc-run's `256 cannot start command: ...`) is a
    `-1` (legacy semantics: the task could not run, the planner retries it), and `error_msg` is redacted."""
    meta = meta_from_status(status)
    if meta["exit_code"] == ExitCode.HARNESS_ERROR and str(meta["error_msg"]).startswith(START_FAILURE_PREFIX):
        meta["exit_code"] = int(ExitCode.NO_NODE)
    meta["error_msg"] = redact_error_msg(meta["error_msg"])
    return meta


def dump_meta(meta: dict) -> str:
    """meta.json text: compact JSON as pydantic's `model_dump_json` writes it, no trailing newline."""
    out = _canonical_meta(meta, with_added=False)
    if "last_attempt" in meta:  # schedule metas only
        out["last_attempt"] = meta["last_attempt"]
    return json.dumps(out, separators=(",", ":"), ensure_ascii=False)


# --------------------------------------------------------------------------- transforms


# Benchmark names of sc-crawler's `lookup.benchmarks` for framework geekbench (without "Score").
GEEKBENCH_BENCHMARKS = [
    "File Compression",
    "Navigation",
    "HTML5 Browser",
    "PDF Renderer",
    "Photo Library",
    "Clang",
    "Text Processing",
    "Asset Compression",
    "Object Detection",
    "Background Blur",
    "Horizon Detection",
    "Object Remover",
    "HDR",
    "Photo Filter",
    "Ray Tracer",
    "Structure from Motion",
]
GEEKBENCH_TABLES = ["Single-Core Performance", "Multi-Core Performance"]
GEEKBENCH_SECTION_NAMES = {"Single-Core": "Single-Core Performance", "Multi-Core": "Multi-Core Performance"}
GEEKBENCH_UPLOAD_MARKERS = ("GEEKBENCH_UPLOAD_DOCUMENT\n", "\nGEEKBENCH_UPLOAD_DOCUMENT_END")
_UNIT_SUFFIX = {
    11: ("pairs/sec", 1e6, 1),
    12: ("FPS", 1, 1),
    13: ("TE/sec", 1e6, 2),
    15: ("rows/sec", 1e3, 1),
    16: ("Elements/sec", 1e6, 2),
    17: ("Words/sec", 1, 1),
    20: ("images/sec", 1, 1),
    23: ("lines/sec", 1e3, 2),
    25: ("pages/sec", 1, 1),
    26: ("routes/sec", 1, 1),
}


def _format_workload_rate(rate: float, units: int) -> str:
    if units == 2:
        if rate >= 1024**3:
            return f"{rate / 1024**3:.2f} GB/sec"
        if rate >= 1024**2:
            return f"{rate / 1024**2:.1f} MB/sec"
        if rate >= 1024:
            return f"{rate / 1024:.1f} KB/sec"
        return f"{rate:.0f} B/sec"
    if units == 9:
        if rate >= 1e9:
            return f"{rate / 1e9:.2f} Gpixels/sec"
        if rate >= 1e6:
            return f"{rate / 1e6:.1f} Mpixels/sec"
        if rate >= 1e3:
            return f"{rate / 1e3:.1f} Kpixels/sec"
        return f"{rate:.0f} pixels/sec"
    suffix, scale, precision = _UNIT_SUFFIX.get(units, ("units/sec", 1, 1))
    scaled = rate / scale
    if scale > 1:
        prefix = {1e9: "G", 1e6: "M", 1e3: "K"}.get(scale, "")
        suffix = suffix.replace("pairs/sec", f"{prefix}pairs/sec")
        suffix = suffix.replace("rows/sec", f"{prefix}rows/sec")
        suffix = suffix.replace("Elements/sec", f"{prefix}Elements/sec")
        suffix = suffix.replace("lines/sec", f"{prefix}lines/sec")
        suffix = suffix.replace("TE/sec", f"{prefix}TE/sec")
    return f"{scaled:.{precision}f} {suffix}"


def _geekbench_upload_document_to_json(document: dict) -> dict:
    results = {}
    for section in document["sections"]:
        section_name = GEEKBENCH_SECTION_NAMES.get(section["name"], section["name"])
        results[section_name] = {"Score": {"score": int(section["score"])}}
        for workload in section["workloads"]:
            entry = {"score": int(workload["score"])}
            if "rate" in workload and "units" in workload:
                entry["description"] = _format_workload_rate(float(workload["rate"]), int(workload["units"]))
            results[section_name][workload["name"]] = entry
    return results


def _geekbench_upload_document_from_stderr(stderr: bytes) -> dict | None:
    text = stderr.decode("utf-8")
    start_marker, end_marker = GEEKBENCH_UPLOAD_MARKERS
    if start_marker not in text:
        return None
    start = text.index(start_marker) + len(start_marker)
    end = text.index(end_marker, start)
    return json.loads(text[start:end])


def _geekbench_html_to_json(html_content: str) -> dict:
    from lxml import html

    doc = html.fromstring(html_content)
    results = {}
    for table_name in GEEKBENCH_TABLES:
        table = doc.xpath(
            f'.//div[@class="heading"]/h3[text()="{table_name}"]/../following-sibling::div[@class="table-wrapper"][1]/table'
        )[0]
        results[table_name] = {"Score": {"score": int(table.xpath('.//th[@class="score"]/text()')[0].strip())}}
        for name in GEEKBENCH_BENCHMARKS:
            cell = table.xpath(f'.//td[@class="name" and normalize-space(string())="{name}"]/../td[@class="score"]')[0]
            results[table_name][name] = {
                "score": int(cell.xpath("text()")[0].strip()),
                "description": cell.xpath('span[@class="description"]/text()')[0],
            }
    return results


def _fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310 (URL comes from the benchmark output)
        return response.read().decode("utf-8")


def geekbench_results(stdout: bytes, stderr: bytes) -> bytes | None:
    """Transform `fetch_geekbench_results`: `results.json` content, or None when there is nothing to fetch.
    Prefers the upload document in stderr, else downloads the result page named in stdout (raises on failure)."""
    document = _geekbench_upload_document_from_stderr(stderr)
    if document:
        results = _geekbench_upload_document_to_json(document)
    else:
        urls = re.findall(re.compile(r'https://[^\s"]*/v6/cpu[^\s"]*'), stdout.decode("utf-8"))
        if not urls:
            return None
        results = _geekbench_html_to_json(_fetch_text(urls[0]))
    return json.dumps(results, indent=2).encode()


# --------------------------------------------------------------------------- parsers


def _si_prefixes(binary: bool = False) -> dict[str, float]:
    base = 1024 if binary else 1000
    return {
        "Q": base**10,
        "Qi": 1024**10,
        "R": base**9,
        "Ri": 1024**9,
        "Y": base**8,
        "Yi": 1024**8,
        "Z": base**7,
        "Zi": 1024**7,
        "E": base**6,
        "Ei": 1024**6,
        "P": base**5,
        "Pi": 1024**5,
        "T": base**4,
        "Ti": 1024**4,
        "G": base**3,
        "Gi": 1024**3,
        "M": base**2,
        "Mi": 1024**2,
        "k": base,
        "ki": 1024,
        "K": base,  # non-standard
        "Ki": 1024,  # non-standard
        "h": 100,
        "da": 10,
        "": 1,
        "d": 10**-1,
        "c": 10**-2,
        "m": 10**-3,
        "μ": 10**-6,
        "n": 10**-9,
        "p": 10**-12,
        "f": 10**-15,
        "a": 10**-18,
        "z": 10**-21,
        "y": 10**-24,
        "r": 10**-27,
        "q": 10**-30,
    }


_UNITS = ["bits", "Hz", "B", "V", "T/s", "s"]  # B = bytes, V = volts, s = seconds


def _multipliers(binary: bool = False) -> dict[str, tuple[float, str]]:
    res = {}
    for unit in _UNITS:
        si_prefix = _si_prefixes()
        if unit == "B" and binary:
            # use binary multipliers for bytes if asked to (so kB == kiB)
            si_prefix = _si_prefixes(binary)
        for prefix, multiplier in si_prefix.items():
            res[f"{prefix}{unit}"] = (multiplier, unit)
    return res


def _computer_readable(input_str: str, binary: bool = False):
    import dateparser
    import fastnumbers

    input_num = fastnumbers.fast_real(input_str)
    if type(input_num) in (int, float):
        return input_num, None

    # Regular expression to match the numeric value and the unit
    match = re.match(r"([0-9.]+)\s+([a-zA-Z/]+)$", input_str)
    if not match:
        dt = dateparser.parse(
            input_str,
            settings={
                "STRICT_PARSING": True,
                # exclude relative-time as returns current date for e.g. "0000:00:1c.4"
                "PARSERS": ["timestamp", "custom-formats", "absolute-time"],
            },
        )
        if dt:
            return dt.isoformat(), None
        return input_str, None

    value, unit_with_prefix = match.groups()
    value = float(value)
    multiplier, unit = _multipliers(binary).get(unit_with_prefix, (None, None))
    if not multiplier:
        return input_str, None
    return fastnumbers.fast_real(float(value) * multiplier), unit


_DMIDECODE_NO_PARSE = {"ID"}


def _dmi_propvals(data: dict) -> dict:
    data_copy = copy.deepcopy(data)
    for k, v in data["props"].items():
        if len(v["values"]) == 1:
            if k in _DMIDECODE_NO_PARSE:
                # don't try to convert these fields to numbers/dates, they are either useless, or parsed
                # incorrectly as dates
                data_copy["props"][k] = v["values"][0]
                unit = None
            else:
                # dmidecode uses SI prefixes for binary values (kB == kiB), so we treat them as the same
                data_copy["props"][k], unit = _computer_readable(v["values"][0], binary=True)
            if unit is not None:
                data_copy["props"][f"{k}:unit"] = unit
        else:
            data_copy["props"][k] = v["values"]
    return data_copy


def _text(data: bytes) -> str:
    """What the legacy got from `open(path, "r").read()`: UTF-8 with universal newlines."""
    return data.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")


def parse_dmidecode(stdout: bytes) -> bytes:
    from dmiparser import DmiParser

    dmi = json.loads(str(DmiParser(_text(stdout))))
    return json.dumps([_dmi_propvals(d) for d in dmi], indent=2).encode()


def parse_openssl(stdout: bytes) -> bytes:
    """Parses `openssl speed -mr` output from multiple, separate (one algo per run) runs, with a separator of
    `ALGO: (optional -evp option) algo_name ----`."""
    parsed_output = []
    for line in _text(stdout).splitlines():
        if m := re.search(r"^ALGO:( -[^\s]+ | )([^\s]+) --------------", line):
            # algo start, e.g. `ALGO: -evp blake2b512 ----` or `ALGO: X448 ----`: reset reused variables
            algo = m.group(2)
            blocksizes = []
        if m := re.search(r"^\+DT:[^\s:]+:[0-9]+:[0-9]+", line):
            algo = m.group(0).split(":")[1]
        if m := re.search(r"\+H(:[0-9]+)+", line):
            # block sizes of this test: `+H:16:64:256:1024` (single core) or `Got: +H:16:64:... from 15` (-multi)
            blocksizes = list(map(int, m.group(0).split(":")[1:]))
        if m := re.search(r"^\+F:[0-9]+:" + algo + "(:[0-9.]+)+$", line):
            res = m.group(0).split(":")
            algo = res[2]
            speed = res[3:]
            for i in range(len(blocksizes)):
                parsed_output.append(dict(algo=algo, block_size=blocksizes[i], speed=float(speed[i])))
    return json.dumps(parsed_output, indent=2).encode()


def parse_stressnglongrun(stderr: bytes) -> bytes:
    def extract(chunk: str, key: str) -> str:
        return re.search(f"{key}: ([0-9:\\.]*)\\n", chunk).group(1)

    out = io.StringIO(newline="")
    writer = csv.writer(out)
    for chunk in _text(stderr).split("...\n---"):
        date = extract(chunk, "date-yyyy-mm-dd").replace(":", "-")
        time = extract(chunk, r"time-hh-mm-ss")
        value = extract(chunk, "bogo-ops-per-second-real-time")
        duration = extract(chunk, "wall-clock-time")
        writer.writerow([date + "T" + time, duration, value])
    return out.getvalue().encode()


def _style_lstopo_svg(svg_content: str) -> str:
    """Apply the Spare Cores color scheme to the lstopo SVG."""
    import xml.etree.ElementTree as ET

    ET.register_namespace("", "http://www.w3.org/2000/svg")
    root = ET.fromstring(svg_content)

    stroke_color = "#34d399"
    font_stack = "ui-monospace, monospace"
    palettes = {
        "primary": {"fill": "#082f49", "text": "#ffffff"},
        "secondary": {"fill": "#0c4a6e", "text": "#ffffff"},
        "inverse": {"fill": "#ffffff", "text": "#082f49"},
    }
    themes = {
        "Machine": "primary",
        "Package": "primary",
        "NUMANode": "secondary",
        "Die": "secondary",
        "Core": "primary",
        "PU": "inverse",
        "L1d": "primary",
        "L1i": "primary",
        "L2": "primary",
        "L3": "primary",
        "PCIBridge": "primary",
        "HostBridge": "primary",
        "PCI": "secondary",
        "Net": "primary",
        "Block": "primary",
        "Misc": "primary",
    }
    descriptions = {
        "HostBridge": {"rect": "Host bridge", "text": "Host bridge speed"},
        "PCIBridge": {"rect": "PCI bridge", "text": "PCI bridge speed"},
        "L1i": {"rect": "CPU Level 1 (L1) instruction cache"},
        "L1d": {"rect": "CPU Level 1 (L1) data cache"},
        "L2": {"rect": "CPU Level 2 (L2) cache"},
        "L3": {"rect": "CPU Level 3 (L3) cache"},
        "PU": {"rect": "Processing unit"},
    }
    for elem in root.iter():
        tag = elem.tag
        # drop namespace prefix if present
        if isinstance(tag, str) and "}" in tag:
            tag = tag.split("}", 1)[1]
        if tag in ["rect", "line"]:
            elem.set("stroke", stroke_color)
        if tag == "text":
            elem.set("font-family", font_stack)
        theme = themes.get(elem.get("class", ""))
        if theme:
            palette = palettes[theme]
            if tag == "rect":
                elem.set("fill", palette["fill"])
            elif tag == "text":
                elem.set("fill", palette["text"])
        description = descriptions.get(elem.get("class", ""), {}).get(tag, "")
        if description:
            elem.set("data-description", description)

    # extra stylesheet injection on the top for hover effects
    style = ET.Element("style")
    style.text = f"""
        text {{ pointer-events: none; font-family: {font_stack}; }}
        rect {{ transition: stroke-width 0.15s ease, filter 0.15s ease; }}
        rect:hover {{
            stroke: #a7f3d0;
            stroke-width: 2;
            filter: brightness(1.2);
            cursor: pointer;
        }}
    """
    root.insert(0, style)
    return ET.tostring(root, encoding="unicode")


def parse_lstopo(stdout: bytes) -> bytes:
    """lstopo XML -> styled SVG with the hwloc `lstopo` binary (as the legacy: `lstopo -i <file> --if xml ...`)."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "stdout"
        path.write_bytes(stdout)
        result = subprocess.run(
            ["lstopo", "-i", str(path), "--if", "xml", "--of", "svg", "--no-legend"],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        )
    return _style_lstopo_svg(result.stdout).encode()


# parser name -> (input stream, output file name, function)
PARSERS = {
    "dmidecode": ("stdout", "parsed.json", parse_dmidecode),
    "openssl": ("stdout", "parsed.json", parse_openssl),
    "stressnglongrun": ("stderr", "parsed.csv", parse_stressnglongrun),
    "lstopo": ("stdout", "lstopo.svg", parse_lstopo),
}


# --------------------------------------------------------------------------- meta files and the schedule union


def _read_meta(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def _is_good(meta: dict | None, task_hash: str) -> bool:
    return bool(
        meta and meta.get("exit_code") == ExitCode.OK and meta.get("end") and meta.get("task_hash") == task_hash
    )


def _remove_stale_files(task_dir: Path, keep: Iterable[str]) -> None:
    """A new result replaces the old one: files of the old result that the new one does not write go (next to the
    new meta.json they would read as current). The caller writes meta.json after this."""
    if task_dir.is_dir():
        for path in task_dir.iterdir():
            if path.is_file() and path.name != "meta.json" and path.name not in keep:
                path.unlink()


def _is_stale(path: Path, start: datetime | None) -> bool:
    """The meta already in the tree started later than this result: a stale duplicate."""
    existing = _read_meta(path)
    existing_start = _parse_dt(existing.get("start")) if existing else None
    return bool(existing_start and start and existing_start > start)


def _write(path: Path, data: bytes | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data.encode() if isinstance(data, str) else data)


def _variant_roots(instance_root: Path) -> list[Path]:
    """Numeric variant directories, sorted by number."""
    if not instance_root.is_dir():
        return []
    return sorted((p for p in instance_root.iterdir() if p.is_dir() and p.name.isdigit()), key=lambda p: int(p.name))


def _meta_sort_key(meta: dict) -> tuple:
    """Newest-first key: prefer end, then start, then empty."""
    return (_parse_dt(meta.get("end")) or datetime.min, _parse_dt(meta.get("start")) or datetime.min)


def select_schedule_meta(instance_root: Path, task: str, current_hash: str, extra: dict | None = None) -> dict | None:
    """Materialized-union pick across variant metas for one task (legacy `select_schedule_meta`): unfinished
    metas are leftovers of dead runs and are ignored; otherwise current-hash success, else newest terminal
    current-hash, else newest available. `extra` is one more candidate that is not in a variant dir (a failure
    kept out of it)."""
    candidates = [extra] if extra else []
    for root in _variant_roots(instance_root):
        meta = _read_meta(root / task / "meta.json")
        if meta is None:
            continue
        if meta.get("start") and meta.get("end") is None:
            continue
        candidates.append(meta)
    if not candidates:
        return None
    success = [m for m in candidates if m.get("exit_code") == 0 and m.get("task_hash") == current_hash and m.get("end")]
    if success:
        return max(success, key=_meta_sort_key)
    terminal = [m for m in candidates if m.get("task_hash") == current_hash and m.get("end") is not None]
    if terminal:
        return max(terminal, key=_meta_sort_key)
    return max(candidates, key=_meta_sort_key)


ATTEMPT_FIELDS = ("run_id", "start", "end", "exit_code", "error_msg", "task_hash", "start_retries")


def latest_attempt(schedule_meta: dict) -> dict:
    """The latest attempt of a task on the SKU: `last_attempt` of its schedule meta, else the meta itself (it is
    the latest attempt whenever `last_attempt` is absent, and in a meta written before `last_attempt` existed)."""
    return schedule_meta.get("last_attempt") or schedule_meta


def refresh_schedule_meta(instance_root: Path, task: str, attempt: dict, extra: dict | None = None) -> Path | None:
    """Recompute `schedule/<task>/meta.json` from all variant metas (and `extra`) and record `attempt`, the
    result just published (any outcome), as `last_attempt` unless the meta is that attempt itself. An attempt older
    than the recorded one is not recorded. Untouched when nothing changes."""
    path = instance_root / "schedule" / task / "meta.json"
    existing = _read_meta(path)
    selected = select_schedule_meta(instance_root, task, attempt["task_hash"], extra)
    if selected is None:
        return None
    if selected.get("exit_code") != ExitCode.OK and _is_good(existing, attempt["task_hash"]):
        log.info("%s: exit code %s kept out of the schedule, which has a good result", path, selected.get("exit_code"))
        selected = existing  # a failure never replaces a good result of the same task hash
    recorded = latest_attempt(existing) if existing else None
    if recorded and (_parse_dt(recorded.get("start")) or datetime.min) > (
        _parse_dt(attempt.get("start")) or datetime.min
    ):
        attempt = recorded
    attempt = {field: attempt.get(field) for field in ATTEMPT_FIELDS}
    result = {k: v for k, v in selected.items() if k != "last_attempt"}
    if attempt != {field: result.get(field) for field in ATTEMPT_FIELDS}:
        result["last_attempt"] = attempt
    text = dump_meta(result)
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return None  # unchanged
    _write(path, text)
    return path


def _is_start_failure(meta: dict) -> bool:
    """A `-1` published for a command that did not start (see `published_meta`)."""
    return meta.get("exit_code") == ExitCode.NO_NODE and str(meta.get("error_msg")).startswith(START_FAILURE_PREFIX)


def _apply_start_retries(meta: dict, prior: dict | None) -> None:
    """`prior` is the latest attempt before this one (`latest_attempt`). `start_retries` counts consecutive start
    attempts of the same task hash that never produced a result: a `-4` or a start-failure `-1` (a command that does
    not start). It is +1 on such a code after one of that hash, otherwise 1 on such a code and 0 on any other code
    (success, a changed task hash, another failure; `-5` counts as a `-4` here). The third in a row becomes `-5`.
    The same run and start seen again (re-render after a crash) keeps its count."""
    counted = meta["exit_code"] == ExitCode.NEVER_FINISHED or _is_start_failure(meta)
    if prior and prior.get("run_id") == meta["run_id"] and prior.get("start") == meta["start"]:
        retries = prior.get("start_retries", 0)
    elif not counted:
        retries = 0
    elif (
        prior
        and (prior.get("exit_code") in NOT_FINAL_CODES or _is_start_failure(prior))
        and prior.get("task_hash") == meta["task_hash"]
    ):
        retries = prior.get("start_retries", 0) + 1
    else:
        retries = 1
    meta["start_retries"] = retries
    if counted and retries >= MAX_START_RETRIES:
        meta["exit_code"] = int(ExitCode.RETRIES_EXHAUSTED)
        meta["error_msg"] = f"exceeded {MAX_START_RETRIES} start retries"


# --------------------------------------------------------------------------- rendering


def render_task(tree: Path, vendor: str, sku: str, variant: int | None, inp: RenderInput) -> list[Path]:
    """Write `data/<vendor>/<sku>/<variant>/<task>/` (meta.json, stdout/stderr via the `raw` transform -- empty
    streams not written --, geekbench `results.json`, parsers' `parsed.*`/`lstopo.svg`) and refresh
    `schedule/<task>/meta.json`. `variant=None` (no node or no lscpu result: nothing to identify the hardware) and
    `-4`/`-5` (never finished: no outputs) write only `schedule/<task>/meta.json`. Skip (return []) when the meta
    already in the tree has a newer `start` (stale duplicate). Returns the written paths (removed stale files are
    not listed)."""
    task = inp.task.name
    instance = tree / "data" / vendor / sku
    schedule_path = instance / "schedule" / task / "meta.json"
    meta = published_meta(inp.status)
    start = _parse_dt(meta["start"])
    schedule_only = variant is None or meta["exit_code"] in NOT_FINAL_CODES
    if _is_stale(schedule_path if schedule_only else instance / str(variant) / task / "meta.json", start):
        return []
    scheduled = _read_meta(schedule_path)
    _apply_start_retries(meta, latest_attempt(scheduled) if scheduled else None)
    schedule_only = variant is None or meta["exit_code"] in NOT_FINAL_CODES  # a third `-4` or start failure became `-5`

    if schedule_only:
        meta["stdout_bytes"] = meta["stderr_bytes"] = None
        schedule = refresh_schedule_meta(instance, task, meta, extra=meta)
        return [schedule] if schedule else []

    task_dir = instance / str(variant) / task
    previous = _read_meta(task_dir / "meta.json")
    if (
        meta["exit_code"] != ExitCode.OK
        and previous
        and previous.get("exit_code") == ExitCode.OK
        and previous.get("end")
    ):
        # A failure (a cancelled task's partial output included) never replaces a good result of the task in this
        # variant. A changed task hash still needs the failure in the schedule (picked among the variants like any
        # other meta), or the planner would start the task again at every cycle.
        if _is_good(previous, meta["task_hash"]):
            log.info(
                "%s/%s/%s: exit code %s kept out of variant %s, which has a good result",
                vendor,
                sku,
                task,
                meta["exit_code"],
                variant,
            )
            schedule = refresh_schedule_meta(instance, task, meta)
            return [schedule] if schedule else []
        schedule = refresh_schedule_meta(instance, task, meta, extra=meta)
        return [schedule] if schedule else []
    files: dict[str, bytes] = {}
    outputs: list[str] = []
    if "raw" in inp.task.transforms and meta["exit_code"] != ExitCode.HARNESS_ERROR:
        for name, data in (("stdout", inp.stdout), ("stderr", inp.stderr)):
            if data:
                files[name] = data
                outputs.append(name)
    if meta["exit_code"] == ExitCode.OK:
        if "fetch_geekbench_results" in inp.task.transforms:
            results = geekbench_results(inp.stdout, inp.stderr)
            if results is not None:
                files["results.json"] = results
                outputs.append("results.json")
        if inp.task.parse:
            stream, out_name, parse = PARSERS[inp.task.parse]
            if stream in outputs:
                files[out_name] = parse(inp.stdout if stream == "stdout" else inp.stderr)
    meta["outputs"] = outputs
    meta["stdout_bytes"] = len(files["stdout"]) if "stdout" in files else None
    meta["stderr_bytes"] = len(files["stderr"]) if "stderr" in files else None

    written = []
    _remove_stale_files(task_dir, keep=files)
    for name, data in files.items():
        _write(task_dir / name, data)
        written.append(task_dir / name)
    _write(task_dir / "meta.json", dump_meta(meta))
    written.append(task_dir / "meta.json")
    schedule = refresh_schedule_meta(instance, task, meta)
    return written + ([schedule] if schedule else [])


def render_dbaas_task(tree: Path, database: dict, inp: RenderInput) -> list[Path]:
    """DBaaS result: `dbaas/<vendor>/<id>/postgres/<engineVersion>/standalone/<task>/{meta.json,stdout}`
    (`database` is `RunSpec.database`). No variant, no schedule."""
    task_dir = (
        tree / "dbaas" / database["vendor"] / database["id"] / "postgres" / str(database["engineVersion"])
        / "standalone" / inp.task.name
    )  # fmt: skip
    meta = published_meta(inp.status)
    if _is_stale(task_dir / "meta.json", _parse_dt(meta["start"])):
        return []
    previous = _read_meta(task_dir / "meta.json")
    if meta["exit_code"] != ExitCode.OK and previous and previous.get("exit_code") == ExitCode.OK:
        return []  # a failure never replaces a good result (sc-crawler reads the stdout without the exit code)
    _apply_start_retries(meta, previous)
    if meta["version"] is None:  # no version command runs against a managed database: the legacy wrote the image tag
        meta["version"] = inp.task.image.rsplit(":", 1)[-1]
    stdout = inp.stdout if inp.stdout and meta["exit_code"] not in NOT_FINAL_CODES else b""
    meta["outputs"] = ["stdout"] if stdout else []
    meta["stdout_bytes"] = len(stdout) if stdout else None
    meta["stderr_bytes"] = None
    written = []
    _remove_stale_files(task_dir, keep=["stdout"] if stdout else [])
    if stdout:
        _write(task_dir / "stdout", stdout)
        written.append(task_dir / "stdout")
    _write(task_dir / "meta.json", dump_meta(meta))
    return [*written, task_dir / "meta.json"]


def render_variant(
    tree: Path,
    vendor: str,
    sku: str,
    variant: int,
    lscpu_stdout: bytes,
    *,
    first_seen: str | None = None,
    lscpu_version: str | None = None,
) -> None:
    """Write `variant.json` when the variant dir has none (`variant.build_variant_json`, source `observed`).
    `first_seen` (naive UTC ISO, the lscpu task's end) and `lscpu_version` (the lscpu task's version) are
    stored as the legacy did."""
    from k8s_inspector import variant as variants

    path = tree / "data" / vendor / sku / str(variant) / "variant.json"
    if path.exists():
        return
    fields = variants.parse_lscpu_stdout(lscpu_stdout)
    data = variants.build_variant_json(fields, source="observed", first_seen=first_seen, lscpu_version=lscpu_version)
    _write(path, json.dumps(data, indent=2) + "\n")


def render_timing(tree: Path, vendor: str, sku: str, variant: int, run: RunKey, timings: dict[str, str]) -> None:
    """`timing/<run_id>/<name>`: one ISO UTC line each (`api_start`, `api_end`, `machine_start`,
    `node_registered`, `node_ready`, `inspector_start`, `inspector_end`); build the values with
    `format_timing_utc`."""
    run_dir = tree / "data" / vendor / sku / str(variant) / "timing" / run.run_id
    for name, value in timings.items():
        _write(run_dir / name, value + "\n")
