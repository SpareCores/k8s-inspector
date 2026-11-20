"""Object storage (S3): key layout, upload planning, presigning, multipart control, verification.

Owner: storage agent. Implements docs/explanation/outputs.md "In the controller" and
docs/reference/output-contract.md "Object storage"; its urls.json output must match what `sc-run` reads
(Go: internal/urls: `{"version": 1, "objects": {"<key>/<file>": {"put", "upload_id", "part_size", "parts"}}}`).
Port the proven mechanics of spikes/p0/presign.py (path-style addressing for S3-compatible endpoints,
`request_checksum_calculation="when_required"` so presigned PUTs carry no checksum header).

Only `PutObject` and `UploadPart` are presigned. Create/Complete/Abort/ListParts, HEAD, GET, LIST and the
`status.json`/`run.json` writes are direct calls with the controller's role (web identity from the
ServiceAccount token, role `K8SI_ROLE_ARN`, 12 h sessions). Presigned URLs stop working when the signing
credentials expire, so `resign` must run at least hourly.

URLs are bearer secrets: never log them or put them in exceptions.
"""

from collections.abc import Sequence

from k8s_inspector.model import (
    EnvConfig,
    Manifest,
    RunKey,
    StepContainer,
    UploadPlan,
    UploadSettings,
    Verification,
)

# Files every task container produces (sc-run exec); each gets a presigned PUT.
STANDARD_FILES = ("stdout", "stderr", "version", "result.json", "resource_tracker.jsonl", "manifest.json")
# Standard files that also get multipart slots from the start (they can outgrow 32 MiB); a task adds its own
# via `TaskDef.large_outputs`. Not settled by the docs: see INTERFACES.md "Decisions".
LARGE_STANDARD_FILES = ("stdout", "stderr", "resource_tracker.jsonl")


# --------------------------------------------------------------------------- key layout (pure)


def run_prefix(run: RunKey) -> str:
    """`logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/<run_id>/` (with trailing slash)."""
    raise NotImplementedError


def object_key(run: RunKey, key: str, name: str) -> str:
    """`<run_prefix>tasks/<key>/<name>`."""
    raise NotImplementedError


def run_json_key(run: RunKey) -> str:
    """`<run_prefix>run.json`."""
    raise NotImplementedError


def split_urls_key(urls_key: str) -> tuple[str, str]:
    """`"<key>/<file>"` -> `(key, file)` (split at the first `/`; file names have none)."""
    raise NotImplementedError


class Storage:
    """S3 access of the controller. One instance, shared by threads (boto3 clients are thread-safe)."""

    def __init__(self, env: EnvConfig, uploads: UploadSettings) -> None:
        """Build the boto3 client for `env.bucket_region` (and `env.s3_endpoint_url`), using the web identity
        role when `env.role_arn` is set. No network call here."""
        raise NotImplementedError

    # ---- planning and signing

    def plan_uploads(self, run: RunKey, containers: Sequence[StepContainer]) -> list[UploadPlan]:
        """For each container: a slot per `STANDARD_FILES` entry plus `task.large_outputs`, and the names
        of `task.collect` specs (the part after `:`). Large-capable files (`LARGE_STANDARD_FILES` plus
        `large_outputs` plus collect names in `large_outputs`) get `CreateMultipartUpload`, `part_size` =
        `uploads.part_mib` MiB and `n_parts` = `uploads.max_parts`. Returned plans have no URLs yet.
        Idempotent per attempt is NOT needed: a repeated task in a later attempt creates new uploads and
        overwrites its keys."""
        raise NotImplementedError

    def presign(self, plans: Sequence[UploadPlan]) -> list[UploadPlan]:
        """Copies of `plans` with `put_url` and `part_urls` filled (PutObject / UploadPart presigned for
        `uploads.url_ttl_hours`). Also the re-sign: plans keep their `upload_id`."""
        raise NotImplementedError

    def plans_from_urls_json(self, run: RunKey, urls_json: str) -> list[UploadPlan]:
        """Parse the Secret's current urls.json back into plans (no URLs kept) so a re-sign reuses the same
        multipart uploads. `object_key` is derived with `object_key()`."""
        raise NotImplementedError

    # ---- finishing a step

    def read_manifest(self, run: RunKey, key: str) -> Manifest | None:
        """GET `tasks/<key>/manifest.json` and parse it (objects.md "manifest.json"); None if absent."""
        raise NotImplementedError

    def read_json(self, run: RunKey, key: str, name: str) -> dict | None:
        """GET and parse a JSON object of a task (`result.json`, `status.json`); None if absent."""
        raise NotImplementedError

    def read_bytes(self, object_key: str) -> bytes | None:
        """GET any object (publisher: stdout, stderr, version...); None if absent."""
        raise NotImplementedError

    def finish_task(self, run: RunKey, key: str) -> Verification:
        """The "when a step ends" sequence for one container: read the manifest, `CompleteMultipartUpload`
        for every `multipart` object (ETags from the manifest), `HEAD` every object and compare size, abort
        the unused multipart uploads under the task prefix. No manifest or a mismatch -> `ok=False` with
        `problems`; nothing is raised for those."""
        raise NotImplementedError

    def salvage(self, run: RunKey, key: str, plans: Sequence[UploadPlan]) -> Verification:
        """Node-loss path: no manifest. For each plan with an `upload_id`, `ListParts` and complete the
        upload from the parts present (skip when none); keep whatever small logs the last re-PUT left;
        abort the rest. `ok` is False unless something usable remains; the runner records `-4`."""
        raise NotImplementedError

    def abort_open(self, run: RunKey, key: str) -> int:
        """Abort every open multipart upload under `tasks/<key>/`; returns how many."""
        raise NotImplementedError

    def write_status_json(self, run: RunKey, key: str, doc: dict) -> None:
        """PUT `tasks/<key>/status.json`: the task's commit point. Written only after `finish_task` ok or
        after a final failure was decided."""
        raise NotImplementedError

    def write_run_json(self, run: RunKey, doc: dict) -> None:
        """PUT `run.json` (vendor, sku, plan, attempts, variant); overwritten as the run progresses."""
        raise NotImplementedError

    # ---- discovery (publisher rescan, DR)

    def list_status_keys(self, run_prefix_or_day_prefix: str) -> list[str]:
        """Object keys of `status.json` objects under a prefix (rescan: `logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/`)."""
        raise NotImplementedError


# --------------------------------------------------------------------------- urls.json


def render_urls_json(plans: Sequence[UploadPlan]) -> str:
    """The Secret's `urls.json`: `{"version": 1, "objects": {urls_key: entry}}` with `put` always,
    and `upload_id`, `part_size`, `parts` for multipart-capable plans (schema above). Deterministic key
    order, compact JSON."""
    raise NotImplementedError
