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

import json
import os
from collections.abc import Sequence
from dataclasses import replace
from datetime import date

import boto3
from botocore.config import Config
from botocore.credentials import AssumeRoleWithWebIdentityCredentialFetcher, DeferredRefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import get_session

from k8s_inspector.model import (
    EnvConfig,
    Manifest,
    ManifestObject,
    ManifestPart,
    RunKey,
    StepContainer,
    UploadPlan,
    UploadSettings,
    Verification,
)

MIB = 1024 * 1024
DEFAULT_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SESSION_SECONDS = 12 * 3600
REFRESH_MARGIN_SECONDS = 15 * 60

# Files every task container produces (sc-run exec); each gets a presigned PUT.
STANDARD_FILES = ("stdout", "stderr", "version", "result.json", "resource_tracker.jsonl", "manifest.json")
# Standard files of serial tasks that also get multipart slots from the start (they can outgrow 32 MiB); a task
# adds its own via `TaskDef.large_outputs`.
# Parallel (probe) tasks get none of them: they run without resource-tracker and their logs are small, and
# the urls Secret must stay far below the 1 MiB object limit.
LARGE_STANDARD_FILES = ("stdout", "stderr", "resource_tracker.jsonl")
LOG_MAX_PARTS = 16  # stdout/stderr never need `uploads.max_parts` parts (16 x 16 MiB = 256 MiB)
MAX_URLS_BYTES = 700 * 1024  # urls.json size above which the Secret (base64: x 4/3) gets near 1 MiB


class UrlsTooLarge(ValueError):
    """The rendered urls Secret would approach the Kubernetes 1 MiB object limit."""


# --------------------------------------------------------------------------- key layout (pure)


_key_prefix = ""  # K8SI_KEY_PREFIX, set once by `Storage.__init__`; "" or ending in "/"


def day_prefix(vendor: str, sku: str, day: date) -> str:
    """`<key prefix>logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/` (with trailing slash)."""
    return f"{_key_prefix}logs/inspect/{vendor}/{sku}/{day:%Y/%m/%d}/"


def run_prefix(run: RunKey) -> str:
    """`<day_prefix>/<run_id>/` (with trailing slash)."""
    return f"{day_prefix(run.vendor, run.sku, run.day)}{run.run_id}/"


def object_key(run: RunKey, key: str, name: str) -> str:
    """`<run_prefix>tasks/<key>/<name>`."""
    return f"{run_prefix(run)}tasks/{key}/{name}"


def run_json_key(run: RunKey) -> str:
    """`<run_prefix>run.json`."""
    return f"{run_prefix(run)}run.json"


def split_urls_key(urls_key: str) -> tuple[str, str]:
    """`"<key>/<file>"` -> `(key, file)` (split at the first `/`; file names have none)."""
    key, _, name = urls_key.partition("/")
    return key, name


def _etag(value: str) -> str:
    return value.strip('"')


def _is_missing(e: ClientError) -> bool:
    return e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound")


def refreshable_credentials(refresh_using, uploads: UploadSettings) -> DeferredRefreshableCredentials:
    """Credentials that refresh early: presigned URLs die with the credentials that signed them and are
    re-signed every `resign_every_minutes`, so credentials with less than that plus a margin left are replaced
    (botocore's default is 15 minutes, too late for 12 h URLs)."""
    credentials = DeferredRefreshableCredentials(method="assume-role-with-web-identity", refresh_using=refresh_using)
    resign = uploads.resign_every_minutes * 60
    credentials._advisory_refresh_timeout = resign + REFRESH_MARGIN_SECONDS
    credentials._mandatory_refresh_timeout = resign
    return credentials


def _s3_client(env: EnvConfig, uploads: UploadSettings):
    # "when_required": newer boto3 otherwise signs a checksum of an empty body into presigned PUT URLs.
    # Path-style addressing for S3-compatible endpoints (for example OCI).
    config = Config(
        signature_version="s3v4",
        request_checksum_calculation="when_required",
        s3={"addressing_style": "path"} if env.s3_endpoint_url else {},
    )
    session = boto3.Session(region_name=env.bucket_region)
    if env.role_arn:
        # Web identity: the ServiceAccount token is exchanged for 12 h credentials, refreshed on demand.
        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE", DEFAULT_TOKEN_FILE)

        def load_token() -> str:
            with open(token_file) as f:
                return f.read().strip()

        botocore_session = get_session()
        fetcher = AssumeRoleWithWebIdentityCredentialFetcher(
            client_creator=botocore_session.create_client,
            web_identity_token_loader=load_token,
            role_arn=env.role_arn,
            extra_args={"RoleSessionName": "k8s-inspector", "DurationSeconds": SESSION_SECONDS},
        )
        botocore_session._credentials = refreshable_credentials(fetcher.fetch_credentials, uploads)
        session = boto3.Session(botocore_session=botocore_session, region_name=env.bucket_region)
    return session.client("s3", config=config, endpoint_url=env.s3_endpoint_url)


class Storage:
    """S3 access of the controller. One instance, shared by threads (boto3 clients are thread-safe)."""

    def __init__(self, env: EnvConfig, uploads: UploadSettings) -> None:
        """Build the boto3 client for `env.bucket_region` (and `env.s3_endpoint_url`), using the web identity
        role when `env.role_arn` is set (token file: `AWS_WEB_IDENTITY_TOKEN_FILE`, default the ServiceAccount
        token). No network call here."""
        global _key_prefix
        _key_prefix = f"{env.key_prefix.strip('/')}/" if env.key_prefix.strip("/") else ""
        self.bucket = env.bucket
        self.uploads = uploads
        self.s3 = _s3_client(env, uploads)

    # ---- planning and signing

    def plan_uploads(self, run: RunKey, containers: Sequence[StepContainer]) -> list[UploadPlan]:
        """For each container: a slot per `STANDARD_FILES` entry plus `task.large_outputs`, and the names
        of `task.collect` specs (the part after `:`). Large-capable files (`LARGE_STANDARD_FILES` plus
        `large_outputs`) get `CreateMultipartUpload`, `part_size` = `uploads.part_mib` MiB and `n_parts` =
        `uploads.max_parts` (`LOG_MAX_PARTS` at most for stdout/stderr). Only serial tasks get the standard
        ones (parallel probes run without resource-tracker and write small logs); `large_outputs` always do.
        Returned plans have no URLs yet. Whether a file goes by single PUT or by parts
        is sc-run's decision (`singlePutMaxMiB` is passed to it); the PUT URL is always present. Idempotent
        per attempt is NOT needed: a repeated task in a later attempt creates new uploads and overwrites
        its keys."""
        plans = []
        for c in containers:
            collected = [spec.partition(":")[2] for spec in c.task.collect]
            names = dict.fromkeys([*STANDARD_FILES, *c.task.large_outputs, *collected])
            large = {*(() if c.task.parallel else LARGE_STANDARD_FILES), *c.task.large_outputs}
            for name in names:
                okey = object_key(run, c.key, name)
                plan = UploadPlan(urls_key=f"{c.key}/{name}", object_key=okey)
                if name in large:
                    upload_id = self.s3.create_multipart_upload(Bucket=self.bucket, Key=okey)["UploadId"]
                    plan = replace(
                        plan,
                        upload_id=upload_id,
                        part_size=self.uploads.part_mib * MIB,
                        n_parts=min(self.uploads.max_parts, LOG_MAX_PARTS)
                        if name in ("stdout", "stderr")
                        else self.uploads.max_parts,
                    )
                plans.append(plan)
        return plans

    def _sign(self, operation: str, **params) -> str:
        ttl = self.uploads.url_ttl_hours * 3600
        return self.s3.generate_presigned_url(operation, Params={"Bucket": self.bucket, **params}, ExpiresIn=ttl)

    def presign(self, plans: Sequence[UploadPlan]) -> list[UploadPlan]:
        """Copies of `plans` with `put_url` and `part_urls` filled (PutObject / UploadPart presigned for
        `uploads.url_ttl_hours`). Also the re-sign: plans keep their `upload_id`."""
        signed = []
        for p in plans:
            part_urls = tuple(
                self._sign("upload_part", Key=p.object_key, UploadId=p.upload_id, PartNumber=n)
                for n in range(1, p.n_parts + 1)
                if p.upload_id
            )
            signed.append(replace(p, put_url=self._sign("put_object", Key=p.object_key), part_urls=part_urls))
        return signed

    def plans_from_urls_json(self, run: RunKey, urls_json: str) -> list[UploadPlan]:
        """Parse the Secret's current urls.json back into plans (no URLs kept) so a re-sign reuses the same
        multipart uploads. Every entry is returned (other tasks included); `object_key` is derived with
        `object_key()`."""
        plans = []
        for urls_key, e in json.loads(urls_json)["objects"].items():
            key, name = split_urls_key(urls_key)
            plans.append(
                UploadPlan(
                    urls_key=urls_key,
                    object_key=object_key(run, key, name),
                    upload_id=e.get("upload_id"),
                    part_size=e.get("part_size", 0),
                    n_parts=len(e.get("parts", [])),
                )
            )
        return plans

    # ---- reading

    def read_bytes(self, object_key: str) -> bytes | None:
        """GET any object (publisher: stdout, stderr, version...); None if absent."""
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=object_key)["Body"].read()
        except ClientError as e:
            if _is_missing(e):
                return None
            raise

    def read_json(self, run: RunKey, key: str, name: str) -> dict | None:
        """GET and parse a JSON object of a task (`result.json`, `status.json`); None if absent."""
        body = self.read_bytes(object_key(run, key, name))
        return None if body is None else json.loads(body)

    def read_manifest(self, run: RunKey, key: str) -> Manifest | None:
        """GET `tasks/<key>/manifest.json` and parse it (objects.md "manifest.json"); None if absent."""
        doc = self.read_json(run, key, "manifest.json")
        if doc is None:
            return None
        objects = tuple(
            ManifestObject(
                name=name,
                method=o["method"],
                size=o["size"],
                md5=o.get("md5"),
                etag=o.get("etag"),
                upload_id=o.get("upload_id"),
                part_size=o.get("part_size", 0),
                parts=tuple(ManifestPart(n=p["n"], size=p["size"], etag=p["etag"]) for p in o.get("parts", [])),
            )
            for name, o in sorted(doc.get("objects", {}).items())
        )
        return Manifest(
            task=doc.get("task", key),
            complete=doc["complete"],
            objects=objects,
            missing=tuple(doc.get("missing", [])),
        )

    # ---- finishing a step

    def _complete(self, okey: str, upload_id: str, parts: Sequence[tuple[int, str]]) -> None:
        """CompleteMultipartUpload; an upload that no longer exists counts as done (a repeated call)."""
        try:
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=okey,
                UploadId=upload_id,
                MultipartUpload={"Parts": [{"PartNumber": n, "ETag": f'"{_etag(e)}"'} for n, e in parts]},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchUpload":
                raise

    def _head(self, okey: str) -> dict | None:
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=okey)
        except ClientError as e:
            if _is_missing(e):
                return None
            raise

    def finish_task(self, run: RunKey, key: str) -> Verification:
        """The "when a step ends" sequence for one container: read the manifest, `CompleteMultipartUpload`
        for every `multipart` object (ETags from the manifest), `HEAD` every object and compare size (and the
        ETag of `put` objects), then abort the unused multipart uploads under the task prefix. No manifest,
        an incomplete manifest or a mismatch -> `ok=False` with `problems`; nothing is raised for those.
        In those cases nothing is completed or aborted (an incomplete manifest or a failed check is followed
        by a `flush` step that uses the same uploads); the runner calls `salvage` or `abort_open` when it
        gives up. Failures of S3 itself are raised."""
        manifest = self.read_manifest(run, key)
        if manifest is None:
            return Verification(key, False, None, ("manifest.json missing",))
        if not manifest.complete:
            problem = "manifest lists missing files: " + ", ".join(manifest.missing)
            return Verification(key, False, manifest, (problem,))

        problems = []
        for o in manifest.objects:
            okey = object_key(run, key, o.name)
            if o.method == "multipart":
                if not o.parts or not o.upload_id:
                    problems.append(f"{o.name}: multipart entry without upload_id or parts")
                    continue
                try:
                    self._complete(okey, o.upload_id, [(p.n, p.etag) for p in o.parts])
                except ClientError as e:
                    problems.append(f"{o.name}: complete failed ({e.response['Error']['Code']})")
                    continue
            head = self._head(okey)
            if head is None:
                problems.append(f"{o.name}: object missing")
            elif head["ContentLength"] != o.size:
                problems.append(f"{o.name}: size {head['ContentLength']} != manifest {o.size}")
            elif o.method == "put" and o.etag and _etag(head["ETag"]) != _etag(o.etag):
                problems.append(f"{o.name}: etag differs from manifest")
        if not problems:
            self.abort_open(run, key)
        return Verification(key, not problems, manifest, tuple(problems))

    def salvage(self, run: RunKey, key: str, plans: Sequence[UploadPlan]) -> Verification:
        """Node-loss path: no manifest. For each plan with an `upload_id`, `ListParts` and complete the
        upload from the parts present (skip when none); keep whatever small logs the last re-PUT left;
        abort the rest. `ok` is False unless something usable remains; the runner records `-4`."""
        problems = []
        usable = 0
        for p in plans:
            if split_urls_key(p.urls_key)[0] != key:
                continue
            if p.upload_id:
                parts = self._list_parts(p.object_key, p.upload_id)
                if parts:
                    try:
                        self._complete(p.object_key, p.upload_id, [(n, e) for n, e in parts])
                    except ClientError as e:
                        problems.append(f"{p.urls_key}: complete failed ({e.response['Error']['Code']})")
            if self._head(p.object_key) is not None:
                usable += 1
        self.abort_open(run, key)
        if not usable:
            problems.append("no object left")
        return Verification(key, usable > 0, None, tuple(problems))

    def _list_parts(self, okey: str, upload_id: str) -> list[tuple[int, str]]:
        try:
            pages = self.s3.get_paginator("list_parts").paginate(Bucket=self.bucket, Key=okey, UploadId=upload_id)
            return [(p["PartNumber"], p["ETag"]) for page in pages for p in page.get("Parts", [])]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchUpload":
                return []
            raise

    def abort_open(self, run: RunKey, key: str) -> int:
        """Abort every open multipart upload under `tasks/<key>/`; returns how many."""
        prefix = f"{run_prefix(run)}tasks/{key}/"
        count = 0
        pages = self.s3.get_paginator("list_multipart_uploads").paginate(Bucket=self.bucket, Prefix=prefix)
        for page in pages:
            for u in page.get("Uploads", []):
                self.s3.abort_multipart_upload(Bucket=self.bucket, Key=u["Key"], UploadId=u["UploadId"])
                count += 1
        return count

    # ---- writes

    def _put_json(self, okey: str, doc: dict) -> None:
        body = json.dumps(doc, indent=1).encode()
        self.s3.put_object(Bucket=self.bucket, Key=okey, Body=body, ContentType="application/json")

    def write_status_json(self, run: RunKey, key: str, doc: dict) -> None:
        """PUT `tasks/<key>/status.json`: the task's commit point. Written only after `finish_task` ok or
        after a final failure was decided."""
        self._put_json(object_key(run, key, "status.json"), doc)

    def write_run_json(self, run: RunKey, doc: dict) -> None:
        """PUT `run.json` (vendor, sku, plan, attempts, variant); overwritten as the run progresses."""
        self._put_json(run_json_key(run), doc)

    # ---- discovery (publisher rescan, DR)

    def list_status_keys(self, run_prefix_or_day_prefix: str) -> list[str]:
        """Object keys of `status.json` objects under a prefix (rescan: `day_prefix()`)."""
        pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=run_prefix_or_day_prefix)
        keys = (o["Key"] for page in pages for o in page.get("Contents", []))
        return [k for k in keys if k.endswith("/status.json") and "/tasks/" in k]


# --------------------------------------------------------------------------- urls.json


def render_urls_json(plans: Sequence[UploadPlan]) -> str:
    """The Secret's `urls.json`: `{"version": 1, "objects": {urls_key: entry}}` with `put` always,
    and `upload_id`, `part_size`, `parts` for multipart-capable plans (schema above). Deterministic key
    order, compact JSON. Raises `UrlsTooLarge` above `MAX_URLS_BYTES`."""
    objects = {}
    for p in sorted(plans, key=lambda p: p.urls_key):
        entry: dict = {"put": p.put_url}
        if p.upload_id:
            entry |= {"upload_id": p.upload_id, "part_size": p.part_size, "parts": list(p.part_urls)}
        objects[p.urls_key] = entry
    text = json.dumps({"version": 1, "objects": objects}, separators=(",", ":"))
    check_urls_size(text)
    return text


def secret_size(urls_json: str) -> int:
    """Bytes the urls.json takes in the Secret object (base64 of the JSON)."""
    return len(urls_json.encode()) * 4 // 3


def check_urls_size(urls_json: str) -> None:
    """Raise `UrlsTooLarge` when `urls_json` exceeds `MAX_URLS_BYTES` (Kubernetes rejects Secrets over 1 MiB)."""
    size = len(urls_json.encode())
    if size > MAX_URLS_BYTES:
        raise UrlsTooLarge(
            f"urls.json is {size // 1024} KiB (Secret about {secret_size(urls_json) // 1024} KiB), above the "
            f"{MAX_URLS_BYTES // 1024} KiB limit: too many multipart slots in one step "
            "(lower uploads.maxParts or split the step)"
        )
