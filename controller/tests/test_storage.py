"""Storage tests against a moto S3 server: presigned PUT/UploadPart really work over HTTP."""

import json
import re
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pytest
import requests
from moto import mock_aws

from k8s_inspector import storage
from k8s_inspector.model import EnvConfig, RunKey, StepContainer, TaskDef, UploadSettings
from k8s_inspector.storage import Storage

RUN = RunKey("aws", "m5.large", date(2026, 3, 7), "r20260307T1200-abcd")
MIB = 1024 * 1024
BUCKET = "bkt"


@pytest.fixture
def store(monkeypatch):
    """Storage on moto's in-process mock; it also intercepts `requests`, so presigned URLs are really used.
    (moto's server mode needs flask, which is not a dev dependency.)"""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    with mock_aws():
        boto3.client("s3", region_name="eu-west-1").create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
        )
        yield Storage(EnvConfig(bucket=BUCKET, bucket_region="eu-west-1"), UploadSettings(part_mib=5, max_parts=3))


def container(key="membench", **task_kwargs) -> StepContainer:
    task = TaskDef(name=key, image="img", command=None, timeout=timedelta(hours=1), **task_kwargs)
    return StepContainer(key=key, task=task, image="img@sha256:x")


def http_put(url: str, data: bytes) -> str:
    resp = requests.put(url, data=data, timeout=30)
    resp.raise_for_status()
    return resp.headers["ETag"].strip('"')


def write_manifest(store: Storage, key: str, objects: dict, complete=True, missing=()):
    doc = {"version": 1, "task": key, "complete": complete, "objects": objects, "missing": list(missing)}
    store.s3.put_object(Bucket=BUCKET, Key=storage.object_key(RUN, key, "manifest.json"), Body=json.dumps(doc))


def signed_plans(store: Storage, containers):
    return store.presign(store.plan_uploads(RUN, containers))


# --------------------------------------------------------------------------- pure functions


def test_key_layout():
    prefix = "logs/inspect/aws/m5.large/2026/03/07/r20260307T1200-abcd/"
    assert storage.run_prefix(RUN) == prefix
    assert storage.object_key(RUN, "membench", "stdout") == prefix + "tasks/membench/stdout"
    assert storage.run_json_key(RUN) == prefix + "run.json"
    assert storage.split_urls_key("vllm-probe-x/result.json") == ("vllm-probe-x", "result.json")


def test_render_urls_json_matches_go_struct_fields():
    plan = storage.UploadPlan("t/big", "k", upload_id="U", part_size=8, n_parts=2, put_url="p", part_urls=("a", "b"))
    small = storage.UploadPlan("t/version", "k2", put_url="p2")
    doc = json.loads(storage.render_urls_json([plan, small]))
    assert doc == {
        "version": 1,
        "objects": {
            "t/big": {"put": "p", "upload_id": "U", "part_size": 8, "parts": ["a", "b"]},
            "t/version": {"put": "p2"},
        },
    }
    # The Go side (internal/urls/urls.go) must still use exactly these JSON names.
    go_src = Path(__file__).parents[2] / "internal" / "urls" / "urls.go"
    if go_src.exists():
        tags = set(re.findall(r'json:"(\w+)', go_src.read_text()))
        assert tags == {"put", "upload_id", "part_size", "parts", "version", "objects"}


# --------------------------------------------------------------------------- planning and signing


def test_plan_uploads_slots(store):
    c = container(large_outputs=("big.bin",), collect=("/tmp/pg.log:postgres.log", "/x:big.bin"))
    plans = {p.urls_key: p for p in store.plan_uploads(RUN, [c])}
    assert set(plans) == {f"membench/{n}" for n in (*storage.STANDARD_FILES, "big.bin", "postgres.log")}
    large = {k for k, p in plans.items() if p.upload_id}
    assert large == {"membench/stdout", "membench/stderr", "membench/resource_tracker.jsonl", "membench/big.bin"}
    p = plans["membench/big.bin"]
    assert (p.part_size, p.n_parts) == (5 * MIB, 3)
    assert p.object_key == storage.object_key(RUN, "membench", "big.bin")
    assert not p.put_url and not p.part_urls


def test_presigned_put_works(store):
    plans = {p.urls_key: p for p in signed_plans(store, [container()])}
    assert plans["membench/version"].part_urls == ()
    http_put(plans["membench/version"].put_url, b"1.2.3")
    assert store.read_bytes(storage.object_key(RUN, "membench", "version")) == b"1.2.3"
    assert store.read_bytes(storage.object_key(RUN, "membench", "nope")) is None


def test_presigned_urls_on_custom_endpoint(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    env = EnvConfig(bucket=BUCKET, bucket_region="eu-test-1", s3_endpoint_url="https://ns.compat.example:8443")
    plan = Storage(env, UploadSettings()).presign([storage.UploadPlan("t/version", "a/b/version")])[0]
    url = urlparse(plan.put_url)
    assert (url.netloc, url.path) == ("ns.compat.example:8443", f"/{BUCKET}/a/b/version")  # path-style
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in plan.put_url
    assert "eu-test-1" in plan.put_url  # credential scope has the bucket region
    assert f"X-Amz-Expires={12 * 3600}" in plan.put_url
    assert "x-amz-checksum" not in plan.put_url.lower()


def test_resign_keeps_other_tasks_and_upload_ids(store):
    first = signed_plans(store, [container("a"), container("b")])
    urls = storage.render_urls_json(first)
    again = store.presign(store.plans_from_urls_json(RUN, urls))
    old, new = json.loads(urls)["objects"], json.loads(storage.render_urls_json(again))["objects"]
    assert old.keys() == new.keys()
    for k, e in old.items():
        assert new[k].get("upload_id") == e.get("upload_id")
        assert len(new[k].get("parts", [])) == len(e.get("parts", []))
        assert new[k].get("part_size") == e.get("part_size")
    assert len(store.s3.list_multipart_uploads(Bucket=BUCKET)["Uploads"]) == 6


# --------------------------------------------------------------------------- finishing


def upload_large(store, plan, data: bytes, part_size=5 * MIB):
    parts = []
    for i, off in enumerate(range(0, len(data), part_size)):
        chunk = data[off : off + part_size]
        parts.append({"n": i + 1, "size": len(chunk), "etag": http_put(plan.part_urls[i], chunk)})
    return {
        "method": "multipart",
        "size": len(data),
        "upload_id": plan.upload_id,
        "part_size": part_size,
        "parts": parts,
    }


def test_finish_task_completes_multipart_and_verifies(store):
    plans = {p.urls_key: p for p in signed_plans(store, [container()])}
    data = b"x" * (5 * MIB) + b"tail"
    objects = {"stdout": upload_large(store, plans["membench/stdout"], data)}
    etag = http_put(plans["membench/version"].put_url, b"v1")
    objects["version"] = {"method": "put", "size": 2, "etag": etag}
    write_manifest(store, "membench", objects)

    v = store.finish_task(RUN, "membench")

    assert v.ok, v.problems
    assert store.read_bytes(storage.object_key(RUN, "membench", "stdout")) == data
    assert store.s3.list_multipart_uploads(Bucket=BUCKET).get("Uploads", []) == []
    assert v.manifest is not None and {o.name for o in v.manifest.objects} == {"stdout", "version"}
    assert store.finish_task(RUN, "membench").ok  # repeatable


def test_finish_task_problems(store):
    plans = {p.urls_key: p for p in signed_plans(store, [container()])}
    assert store.finish_task(RUN, "membench").problems == ("manifest.json missing",)

    http_put(plans["membench/version"].put_url, b"v1")
    write_manifest(store, "membench", {"version": {"method": "put", "size": 99}, "gone": {"method": "put", "size": 1}})
    v = store.finish_task(RUN, "membench")
    assert not v.ok
    assert "version: size 2 != manifest 99" in v.problems
    assert "gone: object missing" in v.problems
    assert len(store.s3.list_multipart_uploads(Bucket=BUCKET)["Uploads"]) == 3  # kept for a flush step

    write_manifest(store, "membench", {}, complete=False, missing=["stderr"])
    v = store.finish_task(RUN, "membench")
    assert not v.ok and "stderr" in v.problems[0]


def test_salvage_completes_listed_parts_and_keeps_small_logs(store):
    plans = signed_plans(store, [container()])
    by_key = {p.urls_key: p for p in plans}
    data = b"y" * (5 * MIB) + b"tail"
    upload_large(store, by_key["membench/stdout"], data)  # no manifest written
    http_put(by_key["membench/version"].put_url, b"v1")

    v = store.salvage(RUN, "membench", plans)

    assert v.ok and v.manifest is None
    assert store.read_bytes(storage.object_key(RUN, "membench", "stdout")) == data
    assert store.read_bytes(storage.object_key(RUN, "membench", "version")) == b"v1"
    assert store.s3.list_multipart_uploads(Bucket=BUCKET).get("Uploads", []) == []


def test_salvage_with_nothing_is_not_ok(store):
    plans = signed_plans(store, [container()])
    v = store.salvage(RUN, "membench", plans)
    assert not v.ok and "no object left" in v.problems
    assert store.abort_open(RUN, "membench") == 0


def test_abort_open_only_touches_task_prefix(store):
    signed_plans(store, [container("a"), container("b")])
    assert store.abort_open(RUN, "a") == 3
    assert len(store.s3.list_multipart_uploads(Bucket=BUCKET)["Uploads"]) == 3


# --------------------------------------------------------------------------- status.json, run.json, listing


def test_write_and_list_status_and_run_json(store):
    store.write_status_json(RUN, "membench", {"exit_code": 0})
    store.write_status_json(RUN, "stress", {"exit_code": 1})
    store.write_run_json(RUN, {"vendor": "aws"})
    other = RunKey("aws", "m5.large", date(2026, 3, 8), "r2")
    store.write_status_json(other, "membench", {})

    assert store.read_json(RUN, "membench", "status.json") == {"exit_code": 0}
    assert store.read_json(RUN, "nope", "status.json") is None
    assert store.s3.get_object(Bucket=BUCKET, Key=storage.run_json_key(RUN))["ContentType"] == "application/json"
    day = "logs/inspect/aws/m5.large/2026/03/07/"
    assert store.list_status_keys(day) == [
        storage.object_key(RUN, "membench", "status.json"),
        storage.object_key(RUN, "stress", "status.json"),
    ]
    assert len(store.list_status_keys(storage.run_prefix(other))) == 1
