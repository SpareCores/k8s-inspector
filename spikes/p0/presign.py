#!/usr/bin/env python3
"""P0 spike helper: the controller's object storage work, by hand.

  presign   create multipart uploads, presign PUT / UploadPart URLs, write urls.json
            (--resign OLD.json: new signatures for the same uploads, for URL re-signing)
  complete  read each task's manifest.json and complete its multipart uploads
  abort     abort the multipart uploads of the run that are still open
  verify    HEAD-check every object of the manifests and write status.json

Credentials come from the default AWS chain (use AWS_PROFILE or --profile);
nothing secret is printed. urls.json holds bearer URLs: it is written 0600.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

MIB = 1024 * 1024
# Files every task uploads (sc-run exec); more come from --file.
STANDARD_FILES = ["stdout", "stderr", "version", "result.json", "resource_tracker.jsonl", "manifest.json"]


def s3_client(args):
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    # "when_required": newer boto3 otherwise signs a checksum of an empty body into presigned PUT URLs.
    config = Config(signature_version="s3v4", request_checksum_calculation="when_required")
    return session.client("s3", config=config)


def key(args, task, name):
    return f"{args.prefix.strip('/')}/{args.run_id}/tasks/{task}/{name}"


def split_task_file(spec):
    task, sep, name = spec.partition("/")
    if not sep or not name:
        sys.exit(f"expected <task>/<file>, got {spec!r}")
    return task, name


def presign(args):
    s3 = s3_client(args)
    extra, large = {}, set()
    for spec in args.file:
        task, name = split_task_file(spec)
        extra.setdefault(task, []).append(name)
    for spec in args.large:
        task, name = split_task_file(spec)
        large.add((task, name))
        if name not in extra.get(task, []) and name not in STANDARD_FILES:
            extra.setdefault(task, []).append(name)

    old = {}
    if args.resign:
        with open(args.resign) as f:
            old = json.load(f)["objects"]
    objects = {}
    for task in args.task:
        for name in STANDARD_FILES + extra.get(task, []):
            k = key(args, task, name)
            entry = {
                "put": s3.generate_presigned_url(
                    "put_object", Params={"Bucket": args.bucket, "Key": k}, ExpiresIn=args.expires
                )
            }
            if (task, name) in large:
                upload_id = old.get(f"{task}/{name}", {}).get("upload_id")
                if not upload_id:
                    upload_id = s3.create_multipart_upload(Bucket=args.bucket, Key=k)["UploadId"]
                entry["upload_id"] = upload_id
                entry["part_size"] = args.part_mib * MIB
                entry["parts"] = [
                    s3.generate_presigned_url(
                        "upload_part",
                        Params={"Bucket": args.bucket, "Key": k, "UploadId": upload_id, "PartNumber": n},
                        ExpiresIn=args.expires,
                    )
                    for n in range(1, args.parts + 1)
                ]
            objects[f"{task}/{name}"] = entry

    fd = os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump({"version": 1, "objects": objects}, f, indent=1)
    print(f"wrote {args.out}: {len(objects)} objects, {len(large)} multipart, expires in {args.expires}s")
    print("note: the URLs stop working when the signing credentials expire, even before that time")


def read_manifest(s3, args, task):
    try:
        body = s3.get_object(Bucket=args.bucket, Key=key(args, task, "manifest.json"))["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


def complete(args):
    s3 = s3_client(args)
    failed = False
    for task in args.task:
        manifest = read_manifest(s3, args, task)
        if manifest is None:
            print(f"{task}: no manifest.json")
            failed = True
            continue
        for name, obj in manifest["objects"].items():
            if obj["method"] != "multipart":
                continue
            parts = [{"PartNumber": p["n"], "ETag": f'"{p["etag"]}"'} for p in obj["parts"]]
            s3.complete_multipart_upload(
                Bucket=args.bucket,
                Key=key(args, task, name),
                UploadId=obj["upload_id"],
                MultipartUpload={"Parts": parts},
            )
            print(f"{task}/{name}: completed {len(parts)} parts, {obj['size']} bytes")
    sys.exit(1 if failed else 0)


def abort(args):
    s3 = s3_client(args)
    prefix = f"{args.prefix.strip('/')}/{args.run_id}/"
    count = 0
    for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=args.bucket, Prefix=prefix):
        for u in page.get("Uploads", []):
            s3.abort_multipart_upload(Bucket=args.bucket, Key=u["Key"], UploadId=u["UploadId"])
            count += 1
    print(f"aborted {count} open multipart uploads under {prefix}")


def verify(args):
    s3 = s3_client(args)
    all_ok = True
    for task in args.task:
        manifest = read_manifest(s3, args, task)
        problems, checked = [], 0
        if manifest is None:
            problems.append("manifest.json missing")
        else:
            if not manifest["complete"]:
                problems.append("manifest lists missing files: " + ", ".join(manifest.get("missing", [])))
            for name, obj in manifest["objects"].items():
                checked += 1
                try:
                    head = s3.head_object(Bucket=args.bucket, Key=key(args, task, name))
                except ClientError as e:
                    problems.append(f"{name}: HEAD failed ({e.response['Error']['Code']})")
                    continue
                if head["ContentLength"] != obj["size"]:
                    problems.append(f"{name}: size {head['ContentLength']} != manifest {obj['size']}")
        status = {
            "task": task,
            "ok": not problems,
            "objects_checked": checked,
            "problems": problems,
            "verified_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        s3.put_object(
            Bucket=args.bucket,
            Key=key(args, task, "status.json"),
            Body=json.dumps(status, indent=1).encode(),
        )
        print(f"{task}: {'ok' if status['ok'] else 'PROBLEMS'} ({checked} objects)")
        for p in problems:
            print(f"  - {p}")
        all_ok &= status["ok"]
    sys.exit(0 if all_ok else 1)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bucket", required=True)
    common.add_argument("--prefix", required=True, help="key prefix, e.g. spikes/p0")
    common.add_argument("--run-id", required=True)
    common.add_argument("--task", action="append", default=[], help="task name (repeat)")
    common.add_argument("--region")
    common.add_argument("--profile", help="AWS profile (default: the usual credential chain)")
    sub = p.add_subparsers(dest="command", required=True)

    ps = sub.add_parser("presign", parents=[common], help="create multipart uploads and write urls.json")
    ps.add_argument("--expires", type=int, default=3600, help="URL lifetime in seconds")
    ps.add_argument("--file", action="append", default=[], help="extra output <task>/<file> (repeat)")
    ps.add_argument("--large", action="append", default=[], help="<task>/<file> that gets multipart URLs (repeat)")
    ps.add_argument("--part-mib", type=int, default=16)
    ps.add_argument("--parts", type=int, default=64, help="UploadPart URLs per large file")
    ps.add_argument("--out", default="urls.json")
    ps.add_argument("--resign", metavar="OLD_URLS_JSON", help="reuse the multipart upload ids of this file")
    ps.set_defaults(func=presign)
    for name, func in (("complete", complete), ("abort", abort), ("verify", verify)):
        sub.add_parser(name, parents=[common]).set_defaults(func=func)

    args = p.parse_args()
    if args.command != "abort" and not args.task:
        p.error("at least one --task is required")
    args.func(args)


if __name__ == "__main__":
    main()
