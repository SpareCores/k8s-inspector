# Output contract

## Object storage

All keys are final: `sc-run` and the controller write each object once (or overwrite it in place); nothing is moved
afterwards. The layout extends sc-inspector's task-log layout, so existing lifecycle rules apply.

```
logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/<run_id>/run.json
logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/<run_id>/tasks/<task>/stdout
                                                         .../stderr
                                                         .../version
                                                         .../result.json              # sc-run: exit, timing, OOM, kernel, btime, nice
                                                         .../resource_tracker.jsonl
                                                         .../postgres.log             # pgbench tasks
                                                         .../postgres.json            # DBaaS pgbench: the bootstrap's PostgreSQL settings, merged into the published stdout
dbaas-state/.pulumi/...                                  # Pulumi state of the DBaaS stacks (written by the provisioner Jobs, see DBaaS)
                                                         .../manifest.json            # sc-run: written last
                                                         .../status.json              # controller: commit point
```

| Item | Rule |
|---|---|
| `<Y>/<m>/<d>` | the run's creation date (UTC), fixed for all attempts of the run |
| `run_id` | `r<UTC yyyymmdd>t<hhmm>-<4 hex>`, lowercase `t`, for example `r20261120t0930-3f9c`; also the `timing/<run_id>/` directory in git and `TRACKER_EXTERNAL_RUN_ID` |
| `<task>` | task name; vLLM probes as `vllm-probe-<image>`; a task repeated in a later attempt overwrites its keys |
| `run.json` | controller, see [run.json](#runjson) |
| `status.json` | controller, after uploads are verified: the final `meta.json` content plus Kubernetes facts (Pod, reason, image digest, node) |
| `K8SI_KEY_PREFIX` | optional, default empty: prepended to every key above (`<prefix>logs/inspect/…`), also when listing for `publish --rescan`; set it to run against a scratch prefix |
| Bucket rules | `AbortIncompleteMultipartUpload` after 7 days on `logs/inspect/` (a `K8SI_KEY_PREFIX` needs its own rule); the existing retention otherwise |

Access: the controller's IAM role may create, complete, abort and list multipart uploads, put, get, head and list under
`logs/inspect/`, and (for DBaaS) read, write, list and delete under `dbaas-state/`, which the provisioner Jobs use with their own role (`K8SI_DBAAS_ROLE_ARN`, limited to `dbaas-state/`). `dbaas-state/` has no expiry rule: the state of a live stack must not expire. Nothing else, and no write access to the CDN dataset prefix: the pgbench dataset is read from the
CDN and never uploaded by benchmark runs (`SC_CDN_UPLOAD=0`).

## run.json

Written by the controller whenever the node, the probe step or the end of an attempt adds facts; earlier attempts'
entries are kept.

```json
{"vendor": "oci", "sku": "VM.Standard.E5.Flex.2-16", "run_id": "r20261120t0930-3f9c",
 "plan": ["lscpu", "stressngfull"], "variant": 0, "database": null,
 "family": "…", "cpu_arch": "x86_64", "gpu_count": 0,
 "timing": {"api_start": "…", "api_end": "…", "machine_start": "…", "node_registered": "…", "node_ready": "…"},
 "attempts": [{"attempt": 1, "api_start": "…", "api_end": "…", "machine_start": "…", "node_registered": "…",
               "node_ready": "…", "lscpu_stdout": "…", "capacity_type": "spot", "node": "…", "node_claim": "…",
               "end": "…", "outcome": "SpotInterrupted", "region": "…", "zone": "…", "price": "0.0337", "ondemand_price": 0.0791}]}
```

`timing` repeats the latest attempt's timings. `api_start` of an attempt is the creation of its anchor. Each attempt's
`lscpu_stdout` is what picks the [variant](#sc-inspector-data) of the tasks that ran in it. `family`, `cpu_arch`,
`gpu_count` and the attempt's `ondemand_price` come from Keeper ([run records](run-records.md)).

## sc-inspector-data

The publisher writes the layout sc-inspector writes today. sc-crawler reads `main` of sc-inspector-data, so this is
the contract to keep.

| Path | Content | Read by sc-crawler |
|---|---|---|
| `data/<vendor>/<sku>/<N>/variant.json` | hardware variant (`id_version`, `id`, `first_seen`, lscpu fields) | no |
| `data/<vendor>/<sku>/<N>/<task>/meta.json` | task result, [fields](#metajson) | `start`, `end`, `exit_code`, `version`, `kernel_version` |
| `data/<vendor>/<sku>/<N>/<task>/stdout`, `stderr` | raw output (empty streams not written) | per task parsers |
| `.../geekbench/results.json` | geekbench transform | yes |
| `.../dmidecode/parsed.json`, `.../openssl/parsed.json` | parsers | yes |
| `.../stressnglongrun/parsed.csv`, `.../lstopo/lstopo.svg` | parsers | no (website) |
| `data/<vendor>/<sku>/<N>/timing/<run_id>/{api_start,api_end,machine_start,…}` | one ISO UTC line each | `api_start`, `machine_start` |
| `data/<vendor>/<sku>/runs/<run_id>.json`, `dbaas/<vendor>/<id>/runs/<run_id>.json` | run record: `run.json` without lscpu plus the final phase, [run records](run-records.md) | no |
| `data/<vendor>/<sku>/schedule/<task>/meta.json` | union view across variants (ported `select_schedule_meta`), plus [`last_attempt`](#last_attempt) | no (planner) |
| `dbaas/<vendor>/<id>/postgres/<ver>/standalone/<task>/{meta.json,stdout}` | DBaaS results | yes |
| `dbaas/<vendor>/<id>/postgres/<ver>/standalone/timing/<run_id>/{api_start,api_end,inspector_start,inspector_end,api_destroy_start,api_destroy_end}` | one ISO UTC line each | `inspector_start`, `inspector_end` |
| not carried over to git | `resource_tracker.jsonl`, `postgres.log`, `result.json`, `manifest.json`, `status.json`, `version`: object storage only | — |

`runs/` is no hardware variant: sc-crawler lists the variant directories of a SKU by `p.is_dir() and p.name.isdigit()`
(`sc_crawler/inspector.py`, `_server_variants`), like `schedule/`, and reads DBaaS results by explicit paths. The
record's content, write rule and backfill are in [run records](run-records.md).

Timing files (from the task's attempt in `run.json`): `api_start` = the attempt's anchor created, `api_end` =
NodeClaim launched, `machine_start` = kernel boot time from `sc-run`, plus `node_registered`, `node_ready`,
`inspector_start`/`inspector_end` (first task start and last task end).

DBaaS timing files (from `status.database.timing` and the task ends): `api_start` = the create Job is about to start,
`api_end` = the create Job succeeded, `inspector_start` = the first task started, `inspector_end` = the last task
ended, `api_destroy_start`/`api_destroy_end` = the destroy Job is about to start / succeeded (the Jobs' completion
times). A run without `api_start` and `api_end` has no timing files. The first four are published with the results; the
destroy files come in a later publish when the destroy succeeds (a run whose destroy never finishes has none, and a run
being deleted loses its object with the finalizer before `api_destroy_end` can be published). `meta.json` `version` of a DBaaS task is the image tag (no version command runs against a
managed database).

Variant dirs `<N>` are integers; the publisher picks the variant of a task from the `lscpu` output of its attempt with
the ported `variant_id.py` (`ID_VERSION=1`).

sc-crawler supports OCI, and the published data already has OCI servers, so results under `data/oci/...` on `main` flow
into sc-crawler and the published data. A SKU is published only after its outputs are checked to parse correctly and to
match the legacy layout; P0 spikes never publish. OCI SKU directories use Keeper's `api_reference` (for example
`VM.Standard.E5.Flex.2-16`), which sc-crawler also uses.

## meta.json

Unchanged fields and semantics, naive UTC timestamps, no trailing newline:
`start, end, exit_code, error_msg, task_hash, version, kernel_version, stdout_bytes, stderr_bytes, outputs,
start_retries, spot, region`. `spot` and `region` are always written (`null` when unknown; `spot` and `capacity_type` are
the capacity type of the node the task ran on, and `null` for a task that never got a node).

Added fields (sc-crawler reads `meta.json` as a dict and ignores unknown keys):
`run_id, image, zone, capacity_type, price, spot_attempts, node_os, kubelet, runtime, nodeclass_hash, nice` (`nice`: the nice
value the task ran at, from `sc-run`'s `result.json`; null when nice was disabled or could not be set).
A DBaaS meta may also carry `failed_regions`, the database regions of the start failures in a row (`DBAAS_CLIENT_FAILED`,
`DBAAS_LEFTOVER_STACK`, `DBAAS_REGION_UNAVAILABLE`), carried like `start_retries`: written only when non-empty, and
gone with any other result.

Exit codes: [exit codes](exit-codes.md).

### last_attempt

`schedule/<task>/meta.json` is the pick among the variants' metas, and a failure never replaces a good result in it.
So it also records the latest attempt of the task on the SKU, whatever its outcome and variant (and whether it was kept
out of the meta or written only to `schedule/`, like `-4`, `-5` and a `-1` without a node): `last_attempt` =
`{run_id, start, end, exit_code, error_msg, task_hash, start_retries}`. It is omitted when the meta is that attempt
itself, and an attempt older than the recorded one is not recorded. `start_retries` (and the `-5` after three in a row)
continues the chain of the previous latest attempt. The planner reads `last_attempt`, else the meta itself (which also
covers metas written before the field existed), to decide retries and [variant fills](configuration.md#autoplan).

## Commits

One commit per publish cycle by the controller, message `k8s-inspector: publish <n> results (<run ids>)`, pushed to
`main`. No placeholder or lease commits.
