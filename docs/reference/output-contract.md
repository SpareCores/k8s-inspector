# Output contract

## Object storage

All keys are final: `sc-run` and the controller write each object once (or overwrite it in place); nothing is moved
afterwards. The layout extends sc-inspector's task-log layout, so existing lifecycle rules apply.

```
logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/<run_id>/run.json
logs/inspect/<vendor>/<sku>/<Y>/<m>/<d>/<run_id>/tasks/<task>/stdout
                                                         .../stderr
                                                         .../version
                                                         .../result.json              # sc-run: exit, timing, OOM, kernel, btime
                                                         .../resource_tracker.jsonl
                                                         .../postgres.log             # pgbench tasks
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
| Bucket rules | `AbortIncompleteMultipartUpload` after 7 days on `logs/inspect/`; the existing retention otherwise |

Access: the controller's IAM role may create, complete, abort and list multipart uploads, put, get, head and list under
`logs/inspect/`. Nothing else, and no write access to the CDN dataset prefix: the pgbench dataset is read from the
CDN and never uploaded by benchmark runs (`SC_CDN_UPLOAD=0`).

## run.json

Written by the controller whenever the node, the probe step or the end of an attempt adds facts; earlier attempts'
entries are kept.

```json
{"vendor": "oci", "sku": "VM.Standard.E5.Flex.2-16", "run_id": "r20261120t0930-3f9c",
 "plan": ["lscpu", "stressngfull"], "variant": 0, "database": null,
 "timing": {"api_start": "…", "api_end": "…", "machine_start": "…", "node_registered": "…", "node_ready": "…"},
 "attempts": [{"attempt": 1, "api_start": "…", "api_end": "…", "machine_start": "…", "node_registered": "…",
               "node_ready": "…", "lscpu_stdout": "…", "capacity_type": "spot", "node": "…", "node_claim": "…",
               "end": "…", "outcome": "SpotInterrupted", "region": "…", "zone": "…", "price": "0.0337"}]}
```

`timing` repeats the latest attempt's timings. `api_start` of an attempt is the creation of its anchor. Each attempt's
`lscpu_stdout` is what picks the [variant](#sc-inspector-data) of the tasks that ran in it.

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
| `data/<vendor>/<sku>/schedule/<task>/meta.json` | union view across variants (ported `select_schedule_meta`) | no (planner) |
| `dbaas/<vendor>/<id>/postgres/<ver>/standalone/<task>/{meta.json,stdout}` | DBaaS results | yes |
| not carried over to git | `resource_tracker.jsonl`, `postgres.log`, `result.json`, `manifest.json`, `status.json`, `version`: object storage only | — |

Timing files (from the task's attempt in `run.json`): `api_start` = the attempt's anchor created, `api_end` =
NodeClaim launched, `machine_start` = kernel boot time from `sc-run`, plus `node_registered`, `node_ready`,
`inspector_start`/`inspector_end` (first task start and last task end).

Variant dirs `<N>` are integers; the publisher picks the variant of a task from the `lscpu` output of its attempt with
the ported `variant_id.py` (`ID_VERSION=1`).

sc-crawler supports OCI, and the published data already has OCI servers, so results under `data/oci/...` on `main` flow
into sc-crawler and the published data. A SKU is published only after its outputs are checked to parse correctly and to
match the legacy layout; P0 spikes never publish. OCI SKU directories use Keeper's `api_reference` (for example
`VM.Standard.E5.Flex.2-16`), which sc-crawler also uses.

## meta.json

Unchanged fields and semantics, naive UTC timestamps, no trailing newline:
`start, end, exit_code, error_msg, task_hash, version, kernel_version, stdout_bytes, stderr_bytes, outputs,
start_retries, spot, region`. `spot` and `region` are always written (`null` when unknown).

Added fields (sc-crawler reads `meta.json` as a dict and ignores unknown keys):
`run_id, image, zone, capacity_type, price, spot_attempts, node_os, kubelet, runtime, nodeclass_hash`.

Exit codes: [exit codes](exit-codes.md).

## Commits

One commit per publish cycle by the controller, message `k8s-inspector: publish <n> results (<run ids>)`, pushed to
`main`. No placeholder or lease commits.
