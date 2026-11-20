# Feature mapping: sc-inspector → k8s-inspector

Legend: **P** planner, **R** run loop, **C** classification, **Pub** publisher, **W** `sc-run`, **S** Pod spec,
**N** node (SCarpenter / cluster platform), **—** dropped.

## Planning and state

| sc-inspector | k8s-inspector |
|---|---|
| `start` (GitHub Actions cron every 15 min, vendor matrix, `--limit 8`) | **P** every 5 min in the controller; per-vendor `maxInFlight` and `$/h` caps |
| `--start-only`, `--force` (`--exclude` unused) | CLI `run <vendor> <sku> [--tasks] [--force]`; creates a `BenchmarkRun` |
| vendor skiplist in `start.yml` | `vendors.<v>.enabled` / `maxInFlight: 0` |
| `available_servers` from sc-data, crc32 order | **P** unchanged |
| Keeper bulk block reasons, fail closed | **P** unchanged; the per-task re-check on the VM is dropped |
| `always_run` / `start_with_instance` | the probe step is in every attempt; `timing` is publisher output |
| 2 h `WAIT_SINCE_LAST_START`, boot placeholder commits as a lease | — replaced by the `BenchmarkRun` lease ([state model](../explanation/state.md)) |
| `FAIL_ON_ERROR` (3 d) raising | — failing tasks via `status --failing` and alerts |
| `task.rerun`, hash change, `RERUN_` edit | `revision`; hash unchanged; `RERUN_` still works ([tasks](tasks.md)) |
| `-1` with retryable markers | **C** SCarpenter reasons; **P** backoff |
| AWS spot self-heal | [spot strategy](../explanation/spot.md) |
| stale reconcile, `MAX_START_RETRIES` 3 → `-5` | **P** `start_retries` on `-4`, cap 3 → `-5` |
| `meta.json` / `schedule/` union, naive UTC | **Pub** same layout and union logic |
| exit codes | **C** same meanings, `-6` added, `-2` unused ([exit codes](exit-codes.md)) |
| priority / parallel groups, 8-thread queue | **P** ordered steps; the parallel group is one multi-container Pod |
| hardware variants | **R** after the probe step; **Pub** writes `variant.json` |

## Execution

| sc-inspector | k8s-inspector |
|---|---|
| Pulumi VM create, location ladder, spot-first AWS, Azure gen1 fallback, retry regexes | **N** SCarpenter (price-ordered offerings, capacity and quota handling) |
| user-data: Docker install, host quieting, registry mirrors | **N** Flatcar + containerd, Butane units, containerd mirrors |
| GPU driver install, nvidia-ctk, persistence mode, nouveau handling | **N** planned SCarpenter GPU feature ([GPU](../explanation/gpu.md)) |
| `shutdown +N`, `poweroff` | **S** step and anchor deadlines, anchor idle timeout, `expireAfter` |
| `cleanup`, `cleanup-sweep`, S3 run records, Pulumi locks | — teardown layers ([run model](../explanation/run-model.md#teardown-layers)) |
| image pull with 15 min cap | kubelet pull by digest; **R** deletes a step stuck pulling > 15 min |
| `docker_prune_after_round` | — kubelet image GC |
| `version_command` in a separate container | **W** `--version-cmd` before the task |
| `DOCKER_OPTS` privileged + host network | **S** `privileged`, `hostNetwork` |
| `DB_DOCKER_OPTS` seccomp, `nofile`, `memlock` | **S** `Unconfined`; **W** `--rlimit` |
| `pid_mode=host` (storage) | **S** `hostPID` on that step |
| 0.85 × available memory cap, `shm_size` 4 GiB | **S** memory limit from Node allocatable; memory-backed `/dev/shm` |
| per-task timeout, output discarded on timeout | **W** timeout with partial output kept, `-6` |
| GPU-first-then-CPU retry, vLLM probe ladder, nvbandwidth `-b` | **R** ported ([GPU](../explanation/gpu.md)) |
| `GPU_VRAM_MIB`, `GPU_COMPUTE_CAP` from the host | `gpu_facts` container in the probe step |
| `HF_TOKEN` to every container | **S** llm and vllm only |
| model cache in anonymous Docker volumes | **S** per-node hostPath shared by llm and vllm |
| `oom_score_adj=-1000` for the inspector | — nothing to protect on the node |
| multi-VM `MultiVmDbTask`, `companion` (dormant) | deferred (decision K12) |

## Outputs and publishing

| sc-inspector | k8s-inspector |
|---|---|
| stdout/stderr from `docker logs` → `transform.raw` | **W** spool files; **Pub** `raw` transform verbatim |
| `fetch_geekbench_results` (incl. the HTTP fallback) | **Pub** verbatim |
| `upload_task_logs_to_s3` (presigned POST, 100 MiB) | **W** presigned PUT / UploadPart to the same final layout ([output pipeline](../explanation/outputs.md)) |
| `resource_tracker.jsonl` (bind mount that never landed) | **W** spooled and uploaded |
| `postgres.log` (never copied) | **W** `--collect` |
| `SC_CDN_DATASET_POST_B64` (pgbench dataset upload on cache miss) | — dropped: the dataset is already on the CDN; a miss builds it locally ([output contract](output-contract.md#object-storage)) |
| git clone and push from every VM, deploy key on the VM | **Pub** single writer, blobless sparse clone, one commit per cycle |
| offline `parse` workflow with `git restore-mtime` | **Pub** parses at publish time |
| timing files | **Pub** from run status and `sc-run` |
| region/zone/spot from the start job | **R** from Node labels and NodeClaim |
| Sentry | controller logs (JSON lines) + [monitoring](../explanation/monitoring.md) |

## DBaaS

| sc-inspector | k8s-inspector |
|---|---|
| `start-dbaas`: one Pulumi stack = managed DB + private network + client VM | Job running Pulumi for the DB only; networks pre-created; the client is a bench node ([DBaaS](../explanation/dbaas.md)) |
| `cleanup-dbaas`, absolute timeout cleanup | controller `down` on every path + stack sweep + alert |
| retry any failure every 2 h forever | normal backoff with the cap of three |
| Azure pre-flight quota check: vCPUs per quota family for the Flexible Server SKU and the client VM | provisioner Job (Flexible Server, quota family from the ARM Resource SKUs API, [DBaaS](../explanation/dbaas.md#quota-pre-flight)); the client VM's quota is SCarpenter's (`QUOTA` / `QUOTA_ZERO`, [exit codes](exit-codes.md)) |
