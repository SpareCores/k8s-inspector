# Alternatives considered

## Sequencing tasks on one node (K1)

| Option | Why not |
|---|---|
| **Anchor + one Pod per step** | chosen ([run model](run-model.md)) |
| One Pod, tasks as sequential initContainers | An image pull error stalls the whole Pod until its deadline; a failing init container fails the Pod, so every container must exit 0 and report its code elsewhere; no per-task deadline; `hostPID` for every task; no adaptive vLLM ladder or GPU fallback; later tasks cannot use what the probe found. |
| A Job per task | Adds retries and backoff to disable, and Job TTLs. Bare Pods with owner references suffice, because outcomes are recorded in object storage and the run status. |
| Argo Workflows, Hatchet | A cluster-wide controller (Hatchet: plus PostgreSQL) for a linear sequence that the controller handles in a few hundred lines. |

## Controller form (K2)

| Option | Why not |
|---|---|
| CronJob (plan) + Jobs | Every tick would launch and remove an `apps` node (the pool scales from zero), paying a full hour each time on hourly vendors; state between ticks would need a store. |
| On the control planes | The control planes are kept for cluster components only. |
| Operator framework (kopf, controller-runtime), leader election, HA | One replica with idempotent writes is enough; a dead controller only delays new work. |

## Run state (K27)

| Option | Why not |
|---|---|
| **`BenchmarkRun` custom resource** | chosen ([state model](state.md)) |
| Placeholder `meta.json` commits as a lock (sc-inspector) | A lock with a 2-hour TTL in a remote git repo: slow, conflict-prone, and a crash between commit and create blocks the SKU. |
| Anchor Pod name and annotations as the only state | Works for the lease, but a run spans several attempts (spot) and the anchor is deleted between them; Pod annotations make a poor status display (UIs cannot show dotted keys as columns). |
| A database | One more stateful service to run and back up, for state that Kubernetes already stores. |

## Outputs

| Option | Why not |
|---|---|
| **`sc-run` spool + presigned PUT/UploadPart to final keys** | chosen ([output pipeline](outputs.md)) |
| Inbox prefix, then copy to the archive after the git push | Two writes per object and a move step that can fail half-way; the final key can be known up front. |
| Presigned POST policy | Single request per file, no multipart, no part-level retry. |
| Controller reads `kubectl logs` | the container log merges stdout and stderr (geekbench parses stderr) and lags the files by up to a second, log rotation keeps 10 MiB × 5, logs vanish with the node, artifact files cannot be read. |
| Controller pulls files with `exec tar` | Binary streams through the API server and kubelet over the tailnet; fragile. |
| Cluster ServiceAccount token or temporary STS keys on benchmark Pods | A privileged benchmark container could use the whole credential scope for hours; needs SigV4 and refresh in the wrapper. |
| rclone, s5cmd, aws-cli, Fluent Bit | Need credentials, or (Fluent Bit) a thinly documented presigned mode in a much larger agent. |

## Other components not added

Kueue, the GPU Operator ([GPU](gpu.md)), Docker on nodes, an agent or uploader sidecar on bench
nodes, a resource-tracker DaemonSet, webhooks, a model-cache proxy, Crossplane or other infrastructure operators
([DBaaS](dbaas.md)), an out-of-cluster force-deleter ([monitoring](monitoring.md#alerts)), a web UI (postponed, K36).
