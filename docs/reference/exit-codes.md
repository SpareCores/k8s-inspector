# Exit codes

`meta.json` `exit_code` values. The meanings of sc-inspector's codes are kept; `-6` is new, `-2` is not used.

| Code | Meaning | `error_msg` | Planner retry |
|---|---|---|---|
| `0` | task succeeded | | none |
| `> 0` | the task's own exit code; `137` with `OOMKilled` when the container's cgroup recorded an OOM kill during the task and the task failed | `OOMKilled` if applicable | none until the task hash or `revision` changes |
| `256` | harness error: `sc-run` could not set up the task (rlimits, nice), or the controller failed handling the step; also every unfinished task of a run whose reconciles kept failing for 30 minutes (in a run that never had a node it is written to `schedule/` only, like a negative code) | message | none |
| `-1` | no node, or `sc-run` could not start the task's command (`error_msg` `cannot start command: ...`, published as `-1` instead of `256`), and the failure is final or not yet known to be temporary: a launch SCarpenter rejected for good (`LAUNCH_REJECTED`), a node that did not register or become Ready the second time (`BOOT_FAILURE`). Temporary launch failures (`ICE`, `QUOTA`, `DAILY_LIMIT`, `VENDOR`, `NO_OFFERING`, which includes an anchor that stays unschedulable without a NodeClaim for `deadlines.noOfferingMinutes`, and `ANCHOR_FAILED`) write no `-1` at all (except for a forced run) (a forced DBaaS run records a `NO_OFFERING` deferral keyed by vendor and SKU, which also defers server runs of that SKU for 24 h) ([launch retries](../explanation/launch-retries.md)) | `SCARPENTER_<LAUNCH_REJECTED\|BOOT_FAILURE>: <NodeClaim message>` (a forced run also `SCARPENTER_<ICE\|QUOTA\|NO_OFFERING...>`) | `LAUNCH_REJECTED` and `BOOT_FAILURE`: final until the hash or `revision` changes, re-checked after 30 days; any other `-1`: 2 h. A start-failure `-1` also counts `start_retries` along `last_attempt`, like `-4`. A DBaaS run's database that could not be created or used is `-1` too, with `DBAAS_CREATE_FAILED`, `DBAAS_CREATE_TIMEOUT`, `DBAAS_BOOTSTRAP_FAILED`, `DBAAS_NO_NETWORK` or `DBAAS_UNKNOWN_DATABASE` (2 h flat wait, then `-5` after three) A `-1` of a temporary reason (also the older `QUOTA_ZERO`, `LAUNCH_FAILED` and `BOOT_FAILURE: no node after ...`) is ignored: the SKU is due |
| `-3` | precheck contradicts the node (for example a GPU task on a node without an NVIDIA GPU) | reason | none |
| `-4` | the task never finished: node lost, spot interruption, image pull timeout, eviction, kubelet admission rejection (message of the rejection), step or anchor deadline, run cancelled, outputs lost | `SPOT_INTERRUPTED`, `NODE_LOST`, `IMAGE_PULL_TIMEOUT`, `EVICTED`, `DEADLINE_EXCEEDED`, `CANCELLED`, `OUTPUTS_LOST` | `start_retries += 1` (counted along `last_attempt`); `CANCELLED` of an operator cancel is never published (the SKU's previous result stays), a SIGTERM-derived `CANCELLED` is, in `schedule/<task>/meta.json` only and never over a good result of the same hash, without stdout/stderr; retried after 2 h (first), 6 h (second); a spot interruption is handled within the run first ([spot](../explanation/spot.md)) |
| `-5` | three `-4` or start-failure `-1` in a row | `exceeded 3 start retries` | never again until the hash or `revision` changes |
| `-6` | the task hit its own timeout; partial stdout/stderr are kept | `Execution timed out after <N>s` | none |

OOM detection: `sc-run` compares the `oom_kill` counter of its own cgroup (`memory.events`, path from
`/proc/self/cgroup`) before and after the task, so it sees a kill anywhere under the task, not only of the direct child
(the direct child is usually `resource-tracker`). Since Kubernetes 1.28 the kubelet sets `memory.oom.group=1` on
container cgroups, so the kernel kills every process of the task at once. The kernel skips tasks with
`oom_score_adj` `-1000` in a group kill, so `sc-run` writes `-1000` to its own `oom_score_adj` (after the task
started, so the task does not inherit it) and survives; it logs a warning if the write fails. If `sc-run` is killed anyway, there is no
`result.json`; the controller then uses the container's own `OOMKilled` termination reason and records `137` with
`OOMKilled`.

As the API shows an OOM (measured with `sc-run` surviving): the Pod is `Succeeded`, the container is `terminated` with
`exitCode: 0` and reason `OOMKilled` (the runtime marks a container `OOMKilled` whenever its cgroup recorded an OOM
kill, even though `sc-run` exited 0), and the truth is in the termination message: `exit_code: 137`,
`error_msg: OOMKilled`, `oom_kills` above 0. The controller uses the termination message (137); the container reason alone does not
mean `sc-run` died.

`DeadlineExceeded` on an idle node: the Pod is `Failed` with `status.reason: DeadlineExceeded` and message
`Pod was active on the node longer than the specified deadline`. The `sc-run` container is `terminated` with
`exitCode: 0`, reason `Completed`, ending 15 s after the SIGTERM (`--grace`); its message has `exit_code: 143`,
`error_msg: cancelled`, `cancelled: true`. A container without the wrapper ends 137 with reason `Error`. If the
container runtime is wedged the Pod can stay `Running` past its deadline; the controller's watchdog force-deletes it
and records `-4 DEADLINE_EXCEEDED`, unless the uploads verify and `result.json` has an exit code
([run model](../explanation/run-model.md#wedged-step-pods)).

Planner retry: a retry starts at the later of the backoff in the table (counted from the task's end) and the deletion
of the run that recorded the failure, because a SKU with a `BenchmarkRun` is never planned again; the run is deleted
`publish.keepRunHours` after publishing, so a retry waits at least `keepRunHours`.

DBaaS tasks follow the same rules (no endless retry); their `-1` and `-4` are counted per database in
`dbaas/<vendor>/<id>/postgres/<version>/standalone/<task>/meta.json`.

`IMAGE_PULL_TIMEOUT` applies to the containers of a step Pod that are still pulling (or creating) after
`imagePullMinutes`; the others of the Pod keep their results. Containers waiting with `CreateContainerConfigError`,
`InvalidImageName` or `ErrImageNeverPull` count as stuck, with the same deadline: they cannot recover. Their tasks
record the kubelet's waiting message as `error_msg` (not `IMAGE_PULL_TIMEOUT`), and unlike a stuck pull they get no
CPU fallback.

A step Pod with its own network namespace (`host_network: false`: `redis`, `static_web`) needs the CNI to create its
sandbox. When the CNI is down on the node, the Pod stays in `ContainerCreating`, so it is reported as
`IMAGE_PULL_TIMEOUT` after `imagePullMinutes`, although no image pull is involved.

Where the controller gets the facts: `sc-run`'s `result.json` and termination message (exit code, timeout, OOM),
container state and reason, Pod `DeadlineExceeded`, NodeClaim conditions and Events (capacity errors), the anchor's
presence (node loss).
