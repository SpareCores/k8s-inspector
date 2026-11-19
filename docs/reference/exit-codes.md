# Exit codes

`meta.json` `exit_code` values. The meanings of sc-inspector's codes are kept; `-6` is new, `-2` is not used.

| Code | Meaning | `error_msg` | Planner retry |
|---|---|---|---|
| `0` | task succeeded | | none |
| `> 0` | the task's own exit code; `137` with `OOMKilled` when the container's cgroup recorded an OOM kill during the task and the task failed | `OOMKilled` if applicable | none until the task hash or `revision` changes |
| `256` | harness error: `sc-run` could not start the command, or the controller failed handling the step | message | none |
| `-1` | no node: capacity, quota, no offering, boot failure, provisioning budget passed | `SCARPENTER_<ICE\|QUOTA\|QUOTA_ZERO\|NO_OFFERING\|BOOT_FAILURE>: <NodeClaim message>` | backoff 2 h, 6 h, 24 h; `QUOTA_ZERO` and `NO_OFFERING` only daily |
| `-3` | precheck contradicts the node (for example a GPU task on a node without an NVIDIA GPU) | reason | none |
| `-4` | the task never finished: node lost, spot interruption, image pull timeout, eviction, step or anchor deadline, run cancelled, outputs lost | `SPOT_INTERRUPTED`, `NODE_LOST`, `IMAGE_PULL_TIMEOUT`, `EVICTED`, `DEADLINE_EXCEEDED`, `CANCELLED`, `OUTPUTS_LOST` | `start_retries += 1`; backoff 2 h, 6 h, 24 h; a spot interruption is handled within the run first ([spot](../explanation/spot.md)) |
| `-5` | three `-4` in a row | `exceeded 3 start retries` | never again until the hash or `revision` changes |
| `-6` | the task hit its own timeout; partial stdout/stderr are kept | `Execution timed out after <N>s` | none |

OOM detection: `sc-run` compares the `oom_kill` counter of its own cgroup (`memory.events`, path from
`/proc/self/cgroup`) before and after the task, so it sees a kill anywhere under the task, not only of the direct child
(the direct child is usually `resource-tracker`). Since Kubernetes 1.28 the kubelet sets `memory.oom.group=1` on
container cgroups, which would make the kernel kill `sc-run` together with the task. `sc-run` is privileged: at start
it writes `0` to its cgroup's `memory.oom.group` and `-1000` to its own `oom_score_adj` (after the task started, so
the task does not inherit it), and logs a warning if either fails. If `sc-run` is killed anyway, there is no
`result.json`; the controller then uses the container's own `OOMKilled` termination reason and records `137` with
`OOMKilled`.

DBaaS tasks follow the same rules (no endless retry).

Where the controller gets the facts: `sc-run`'s `result.json` and termination message (exit code, timeout, OOM),
container state and reason, Pod `DeadlineExceeded`, NodeClaim conditions and Events (capacity errors), the anchor's
presence (node loss).
