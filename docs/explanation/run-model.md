# Run model

**One run attempt = one fresh node = one anchor Pod plus one Pod per step** (decision K1, accepted).

## Why a node per run

A benchmark needs the whole machine and a clean state, so a node is never shared between runs or reused after one.
Bench NodePools have billing retention off, and the controller checks that the anchor's node was created after the
anchor (otherwise it deletes the anchor and that NodeClaim and tries again; the third failed attempt ends the run).

## Anchor

The anchor is the only Pod the scheduler and SCarpenter ever see for a run.

- `nodeSelector`: `karpenter.sh/nodepool: bench-<vendor>[-lowmem|-gpu]` and
  `node.kubernetes.io/instance-type: <vendor>.<sku>`, plus a `karpenter.sh/capacity-type` requirement set by the
  [spot strategy](spot.md).
- It holds the node: a Running Pod keeps the node non-empty, and `karpenter.sh/do-not-disrupt: "true"` blocks
  consolidation and drift. It does **not** block spot reclaim, `expireAfter` or a manual delete.
- Its container is `sc-run hold`: it first installs the `sc-run` binary into a hostPath directory that the steps
  mount, then sleeps. It exits by itself when no heartbeat has been seen for 45 minutes, so a dead controller
  cannot keep a node (decision K14). The heartbeat is the mtime of the file `heartbeat` in the run's hostPath
  directory (`/var/lib/k8s-inspector/runs/<run_id>`): `sc-run exec` touches it every 10 s while a task runs, and
  `kubectl exec <anchor> -- /sc-run heartbeat <dir>` touches it between steps. The idle clock starts when the anchor
  starts.
- Releasing the node = deleting the anchor (or its own exit). Terminal Pods do not count, so the node is empty and
  `WhenEmpty`/`consolidateAfter: 0s` removes it. Measured on OCI: about 1 minute to Node removal, 4-8 minutes until the
  NodeClaim is gone.

## Steps

- Each step is a bare Pod with `spec.nodeName` set to the anchor's node: no scheduler, no SCarpenter involvement. The
  kubelet still admits it on resources and `maxPods`. Step Pods are owned by the anchor, so deleting the anchor
  removes them.
- A step is one serial task, or the parallel hardware-probe group (dmidecode, lshw, lscpu, lstopo, virtualization,
  nvidia_smi, gpu_facts) as one Pod with several containers. This is today's priority-0 parallel group.
- Each step has its own `activeDeadlineSeconds` (pull + version + task timeout + upload + slack), memory limit,
  `hostPID` (storage only) and runtime class (GPU only).
- The controller sequences steps and makes the adaptive decisions that need earlier results: hardware variant after
  the probe step, GPU-first-then-CPU retry, vLLM image ladder, nvbandwidth buffer size (ported from sc-inspector).
- The step in flight (containers, images, commands, GPU runtime) is stored in `status.step` before its Pod is created.
  A restarted controller creates or watches exactly that step; it never plans it again, so a changed catalogue or
  an adaptive decision cannot alter what a Pod already runs.

Pod specs: [Kubernetes objects](../reference/objects.md).

## Memory pressure and shutdown inside a step

`sc-run` shares the task's container cgroup. It sets its own `oom_score_adj` to `-1000` after starting the task,
and the kernel's OOM kill (including the group kill) skips such tasks, so it takes the task and not `sc-run`, which can then still write
`result.json` and the termination message (details and the fallback to the container's `OOMKilled` reason:
[exit codes](../reference/exit-codes.md)).

On SIGTERM (eviction, Pod deletion, spot drain) one deadline counts from the signal: `--grace` (15 s) covers stopping the
task (SIGTERM, SIGKILL after 5 s, also during the version command), the final upload and `manifest.json`, which gets
the last quarter. `result.json` and the termination message are written before the upload starts, and the termination
message is updated afterwards if time allows. Step Pods use `terminationGracePeriodSeconds: 20`. Permanent upload
errors (no URL for a file, a 4xx other than 403, 408 and 429) are not retried: the file is listed as missing in the
manifest. `exec` starts with an empty spool, so a rerun never mixes its output with an earlier attempt's.

## Step overhead

Every step adds a little idle time on the node between two benchmarks. Its parts:

| Part | Typical | How it is kept small |
|---|---|---|
| Controller notices the previous step ended | ~1 s | a **watch** on the run's Pods wakes the controller; a 15 s poll would add ~7.5 s on average |
| Controller confirms uploads, writes `status.json`, signs URLs for the next step | ~1 s | a handful of object storage calls |
| Pod creation → kubelet sees it | <1 s | `nodeName` skips scheduling |
| Pod sandbox | ~1 s | `hostNetwork: true` needs no CNI setup |
| Wrapper delivery | 0 s | `sc-run` is installed once per node by the anchor into a hostPath and mounted read-only; no per-step initContainer |
| Container start | <1 s | image pulled by digest with `IfNotPresent` |
| Kubelet reports the end | ~1 s | |

That is about **3-5 seconds per step**, under 2 minutes for a full run of about 20 steps, next to 2-3 minutes of node
boot. A naive version (polling every 15 s and an initContainer per step) would cost about 10 s per step, 3-4 minutes
per run.

**Image pulls** are not step overhead: the VM era pulled each image before its task too. Pulls happen between
benchmarks, never during one. Pre-pulling all of a run's images at node start would only move the time, and it would
fill the disk before `llm` needs it, so it is not planned.

Not worth doing: baking `sc-run` into every benchmark image (an sc-images change for every image to save nothing the
hostPath install does not already save), or packing several tasks into one Pod (loses per-task isolation, deadlines
and adaptive decisions; see [alternatives](alternatives.md)).

## Lifecycle of an attempt

```mermaid
stateDiagram-v2
  [*] --> Provisioning: anchor created
  Provisioning --> Running: anchor Running on a fresh node
  Provisioning --> Failed: budget passed / ICE / quota (-1)
  Running --> Running: next step
  Running --> Releasing: all tasks done, uploads confirmed
  Running --> Interrupted: node or anchor gone (spot, expiry, manual delete)
  Releasing --> [*]: anchor deleted, node removed
  Interrupted --> [*]: remaining tasks go to the next attempt or end as -4
```

A run has one or more attempts. Only the [spot strategy](spot.md) and a failed provisioning (a node older than its
anchor, a vanished anchor) create more than one. An anchor that ends while its node is healthy is a `NODE_LOST` attempt without a spot
record.

A run whose reconciles keep failing for 30 minutes (a time window, not a count: a short API or S3 outage does not
end a run) ends `Failed` with the attempt outcome `HarnessError`: its unfinished tasks are `256` (harness error) and
the status is recorded first. The urls Secret is deleted at that moment; no Pod is: the next pass removes the anchor,
so the SKU lease is released.

An attempt that is lost after every task is done goes to `Publishing` without a retry.

## Teardown layers

| # | Mechanism | Covers |
|---|---|---|
| 1 | Controller deletes the anchor → empty node → `WhenEmpty` | normal path |
| 2 | Step `activeDeadlineSeconds` (kubelet) and the `sc-run` timeout | hung task, stuck upload |
| 3 | Anchor `activeDeadlineSeconds` and the `sc-run hold` idle timeout | dead or stuck controller; enforced on the node |
| 4 | NodePool `expireAfter: 48h` (the longest run, with the 26 h `stressnglongrun`, needs up to ~44 h) + `terminationGracePeriod: 1h` | an anchor held forever |
| 5 | SCarpenter garbage collection of tagged instances without a NodeClaim | controller crash during launch |
| 6 | Alert "bench node running too long" ([monitoring](monitoring.md)) | everything above failed, cluster or SCarpenter bug |

There is no out-of-cluster force-deleter (decision K22): an alert and a human come first.
