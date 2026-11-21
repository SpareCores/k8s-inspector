# Launch retries

A server is never given up on because of a temporary launch error, and retrying costs neither a BenchmarkRun that
lingers, nor a git commit, nor unbounded state.

## The class decides

SCarpenter writes `<Class> (<Code>): <message>` on the NodeClaim launch condition. The controller maps the class to
a reason (no keyword guessing):

| Class | Reason | Kind |
|---|---|---|
| `InsufficientCapacity`, `ZoneUnusable` | `ICE` | temporary |
| `QuotaExceeded` | `QUOTA` | temporary |
| `DailyLimit` | `DAILY_LIMIT` | temporary |
| `Retryable` | `VENDOR` | temporary |
| no instance type for the SKU, or an anchor that stays unschedulable | `NO_OFFERING` | temporary (usually a short SCarpenter mark) |
| `Unsupported`, `InvalidOffering`, `Fatal` | `LAUNCH_REJECTED` | permanent |
| a node that launched but did not register, or registered but did not become Ready, within its limit | `BOOT_FAILURE` | temporary once (24 h wait), permanent the second time |

The class is looked up anywhere in the message (SCarpenter may wrap it: `launching X: Retryable (...)`). A message
without a known class (an older SCarpenter) is `ICE` for a launch failure and `BOOT_FAILURE` for a registration
failure. A run whose NodeClaims never launch within the pre-launch budget (`provisionCpuMinutes`, `provisionGpuHours`)
is `ICE` (Karpenter keeps replacing claims that never launched, which is capacity or quota); after launch failures it
ends with the reason of the last one.

## A node that does not come up

The run ends `BOOT_FAILURE` at the first node of an attempt that does not come up, without waiting for Karpenter or the
pre-launch budget:

- a NodeClaim is `Launched=True` and not `Registered` and either has a deletion timestamp or the registration limit has
  passed since its Launched transition (`registrationMinutes`, 8 for a plain VM; `registrationMinutesBareMetal`, 14);
- a NodeClaim is `Registered` and not `Initialized` (node Ready, startup taints removed, extended resources such as
  `nvidia.com/gpu` registered) the ready limit after it registered (`readyMinutes` 15, `readyMinutesBareMetal` 30,
  `readyMinutesGpu` 45);
- a NodeClaim that is Launched and not yet Initialized gets a deletion timestamp or is gone, registered or not, unless
  it carries a spot interruption notice (a reclaim is not a boot failure). Karpenter deletes a claim that does not
  register within its own fixed 15 minutes, silently (no `Registered=False`, no Event) and creates a new claim for
  the pending anchor; SCarpenter deletes a GPU claim whose NVIDIA check fails or whose `gpu-pending` taint stays 30
  minutes, which the 45 min GPU limit backs up. The run keeps the names and Launched times of such claims in
  `status.uninitializedClaims` to notice a claim that vanished between reconciles; a claim that initializes is
  dropped from the list.

The controller deletes the NodeClaim and queues the run again for the moment the limit expires. A spot attempt does
not fall back to on-demand for it: a node that never joined is not a capacity problem. How a SKU is classified is in
[configuration](../reference/configuration.md#deadlines). A
forced run (`k8s-inspector run`) records its failure as a `-1` instead of being deleted.

## Temporary failure

Nothing is written to sc-inspector-data. The BenchmarkRun is deleted at once (no 24 h lease; a run that already has
finished tasks is kept, they must be published). The SKU goes into the per-vendor ConfigMap
`k8s-inspector-launch-<vendor>`, key `skus`, JSON `{sku: {reason, since, count, next}}`. The planner skips a SKU
until `next`. The count is of failures in a row with the same reason (another reason starts again at 1). The wait is
1 h, doubling with each failure up to a cap:

| Reason | Cap |
|---|---|
| `ICE` | 24 h, never gives up |
| `QUOTA`, `VENDOR` | 6 h |
| `BOOT_FAILURE` | 24 h at once (no doubling): every try bills a VM that never joins |
| `NO_LSCPU` | 24 h at once (no doubling: a node clears the count every run, and a big SKU relaunched hourly costs money); recorded by the publisher, not by a launch: a run ended without an `lscpu` result and its successful tasks were given up ([outputs](outputs.md#publishing-to-git)) |
| `NO_OFFERING` | 24 h at once (no doubling): where the offered-types filter fails open nothing else stops the retries |
| `DAILY_LIMIT` | `next` is the SKU's `until` in SCarpenter's offered-types ConfigMap when it has one (the vendor's reset), else 6 h |

SCarpenter's own short marks handle the fast path; this wait only bounds how often a failing SKU is tried. An entry
is removed when a node of the SKU is ready, or when the SKU is no longer in Keeper's ACTIVE list (the planner
prunes). Losing the ConfigMap only means an earlier retry.

### Vendor breaker

An entry also records `last`, the time of its latest failure. A vendor with three SKUs whose entry is `BOOT_FAILURE` with
a `last` in the past 24 h is paused: autoPlan skips it and logs a warning every cycle, as nodes of the vendor do not
come up whichever the SKU. A node of the vendor becoming ready removes every `last` (and the SKU's own entry), which
ends the pause; the other entries keep their count and wait. The pause also ends 24 h after the newest failure. A
forced run is never blocked.

The planner also skips a SKU that SCarpenter's offered-types ConfigMap lists as unavailable; the rules, and where
`until` comes from, are in [Offered types](../reference/configuration.md#offered-types). A damaged launch ConfigMap
(not a JSON object, entries that are not objects, a `next` that is not a timezone-aware time) is treated as having no
such entries. `k8s-inspector plan --dry-run` reads it but never prunes it.

## Permanent failure

One compact `-1 SCARPENTER_<reason>: <message>` schedule meta, as before. It is final until the task hash or
`revision` changes and is re-checked after 30 days ([exit codes](../reference/exit-codes.md)). The second
`BOOT_FAILURE` in a row is permanent. The ConfigMap entry of a `LAUNCH_REJECTED` SKU is removed; that of a
`BOOT_FAILURE` SKU stays (it counts for the vendor breaker) until a node of the SKU is ready.

## Older records

`-1` metas of temporary reasons written earlier (`ICE`, `QUOTA`, `NO_OFFERING`, `QUOTA_ZERO`, `LAUNCH_FAILED`, and the
`BOOT_FAILURE: no node after <N> minutes` of a run that only waited out its budget) are ignored by the
planner: the SKU is due. Failed BenchmarkRuns whose tasks all ended so are deleted by the publisher unpublished.

Metrics: `launch_failures_total`, `launch_deferred_skus`, `launch_blocked`, `launch_paused` ([monitoring](monitoring.md)).
