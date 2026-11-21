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
| a node that launched but never registered | `BOOT_FAILURE` | temporary twice, permanent the third time |

The class is looked up anywhere in the message (SCarpenter may wrap it: `launching X: Retryable (...)`). A message
without a known class (an older SCarpenter) is `ICE` for a launch failure and `BOOT_FAILURE` for a registration
failure. A run that hits its provisioning budget without a recorded launch failure is `BOOT_FAILURE` only when a
NodeClaim reached `Launched=True` and never registered; otherwise it is `ICE` (Karpenter keeps replacing claims that
never launched, which is capacity or quota). After launch failures the run ends with the reason of the last one. A
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
| `QUOTA`, `VENDOR`, `BOOT_FAILURE` | 6 h |
| `NO_LSCPU` | 6 h; recorded by the publisher, not by a launch: a run ended without an `lscpu` result and its successful tasks were given up ([outputs](outputs.md#publishing-to-git)) |
| `NO_OFFERING` | 24 h at once (no doubling): where the offered-types filter fails open nothing else stops the retries |
| `DAILY_LIMIT` | `next` is the SKU's `until` in SCarpenter's offered-types ConfigMap when it has one (the vendor's reset), else 6 h |

SCarpenter's own short marks handle the fast path; this wait only bounds how often a failing SKU is tried. An entry
is removed when a node of the SKU is ready, or when the SKU is no longer in Keeper's ACTIVE list (the planner
prunes). Losing the ConfigMap only means an earlier retry.

The planner also skips a SKU that SCarpenter's offered-types ConfigMap lists as unavailable; the rules, and where
`until` comes from, are in [Offered types](../reference/configuration.md#offered-types). A damaged launch ConfigMap
(not a JSON object, entries that are not objects, a `next` that is not a timezone-aware time) is treated as having no
such entries. `k8s-inspector plan --dry-run` reads it but never prunes it.

## Permanent failure

One compact `-1 SCARPENTER_<reason>: <message>` schedule meta, as before. It is final until the task hash or
`revision` changes and is re-checked after 30 days ([exit codes](../reference/exit-codes.md)). The third
`BOOT_FAILURE` in a row is permanent; the SKU's ConfigMap entry is removed with any permanent failure.

## Older records

`-1` metas of temporary reasons written earlier (`ICE`, `QUOTA`, `NO_OFFERING`, `QUOTA_ZERO`, `LAUNCH_FAILED`, and the
`BOOT_FAILURE: no node after <N> minutes` of a run that only waited out its budget) are ignored by the
planner: the SKU is due. Failed BenchmarkRuns whose tasks all ended so are deleted by the publisher unpublished.

Metrics: `launch_failures_total`, `launch_deferred_skus`, `launch_blocked` ([monitoring](monitoring.md)).
