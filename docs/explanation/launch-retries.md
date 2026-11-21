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
| `Unsupported`, `InvalidOffering`, `Fatal` | `LAUNCH_REJECTED` | permanent |
| a node that launched but never registered | `BOOT_FAILURE` | temporary twice, permanent the third time |

A message without a known class (an older SCarpenter) is `ICE` when the launch failed and `BOOT_FAILURE` when the
node never registered or no node came up and a NodeClaim exists. A run that hits its provisioning budget after
launch failures ends with the reason of the last launch failure, not `BOOT_FAILURE`.

## Temporary failure

Nothing is written to sc-inspector-data. The BenchmarkRun is deleted at once (no 24 h lease; a run that already has
finished tasks is kept, they must be published). The SKU goes into the per-vendor ConfigMap
`k8s-inspector-launch-<vendor>`, key `skus`, JSON `{sku: {reason, since, count, next}}`. The planner skips a SKU
until `next`. The wait is 1 h, doubling with each failure in a row up to a cap:

| Reason | Cap |
|---|---|
| `ICE` | 24 h, never gives up |
| `QUOTA`, `VENDOR`, `BOOT_FAILURE` | 6 h |
| `DAILY_LIMIT` | 1 h: SCarpenter's offered-types entry (`reason`, `until`) keeps the SKU out until the vendor's reset |

SCarpenter's own short marks handle the fast path; this wait only bounds how often a failing SKU is tried. An entry
is removed when a node of the SKU is ready, or when the SKU is no longer in Keeper's ACTIVE list (the planner
prunes). Losing the ConfigMap only means an earlier retry.

The planner also skips a SKU whose offered-types entry is unavailable with an `until` still ahead; an `until` that
has passed is ignored, a missing or unreadable one changes nothing.

## Permanent failure

One compact `-1 SCARPENTER_<reason>: <message>` schedule meta, as before. It is final until the task hash or
`revision` changes and is re-checked after 30 days ([exit codes](../reference/exit-codes.md)). The third
`BOOT_FAILURE` of a SKU is permanent; its ConfigMap entry is removed then.

## Older records

`-1` metas of temporary reasons written earlier (`ICE`, `QUOTA`, `QUOTA_ZERO`, `LAUNCH_FAILED`) are ignored by the
planner: the SKU is due. Failed BenchmarkRuns whose tasks all ended so are deleted by the publisher unpublished.

Metrics: `launch_failures_total`, `launch_deferred_skus`, `launch_blocked` ([monitoring](monitoring.md)).
