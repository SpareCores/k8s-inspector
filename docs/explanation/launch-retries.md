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
| the node is up but the anchor ended, vanished or never became Ready | `ANCHOR_FAILED` | temporary (6 h cap) |
| a node that launched but did not register, or registered but did not become Ready, within its limit, or was deleted before it did | `BOOT_FAILURE` | temporary once (24 h wait), permanent the second time |

The class is looked up anywhere in the message (SCarpenter may wrap it: `launching X: Retryable (...)`). A message
without a known class (an older SCarpenter) is `ICE` for a launch failure and `BOOT_FAILURE` for a registration
failure. A run whose NodeClaims never launch within the pre-launch budget (`provisionCpuMinutes`, `provisionGpuHours`)
is `ICE` (Karpenter keeps replacing claims that never launched, which is capacity or quota); after launch failures it
ends with the reason of the last one.

Every failed launch of the attempt's NodeClaims counts toward `MAX_LAUNCH_FAILURES` (2 distinct claims). When all of
them name the class `InsufficientCapacity` or `ZoneUnusable`, the run goes on instead, as long as SCarpenter's offered-types ConfigMap lists the SKU as available
(with `on-demand` among its capacity types when the attempt is on-demand only). SCarpenter tries the other regions
and zones itself. The controller confines a run only to the set of regions where its quota fits
([quota-aware planning](../reference/configuration.md#quota-aware-planning)), never to one region. The pre-launch budget (`provisionCpuMinutes`,
`provisionGpuHours`) still ends the run, as does `MAX_ICE_LAUNCH_FAILURES` (20 claims). If the ConfigMap cannot be
read, the cap of 2 applies to `ICE` too. A claim that hit InsufficientCapacity is deleted by Karpenter before any condition is visible, so the
controller reads it from the `InsufficientCapacityError` Event on the anchor Pod (`NodeClaim <name> nominated for this
pod failed to launch: <message>`; Karpenter v1.15 or newer). SCarpenter's message there is `insufficient capacity,
every launch attempt failed (N attempts), the last: <Class> (<Code>): ...` and carries only the last candidate's
class, or no class at all (`no candidate offering had quota headroom`, `no offering satisfies the NodeClaim's
requirements and has capacity`). The controller therefore treats every message starting `insufficient capacity,` as
`ICE` (never `LAUNCH_REJECTED`, so no on-demand fallback), except when the last candidate's class is `QuotaExceeded`
(`QUOTA`).

Karpenter creates the next claim only while it can still launch the SKU. When the attempt has a launch failure, no
NodeClaim is pending, `noOfferingMinutes` have passed since the attempt started and SCarpenter's offered-types
listing is readable and does not offer the SKU (the planner's rules, [Offered types](../reference/configuration.md#offered-types)),
no claim will follow and the run ends at once with the reason of the last failure, like any launch failure. A listing that is missing, unreadable or stale
is no answer: the run keeps waiting for the pre-launch budget.

The controller sees the Event on its next reconcile, which can be up to the 300 s resync after the claim failed, so a
3rd or 4th ICE claim may be launched before the cap applies. That costs nothing: an ICE claim never bills.

## A node that does not come up

The run ends `BOOT_FAILURE` at the first node of an attempt that does not come up, without waiting for Karpenter or the
pre-launch budget:

- a NodeClaim is `Launched=True` and not `Registered` the registration limit after its creation (`registrationMinutes`,
  8 for a plain VM; `registrationMinutesBareMetal`, 14, also used for GPU SKUs). The count starts at the claim's
  `creationTimestamp`, the start of Karpenter's own fixed 15 minute registration timeout, so the limit is always
  ahead of it;
- a NodeClaim is `Registered` and not `Initialized` (node Ready, startup taints removed, extended resources such as
  `nvidia.com/gpu` registered) the ready limit after it registered (`readyMinutes` 15, `readyMinutesBareMetal` 30,
  `readyMinutesGpu` 45; the Registered transition time, else the time the run first saw the claim launched);
- a NodeClaim that is Launched and not yet Initialized is being deleted or is gone, registered or not, unless it
  carries a spot interruption notice or is a spot claim (`karpenter.sh/capacity-type`; a reclaim is not a boot failure,
  and reclaimed claims are often finalised between two passes, so the run keeps each claim's capacity type; a claim
  without one counts as on-demand). A reclaimed claim is forgotten, but counted in `status.spotReclaims` (per attempt): at the second one the spot
  attempt ends and the next is on-demand only, where a claim lost before Ready is a boot failure, so a spot loop (for
  example a GPU check that fails on every VM) is bounded; a spot node that never comes up is still caught by
  the registration and ready limits while its claim exists, and by the pre-launch budget otherwise. Karpenter deletes
  a claim that does not register within its own 15 minutes, silently (no `Registered=False`, no Event) and creates a
  new claim for the pending anchor; SCarpenter deletes a GPU claim whose NVIDIA check fails or whose `gpu-pending`
  taint stays 30 minutes, which the 45 min GPU limit backs up. `gpu-pending` is a startup taint of the bench NodePool
  (with SCarpenter's release that adds it), which is why Karpenter's `Initialized` waits for it; the anchor does not
  tolerate it. The run keeps the names and Launched times of such claims in `status.uninitializedClaims` ({nodeClaim, launched, capacityType}) to notice a
  claim that vanished between reconciles; a claim that initializes is dropped from the list.

Once a claim of the attempt is Initialized, or the anchor has its node, other claims of the attempt are ignored
(Karpenter cleans them up). When a limit has passed the controller deletes the NodeClaim; while a limit is still
running it only queues the run again for the moment that limit expires. A spot attempt does not fall back to on-demand
for a boot failure: a node that never joined is not a capacity problem. How a SKU is classified is in
[configuration](../reference/configuration.md#deadlines). A forced run (`k8s-inspector run`) records its failure as a
`-1` instead of being deleted.

A node that came up but whose anchor Pod ended, vanished, or is still not Ready `anchorReadyMinutes` (15) after its node was Initialized
(an image pull error, a probe, a leftover taint), or a node older than the anchor, ends as `ANCHOR_FAILED`: temporary
like `ICE` (1 h, doubling, up to 6 h), never permanent, and not counted by the vendor breaker.

## Temporary failure

Nothing is written to sc-inspector-data. The BenchmarkRun is deleted at once (no 24 h lease; a run that already has
finished tasks is kept, they must be published). The SKU goes into the per-vendor ConfigMap
`k8s-inspector-launch-<vendor>`, key `skus`, JSON `{sku: {reason, since, count, next}}` (the key `regions` holds the
DBaaS regions blocked for the vendor, see [DBaaS](dbaas.md)). The planner skips a SKU
until `next`. The count is of failures in a row with the same reason (another reason starts again at 1). The wait is
1 h, doubling with each failure up to a cap:

| Reason | Cap |
|---|---|
| `ICE` | 24 h, never gives up |
| `QUOTA`, `VENDOR` | 6 h |
| `BOOT_FAILURE` | 24 h at once (no doubling): every try bills a VM that never joins |
| `NO_LSCPU` | 24 h at once (no doubling: a node clears the count every run, and a big SKU relaunched hourly costs money); recorded by the publisher, not by a launch: a run ended without an `lscpu` result and its successful tasks were given up ([outputs](outputs.md#publishing-to-git)) |
| `NO_OFFERING` | 24 h at once (no doubling) |
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

The planner also skips a SKU that SCarpenter's offered-types ConfigMap lists as unavailable, and a whole vendor whose
listing is missing, unreadable or stale; the rules, and where `until` comes from, are in
[Offered types](../reference/configuration.md#offered-types). A damaged launch ConfigMap
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
