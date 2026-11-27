# Spot strategy

Spot (OCI: preemptible) capacity costs 50-90 % less, and spot and on-demand VMs of a type are the same hardware, so the
results are comparable (the capacity type is recorded in `meta.json`). The risks are interruptions and capacity
shortages. The strategy keeps the savings and bounds the loss: **an interruption costs at most the task in flight, and
no SKU can loop on struggling spot capacity.**

## What the platform gives

- Spot exists at AWS, GCP, Azure, Alibaba Cloud and OCI. Hetzner, OVHcloud, UpCloud and Vultr have none
  ([vendors](../reference/vendors.md)).
- SCarpenter launches spot through `karpenter.sh/capacity-type`, with spot offerings in its catalog. When a Pod allows
  both `spot` and `on-demand`, spot is preferred and an unavailable spot offering falls back to on-demand by itself
  (offerings with capacity errors are skipped for 3 minutes).
- **Interruption handling is a planned SCarpenter feature**: per-vendor detection of the reclaim notice, then cordon
  and drain of the node, with the interruption reason visible on the NodeClaim and the Node. Notices are short (Azure
  30 s, AWS and OCI 2 min, GCP 0 or 2 min, Alibaba 5 min): too short to finish a task, long enough to flush its
  outputs.
- `karpenter.sh/do-not-disrupt` does not stop a reclaim or its drain.

## How k8s-inspector reacts to an interruption

| Signal | Reaction |
|---|---|
| The drain evicts the step Pod (SIGTERM, grace = the step's `terminationGracePeriodSeconds`, 20 s) | `sc-run` stops the task and flushes within `--grace` (15 s, counted from the SIGTERM): `result.json` (`cancelled: true`), the spool's last state and `manifest.json` reach object storage if the link allows, and the termination message is written first either way. |
| Interruption reason on the NodeClaim or Node | The controller classifies the attempt as `SPOT_INTERRUPTED` (not a generic node loss), records it, and starts the next attempt. |
| A task ends cancelled or evicted on a spot node that does not look reclaimed yet (an OS shutdown can end the container before the Node is deleted or the NodeClaim is marked) | The controller holds the outcome back for 5 minutes after the task's end (15 minutes while the Node's Ready condition is not True), looking again every 30 seconds. If the Node vanishes or shows the reason in that time, it is an interruption as above and the task runs again; otherwise the outcome is recorded (`-4 CANCELLED` or `-4 EVICTED`). |
| Node vanished without a notice (before the SCarpenter feature, or a missed notice) | Same classification from the capacity type of the lost node. |
| The anchor ended while its node is healthy | `NODE_LOST`: no spot record, no new attempt. |

A retried attempt aborts the in-flight task's open uploads, because the task runs again. When the run does not retry,
the controller waits until no step Pod can still upload and salvages the in-flight outputs
([output pipeline](outputs.md#what-can-still-be-lost)).

The design works without the SCarpenter feature (the 60 s re-PUT bounds the loss); the feature only makes the
classification exact and the flush complete.

## The strategy

1. **Per-task results are durable.** Each task's outputs and `status.json` are confirmed in object storage before the
   next task starts ([output pipeline](outputs.md)).
2. **Resume, do not restart.** After an interruption the same `BenchmarkRun` starts a new attempt (new anchor, new
   node) with only the remaining tasks. The probe step runs again on every attempt (it is short). A done task stays done (probe-step tasks excepted),
   even when the new node is a different hardware variant: the tasks left run on the new node, and each result is
   filed under the variant of the `lscpu` output of the attempt that ran it ([publishing](outputs.md#publishing-to-git)).
3. **Per-run spot budget.** A run allows spot for at most `maxSpotAttempts` (default 2) interrupted attempts and
   `maxLostMinutes` (default 60) of lost task time. When either is exceeded, the next attempt requires `on-demand`.
4. **Per-SKU memory.** The controller keeps per-SKU interruption counts in the ConfigMap `k8s-inspector-spot`. A SKU
   with `interruptionsToQuarantine` (default 2) interruptions within `quarantineDays` (default 14) starts on-demand
   for that period. Losing this ConfigMap is harmless: spot is tried again.
5. **A failed launch falls back to on-demand.** When a NodeClaim of a spot-allowed attempt fails to launch (two
   distinct `LaunchFailed` claims, or a rejection), the attempt ends `LaunchFailed`, and every later attempt of
   the run allows `on-demand` only. A node that never registers is not a capacity problem: it ends the run
   (`BOOT_FAILURE`, [launch retries](launch-retries.md)). The same failure of an on-demand attempt ends the run (`-1`, below).
6. **Provisioning budget.** An anchor allows `spot, on-demand`, so a spot shortage falls back by itself. If the anchor
   is still pending after the pre-launch budget, the attempt ends as `-1` like any capacity failure (backoff
   2 h, 6 h, 24 h).
7. **Long tasks run on-demand.** Tasks whose timeout exceeds `spotMaxTaskMinutes` (default 240, so `stressnglongrun`
   with 26 h) are never planned on spot; a run that contains one is on-demand.
8. **A busy spot quota defers the run.** When SCarpenter's listing says the spot quota of a SKU is held by other
   instances (but would fit), autoPlan does not create the run, so that it does not start on-demand; it waits for the
   quota and, if it is big (needs more than half of a limit) and waits longer than `quotaReserveAfterMinutes`, keeps
   the bucket free for the SKU in one region so the runs in flight drain ([quota-aware planning](../reference/configuration.md#quota-aware-planning)).
9. **Statistics are recorded.** `status.attempts` lists each attempt's node, the capacity type of that node (none for an
   attempt that never got one), duration and outcome.
   `meta.json` gets `capacity_type` and `spot_attempts`; the controller exports interruption counts per vendor and SKU
   as metrics ([monitoring](monitoring.md)). When a run ends, the publisher writes its attempts, interruptions
   included, to `runs/<run_id>.json` in sc-inspector-data, which is kept permanently
   ([run records](../reference/run-records.md)).

Worst case per run: two lost tasks plus two extra node boots, then the rest on-demand. There is no retry loop: the
attempt counter lives in the run object, and a run that exhausts its budgets either completes on-demand or ends with
`-4` per remaining task, which counts towards the usual cap of three before `-5` ([exit codes](../reference/exit-codes.md)).

## Spot capacity modes

`SpotFirst` lets Karpenter fall back to on-demand inside the same attempt whenever no spot node can launch (spot
quota busy, no capacity, no spot market), so a run can start on-demand without any spot attempt having failed. Two more
vendor settings keep a run on spot ([configuration](../reference/configuration.md#spot-capacity-modes)):

- `SpotOnly`: the anchor allows `spot` only, on every attempt. SKUs that cannot run on spot are not planned. An
  attempt that finds no spot capacity ends the run without results and the SKU is tried again in a later slot.
- `SpotUntilExhausted`: the same, until the SKU is exhausted: `patienceDays` (14) have passed since its first try
  and its tries covered `coverage` percent (100) of the week's `slotHours` (3) hour slots, so that time-of-week
  patterns of spot availability were given a chance. Then it behaves like `SpotFirst`. On-demand is allowed at once
  when spot is impossible for the SKU (no spot offering, or the spot quota can never fit one instance) and after
  `maxSpotAttempts` interruptions.

A **try** is a spot-only attempt launched while the spot quota had room that found no spot capacity in any region or
zone. It ends the run as `SpotUnavailable`: no task started, so no task result or `-1` is written, nothing is published
for the tasks and Keeper still sees them as due; the only trace is the attempt in the run's
[record](../reference/run-records.md). A rejected spot launch under `SpotOnly` ends the run the same way (`SpotRejected`) and keeps the SKU out of planning for
30 days; under `SpotUntilExhausted` it sends the next attempt to on-demand, as in `SpotFirst`. A busy spot quota is not a try (autoPlan waits for quota, item 8 above), nor is a
quota refusal at launch. After a try the planner offers the SKU again only in a slot it has not tried, so tries spread
over the week. A spot node that launches clears the SKU's tries. A quarantined SKU is not planned under `SpotOnly`
(it never runs on-demand); under `SpotUntilExhausted` it starts on-demand as in `SpotFirst`.

## Per-vendor policy

Spot is a per-vendor setting ([configuration](../reference/configuration.md)): `SpotFirst`, `OnDemand`, `SpotUntilExhausted` or `SpotOnly`. The initial
policy: `SpotFirst` for OCI (to exercise the strategy in the first phase, where interruptions are cheap), AWS and GCP;
`OnDemand` elsewhere until a vendor's spot lane in SCarpenter is proven (decision K20).
