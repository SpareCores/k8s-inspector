# Run records

One JSON file per run in sc-inspector-data keeps the run's attempt history, including spot interruptions, for good.
`run.json` in object storage holds the same facts, but it expires with the rest of `logs/` after 90 days.

## Paths

| Run | Path |
|---|---|
| Server | `data/<vendor>/<sku>/runs/<run_id>.json` |
| DBaaS | `dbaas/<vendor>/<id>/runs/<run_id>.json` |

For DBaaS, `<vendor>/<id>` is the database's, the same as in the results path
`dbaas/<vendor>/<id>/postgres/<ver>/standalone/…` (`runs/` sits next to `postgres/`). A server run's `<sku>` is the
SKU it ran on. `runs/` is not a number, so it is not a hardware variant directory; sc-crawler lists the variant
directories of a SKU by `p.is_dir() and p.name.isdigit()` (`sc_crawler/inspector.py`, `_server_variants`) and reads
DBaaS results by explicit paths, so it never sees `runs/` ([output contract](output-contract.md#sc-inspector-data)).

## Content

The record is the controller's [`run.json`](output-contract.md#runjson) without every `lscpu_stdout` field (top level
and per attempt) plus `phase`, with `variant` set as below. It is written with sorted keys and two-space indentation, so diffs are stable.

| Field | Meaning |
|---|---|
| `vendor`, `sku` | vendor and SKU of the node the run ran on (for a DBaaS run: of the client node) |
| `run_id` | the run, `r<YYYYMMDD>t<HHMM>-<hex>` |
| `phase` | the run's final phase: `Done`, `Failed` or `Cancelled` (a run whose last attempt is `SpotUnavailable` or `SpotRejected` is `Failed`: tell it apart by the attempt outcome, nothing failed); `unknown` in a [backfilled](#backfill) record whose attempts do not tell |
| `plan` | task names of the run, in order |
| `variant` | the hardware variant number the run's results were filed under, `null` when none (DBaaS: always). Not `run.json`'s own `variant`, which is the runner's last snapshot: the publisher decides the variant later. Live: the run's `status.variant`; backfilled: the number of the directory holding the run's `timing/<run_id>/` in the tree (`data/<vendor>/<sku>/<N>/timing/<run_id>/`), `null` when there is none |
| `family`, `series`, `cpu_arch`, `gpu_count`, `vcpus`, `memory_gib` | Keeper's `family` (e.g. `Compute Optimized`), `series` (e.g. `c6a`), `cpu_architecture`, `accelerator_count`, `vcpus` and `memory_amount` of the SKU (the client node for a DBaaS run), read once when the run's first attempt gets its node; `memory_gib` is Keeper's `memory_amount` (MiB) divided by 1024. A value Keeper lacks is `null`; all are absent when Keeper could not be asked, and the publisher and the [re-emit](#re-emit) fill them from Keeper's current data when `vcpus` is absent (a run launched before it existed, or an outage at launch) |
| `database` | `null` for a server run; for DBaaS the database object (`vendor`, `id`, `engineVersion`, `region`, `vcpus`, `memoryGib`, `usdPerHour`) |
| `timing` | the latest attempt's `api_start`, `api_end`, `machine_start`, `node_registered`, `node_ready` (ISO UTC; only those known) |
| `attempts[]` | one entry per node the run used, oldest first |

An attempt:

| Field | Meaning |
|---|---|
| `attempt` | number, from 1 |
| `capacity_type` | `spot` or `on-demand`; `null` for an attempt that never got a node (also a `SpotUnavailable` or `SpotRejected` one) |
| `region`, `zone` | where the node ran; only on an attempt that got a node, absent otherwise |
| `price` | the node's price per hour in USD as a string, same availability as `region` |
| `ondemand_price` | USD per hour of the on-demand price of the same SKU in the attempt's region, from Keeper (ACTIVE `ondemand` price rows, the cheapest zone), captured when the attempt gets its node, so it is the price at launch time. `null` when Keeper has no on-demand offer there; absent when Keeper could not be asked |
| `ondemand_price_reason` | why `ondemand_price` is `null`: `no_ondemand_offer` (Keeper has no on-demand price of the SKU in that region), `region_not_in_keeper` (the node's region is not a Keeper region of the vendor), `keeper_unknown_sku`, `keeper_unreachable`. Keeper is asked once per attempt: these `null`s are not retried live, the [re-emit](#re-emit) fills the last two later |
| `duration_s`, `useful_s`, `setup_s`, `task_s`, `cost_usd`, `wasted_usd` | what the attempt cost and how much of it was useful, see [Cost log lines](#cost-log-lines); written into the record, not into `run.json` |
| `node`, `node_claim` | names of the Kubernetes node and its NodeClaim; `null` without a node |
| `api_start` | the attempt's anchor Pod was created (the start of the attempt) |
| `api_end`, `machine_start`, `node_registered`, `node_ready` | NodeClaim launched, kernel boot, node registered, node ready; `null` when not reached |
| `end` | when the attempt ended (ISO, with offset); `null` while open |
| `outcome` | how the attempt ended, see below; `null` while open |
| `spot_try` | only on an attempt that ended `SpotUnavailable`: the evidence of the try, see [below](#spot-unavailable-attempts) |
| `spot_rejected` | only on an attempt that ended `SpotRejected`: `{error, launch_errors}`, the rejection SCarpenter reported and the failed NodeClaims the controller saw (shaped like `error` and `launch_errors` of `spot_try`) |

Attempt `outcome` values:

| Outcome | Meaning |
|---|---|
| `Completed` | the run's tasks finished on this node |
| `SpotInterrupted` | the spot node was reclaimed; another attempt followed or the run ended |
| `NodeLost` | the node disappeared for another reason |
| `ProvisioningFailed` | no node could be provisioned (the run failed) |
| `LaunchFailed` | the NodeClaim of a spot-allowed attempt failed to launch or boot; on-demand next |
| `SpotRejected` | a [`SpotOnly`](configuration.md#spot-capacity-modes) attempt's spot launch was rejected for the SKU (an offering the cloud does not support); no task started and the run ended, the SKU is not planned under `SpotOnly` for 30 days. Not a try |
| `SpotUnavailable` | a spot-only attempt ([`SpotUntilExhausted`, `SpotOnly`](configuration.md#spot-capacity-modes)) found no spot capacity in the regions the run was confined to (where the spot quota fitted) while it had room, with evidence that SCarpenter tried offerings; no task started and the run ended, the SKU is tried again in a later slot. Counts as a try towards the SKU being exhausted |
| `Cancelled` | an operator cancelled the run |
| `HarnessError` | the controller gave up on the run |

## Spot unavailable attempts

A run that ends `SpotUnavailable` is a **try** to launch the SKU on spot, one run and one record file per try. It has
one attempt of that outcome as its last. Earlier attempts can exist: a `SpotInterrupted` one, or `ProvisioningFailed` ones (the controller retries an attempt that never got a usable node, a few times; the try is the attempt that follows them). The last attempt has no node, no `region`, `zone`
or `price`, and `plan` lists the tasks that were not executed. Nothing else is written for it: no task result, no
`schedule/` meta, no exit code, so nothing is a benchmark failure and the tasks stay due for Keeper and the planner
exactly as before the try. The calendar slot of a try is not stored: derive it from the attempt's `api_start` (UTC weekday
and `hour // 3` with the default `slotHours`).

`api_start` and `end` are the start and end of the try. `spot_try` is:

| Field | Meaning |
|---|---|
| `error` | the SCarpenter error that ended the attempt (`SCARPENTER_ICE: <message>`), a string. The message is Karpenter's prefix plus SCarpenter's own text: `insufficient capacity, <Class> (<Code>): <cloud error>; tried N offerings in R regions / Z zones; classes: <Class>:<n>,...; stopped by: <reason>`. Only the last candidate's cloud error is in it |
| `launch_errors` | `[{node_claim, time, message}]`: the NodeClaims of the attempt that failed to launch and that the controller saw (a `LaunchFailed` condition, or Karpenter's `InsufficientCapacityError` event on the anchor), oldest first; possibly empty. `message` is SCarpenter's launch error of that claim |
| `tried` | `{offerings, regions, zones}`: how many offerings, regions and zones SCarpenter tried, the largest numbers of its summaries in `error` and `launch_errors` (a try counts only when the error's `classes:` or a launch error names `InsufficientCapacity` or `ZoneUnusable`; a summary of only `Unsupported`, `InvalidOffering`, `Fatal` or `NotFound` is a rejection, not a try); `null` when no message has a summary (an older SCarpenter). The cloud's error per region is not available to the controller today. SCarpenter plans per-candidate launch outcomes; they will be added to `tried` (consumers: ignore keys you do not know) |
| `regions` | the regions the run was confined to (`RunSpec.regions`, those where the spot quota fitted when it was planned); empty: it was not confined. The try covers these regions, not every region of the vendor |
| `quota` | the SKU's spot quota entries in SCarpenter's offered-types listing when the attempt ended, each `{bucket, unit, need, maxLimit, maxHeadroom, regions}` (numbers; `regions` is `{<region>: {limit, headroom}}`, empty for a global bucket or when SCarpenter lists none). It is the proof that the quota had room: a try counts only when the entry's headroom is at least `need`, for a regional entry the best headroom among `regions` of the run (all listed regions when it was not confined), else `maxHeadroom`. An empty list: the listing had no quota for the SKU |
| `quota_at` | when SCarpenter wrote that listing (its `generatedAt`, ISO with offset), `null` when the listing was unusable |

A launch whose `classes:` name only rejections (`Unsupported`, `InvalidOffering`, `Fatal`, `NotFound`; the cloud refuses the offering, so under `SpotOnly` the attempt ends `SpotRejected` and under `SpotUntilExhausted` the next attempt is on-demand), a launch that ended `ICE` while the quota showed no room, one that ended with `no candidate offering had quota headroom`, a provisioning deadline without evidence of tried offerings, and a quota refusal are not tries:
they are recorded nowhere, like any temporary launch failure ([what is not recorded](#what-is-not-recorded)).

## When a record is written

The publisher writes the record, in its own commit `k8s-inspector: run records (<run ids>)`, when all of these hold:

- the run is `Done`, `Failed` or `Cancelled`;
- all its tasks are published or given up (a cancelled run's discarded tasks count as such);
- its SKU (DBaaS: database) is enabled for publishing ([configuration](configuration.md)), the same rule as for
  results.

A `SpotUnavailable` or `SpotRejected` run has no tasks to publish, so its record is written as soon as the run is
`Failed`, and the run is deleted right after the record is in git (at once when its SKU is not enabled for publishing).

Write once: when the file is already in the tree it is not rewritten. The controller does not delete a finished,
publishable run (`keepRunHours`) before its record is in git, also after a controller restart. A run whose `run.json`
is gone or unusable (malformed, or without a safe vendor and SKU) gets no record (logged as an error) and is deleted
as usual; a storage or rendering error is retried every cycle.

## What is not recorded

- Runs deleted by the launch deferral (a temporary launch failure such as no capacity, except the counted tries of the spot-only modes, which are recorded as `SpotUnavailable`: the `BenchmarkRun` is deleted
  and the SKU waits for its backoff, [launch retries](../explanation/launch-retries.md)), Failed runs whose tasks
  all ended `-1` with a temporary launch reason, and runs deleted because Keeper no longer lists their SKU.
- Runs of SKUs that are not enabled for publishing.
- Runs of the legacy sc-inspector, which has no `run.json`.
- Runs older than the 90 days object storage keeps, and anything `run.json` never held.

## Backfill

`publish --backfill-runs --since 120d` writes the records of past runs from the `run.json` files under
`logs/inspect/<vendor>/` of the enabled vendors (`--since` is capped by the 90-day expiry in practice). It skips runs
that still exist in the cluster (their record comes when they end) and records git already has, and commits in
batches of 200. It can be rerun at any time. The phase is derived from the last attempt's `outcome`: `Completed` is
`Done`, `Cancelled` is `Cancelled`, `ProvisioningFailed`, `SpotUnavailable`, `SpotRejected` and `HarnessError` are `Failed`, anything else is
`unknown`. A run the launch deferral deleted has no `run.json` (skipped) or an open or `NodeLost` last attempt (`unknown`); it is never `Failed`. A malformed `run.json` is skipped with a warning.

Run it in the controller Pod, which has the object storage role, the deploy key and the cluster permissions:

```sh
kubectl -n inspector exec deploy/k8s-inspector -- k8s-inspector publish --backfill-runs --since 120d
```

`kubectl create job --from` does not apply: it needs a CronJob, and the controller is a Deployment. The command
prints `wrote <n> run records`.

## Cost log lines

When a run record is written (once per run, after its commit is pushed) the controller logs what the run's attempts
cost, one `run_attempt_cost` line per attempt, one `task_cost` line per task that ran and one `run_cost` line for the run, at info level in the normal JSON
log (`msg`, `event` and the fields below next to `ts`, `level` and `logger`). A record already in git is not logged
again, also after a controller restart.

Definitions, per attempt (`start` is `api_start`):

- `duration_s`: `end` minus `start`.
- `price`: the node's price per hour as a number (`null` for an attempt that never got a node).
- `cost_usd` = `duration_s` / 3600 × `price`; 0 for an attempt without a node (it has no `region`, `zone`, `price` or
  `ondemand_price` either, and does not set `final_region`) or without an `end`. An attempt without a node also wastes
  0.
- `useful_s`: for a `Completed` attempt its `duration_s`; otherwise the seconds from `start` to the end of the last
  task of that attempt that finished (exit code 0 or more) and whose result was kept (published, not discarded); 0
  when it kept none. A task the controller wrote off (`-4`, `-1`) is never kept and does not finish (a `-4` task that was
  running keeps its real start and gets a `task_cost` line, a task that never started has start and end of the same
  moment). An attempt that ran no task (no node, `ProvisioningFailed`, `LaunchFailed`, `SpotUnavailable`, `SpotRejected`) has 0,
  also when the task files are unknown (a backfill). Task times and
  attempt numbers are the run's `status.tasks`, which hold the latest execution of each task: a task that ran again
  on a later attempt counts there only.
- `setup_s`: `start` to the first start of a finished task of the attempt (kept or not); `null` when there is none.
- `task_s` = max(`useful_s` − `setup_s`, 0): the kept task time (a `null` `setup_s` counts as 0).
- `wasted_usd`: 0 for the `Completed` attempt; otherwise `cost_usd` − `task_s` / 3600 × `price`, so the attempt's
  setup and the time after its last kept task are waste.

`run_attempt_cost`:

| Field | Value |
|---|---|
| `msg`, `event` | `run attempt cost`, `run_attempt_cost` |
| `run`, `vendor`, `sku`, `family`, `series` | run id, the run's vendor and SKU, Keeper's family and series (`null` when unknown) |
| `vcpus`, `memory_gib` | the record's (`null` when Keeper lacks them) |
| `attempt` | number, from 1 |
| `node`, `nodeclaim` | the attempt's `node` and `node_claim` names (`null` without a node) |
| `region`, `zone`, `capacity_type`, `outcome` | as in the attempt (`null` when absent) |
| `start`, `end` | the attempt's `api_start` and `end` |
| `duration_s`, `useful_s`, `setup_s`, `task_s`, `price`, `ondemand_price`, `cost_usd`, `wasted_usd` | as defined above; `ondemand_price` is the attempt's |
| `ondemand_price_source` | `launch`: captured when the attempt got its node; `current`: Keeper's price when the record or the backfill was written, for an attempt launched without one (an older controller, or Keeper unreachable); after one failed Keeper call a pass does not ask again, and the remaining attempts keep `ondemand_price_reason: keeper_unreachable`; an attempt that got a node always has `ondemand_price` or an `ondemand_price_reason` |
| `start_hour_utc`, `start_weekday_utc` | hour `0`-`23` and weekday `Mon`-`Sun` of `start`, UTC |
| `backfill` | `false` |

`task_cost`, one line per task execution that ran on a node of the run that has a `price` (the latest execution of each task, like
`useful_s`), including failed ones and the vLLM probes (`vllm-probe-<image>`); a task that never started (no node, precheck, written off before it ran: start and
end are the same moment) has none:

| Field | Value |
|---|---|
| `msg`, `event` | `task cost`, `task_cost` |
| `run`, `attempt`, `task` | run id, the attempt the task ran in, task name |
| `vendor`, `sku`, `family`, `series` | as above |
| `region`, `zone`, `capacity_type` | of the attempt |
| `start`, `end`, `duration_s` | the task's start and end (ISO, UTC) and their difference in seconds |
| `price` | the attempt's `price` (USD per hour) |
| `cost_usd` | `duration_s` / 3600 × `price` |
| `exit_code`, `result`, `error_msg` | the task's [exit code](exit-codes.md), `ok` (`0`), `unfinished` (`-4`) or `failed` (any other code, such as the task's own, `256` or `-6`), and the task's `error_msg` (`null` when none): for `-4` the reason (`SPOT_INTERRUPTED`, `NODE_LOST`, `DEADLINE_EXCEEDED`, `CANCELLED`, ...) |
| `backfill` | `false` |

`cost_usd` of a task is the node's price for the time the task ran. Tasks that run in parallel on the node (the
containers of the probe step) each get their own duration times the full node price, so their lines overlap in time
and do not sum to the attempt's `cost_usd`; neither does the sum of all tasks, which leaves out the setup and the idle
time between tasks. In a [re-emit](#re-emit) the tasks come from the `meta.json` files of the run id in the tree, so a
task whose result was replaced by a later run, a `-4` task (never filed as a result) and a vLLM probe have no line.

`run_cost`:

| Field | Value |
|---|---|
| `msg`, `event` | `run cost`, `run_cost` |
| `run`, `vendor`, `sku`, `family`, `series` | as above |
| `final_region`, `final_capacity_type` | of the last attempt |
| `attempts`, `spot_interruptions` | attempts, and those that ended `SpotInterrupted` |
| `used_spot` | some spot attempt got a node; a spot attempt that never got one does not count; every run is logged, so filter on it |
| `paid_usd`, `wasted_usd` | sums over the attempts |
| `ondemand_counterfactual_usd` | price of the reference attempt × (sum of `task_s` over all attempts + `setup_s` of the reference attempt) / 3600: on-demand would need one setup plus the kept task time. The reference attempt is the `Completed` one, else the last attempt with kept task time (`task_s` > 0; the results a run kept are published, so it did useful work) Its price is its own `price` when it is on-demand, else its `ondemand_price`. `null` without a reference attempt (a run with no kept task time), without its price, or when a task time is unknown |
| `spot_net_usd` | `paid_usd` − `ondemand_counterfactual_usd`, `null` with it; positive means spot cost more than on-demand would have |
| `backfill` | `false` |

`price` is the node's actual price including extras (such as disk); `ondemand_price` is Keeper's list price of the
server, so small gaps between them are expected. That is why an on-demand `Completed` attempt counts with its own
`price` in the counterfactual: the on-demand alternative is then exactly as dear as the attempt it replaces. A
`null` `setup_s` counts as 0.

The node prices of some vendors include extras, such as the boot volume, that Keeper's on-demand list price does not,
so small gaps are expected, and for a small shape spot can show slightly dearer than on-demand.

Lines exist only for runs that get a record, that is runs of vendors and SKUs enabled for publishing. They are
logged after the record's commit is pushed, so a controller crash between the push and the logging loses that run's
lines until the next re-emit.

### Re-emit

```sh
kubectl -n inspector exec deploy/k8s-inspector -- sh -c 'k8s-inspector publish --emit-cost-logs --since 120d 2>/proc/1/fd/2'
```

logs the cost lines of the run records in git whose run id date is within `--since`, with `"backfill": true`. The
`2>/proc/1/fd/2` is needed because a process started by `kubectl exec` logs to its own terminal, not to the
container log that the log shipping reads; it works because the exec'd process and PID 1 run as the same user
(`runAsUser: 10001` in the Deployment, `USER 10001` in the image). The command prints `logged the cost lines of <n>
runs`. It changes nothing in git.

Re-emitted lines have `ts` = now, not the time of the run, and a rerun logs them all again. So consumers must bucket
by `start`, keep one line per `run` and `attempt` (`task_cost`: `run`, `attempt` and `task`), and a live alert must filter on `backfill:false`.

Values the record lacks (records written before the cost fields) are filled in:

- `ondemand_price` from Keeper now (`"ondemand_price_source": "current"`), also for a stored `null` with the
  reason `keeper_unreachable` or `keeper_unknown_sku`; any other stored price or reason stays (`launch`). `family`
  and `series` likewise; `vcpus` and `memory_gib` are filled when the record has no `vcpus`, only the keys it lacks.
- `useful_s` and `setup_s` from the `start` and `end` of the `meta.json` files with an exit code of 0 or more of
  the run id under `data/<vendor>/<sku>/<variant>/<task>/` (only the record's `variant` is checked out, any variant
  when it has none), each assigned to the attempt that was open when the task
  started. Only results still in the tree count (a replaced result and the vLLM probe keys are missing); `null`
  when the run has no `meta.json` there and for a DBaaS run, so its `wasted_usd`, the run's `wasted_usd` and the
  counterfactual are `null` too. A `Completed` attempt is useful as a whole either way.

## Queries

Counted spot tries per SKU, ISO weekday (1 is Monday) and 3-hour slot (UTC), from a checkout of sc-inspector-data:

```sh
jq -r '.vendor as $v | .sku as $s | .attempts[] | select(.outcome == "SpotUnavailable")
  | (.api_start | fromdateiso8601) as $t
  | [$v, $s, ($t | strftime("%u")), ($t | strftime("%H") | tonumber / 3 | floor)] | @tsv' \
  data/*/*/runs/*.json | sort | uniq -c
```

Spot interruptions per vendor, region and zone, from a checkout of sc-inspector-data:

```sh
jq -r '.vendor as $v | .attempts[] | select(.outcome == "SpotInterrupted") | [$v, .region, .zone] | @tsv' \
  data/*/*/runs/*.json | sort | uniq -c | sort -rn
```

Interruption rate per SKU (interrupted spot attempts / spot attempts):

```sh
jq -s 'group_by(.vendor + "/" + .sku)[] | {sku: (.[0].vendor + "/" + .[0].sku),
  spot: ([.[].attempts[] | select(.capacity_type == "spot")] | length),
  interrupted: ([.[].attempts[] | select(.outcome == "SpotInterrupted")] | length)}
  | select(.spot > 0) | . + {rate: (.interrupted / .spot)}' data/*/*/runs/*.json
```
