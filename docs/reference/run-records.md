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
| `phase` | the run's final phase: `Done`, `Failed` or `Cancelled`; `unknown` in a [backfilled](#backfill) record whose attempts do not tell |
| `plan` | task names of the run, in order |
| `variant` | the hardware variant number the run's results were filed under, `null` when none (DBaaS: always). Not `run.json`'s own `variant`, which is the runner's last snapshot: the publisher decides the variant later. Live: the run's `status.variant`; backfilled: the number of the directory holding the run's `timing/<run_id>/` in the tree (`data/<vendor>/<sku>/<N>/timing/<run_id>/`), `null` when there is none |
| `family`, `series`, `cpu_arch`, `gpu_count` | Keeper's `family` (e.g. `Compute Optimized`), `series` (e.g. `c6a`), `cpu_architecture` and `accelerator_count` of the SKU (the client node for a DBaaS run), read once when the run's first attempt gets its node; absent when Keeper could not be asked |
| `database` | `null` for a server run; for DBaaS the database object (`vendor`, `id`, `engineVersion`, `region`, `vcpus`, `memoryGib`, `usdPerHour`) |
| `timing` | the latest attempt's `api_start`, `api_end`, `machine_start`, `node_registered`, `node_ready` (ISO UTC; only those known) |
| `attempts[]` | one entry per node the run used, oldest first |

An attempt:

| Field | Meaning |
|---|---|
| `attempt` | number, from 1 |
| `capacity_type` | `spot` or `on-demand`; `null` for an attempt that never got a node |
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

Attempt `outcome` values:

| Outcome | Meaning |
|---|---|
| `Completed` | the run's tasks finished on this node |
| `SpotInterrupted` | the spot node was reclaimed; another attempt followed or the run ended |
| `NodeLost` | the node disappeared for another reason |
| `ProvisioningFailed` | no node could be provisioned (the run failed) |
| `LaunchFailed` | the NodeClaim of a spot-allowed attempt failed to launch or boot; on-demand next |
| `Cancelled` | an operator cancelled the run |
| `HarnessError` | the controller gave up on the run |

## When a record is written

The publisher writes the record, in its own commit `k8s-inspector: run records (<run ids>)`, when all of these hold:

- the run is `Done`, `Failed` or `Cancelled`;
- all its tasks are published or given up (a cancelled run's discarded tasks count as such);
- its SKU (DBaaS: database) is enabled for publishing ([configuration](configuration.md)), the same rule as for
  results.

Write once: when the file is already in the tree it is not rewritten. The controller does not delete a finished,
publishable run (`keepRunHours`) before its record is in git, also after a controller restart. A run whose `run.json`
is gone or unusable (malformed, or without a safe vendor and SKU) gets no record (logged as an error) and is deleted
as usual; a storage or rendering error is retried every cycle.

## What is not recorded

- Runs deleted by the launch deferral (a temporary launch failure such as no capacity: the `BenchmarkRun` is deleted
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
`Done`, `Cancelled` is `Cancelled`, `ProvisioningFailed` and `HarnessError` are `Failed`, anything else is
`unknown`. A run the launch deferral deleted has no `run.json` (skipped) or an open or `NodeLost` last attempt (`unknown`); it is never `Failed`. A malformed `run.json` is skipped with a warning.

Run it in the controller Pod, which has the object storage role, the deploy key and the cluster permissions:

```sh
kubectl -n inspector exec deploy/k8s-inspector -- k8s-inspector publish --backfill-runs --since 120d
```

`kubectl create job --from` does not apply: it needs a CronJob, and the controller is a Deployment. The command
prints `wrote <n> run records`.

## Cost log lines

When a run record is written (once per run, after its commit is pushed) the controller logs what the run's attempts
cost, one `run_attempt_cost` line per attempt and one `run_cost` line for the run, at info level in the normal JSON
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
  when it kept none. A task the controller wrote off (`-4` lost with the node, `-1` never ran) did not run: it has no
  real times and is never kept. An attempt that ran no task (no node, `ProvisioningFailed`, `LaunchFailed`) has 0,
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
| `attempt` | number, from 1 |
| `region`, `zone`, `capacity_type`, `outcome` | as in the attempt (`null` when absent) |
| `start`, `end` | the attempt's `api_start` and `end` |
| `duration_s`, `useful_s`, `setup_s`, `task_s`, `price`, `ondemand_price`, `cost_usd`, `wasted_usd` | as defined above; `ondemand_price` is the attempt's |
| `ondemand_price_source` | `launch`: captured when the attempt got its node; `current`: Keeper's price when the record or the backfill was written, for an attempt launched without one (an older controller, or Keeper unreachable); after one failed Keeper call a pass does not ask again, and the remaining attempts keep `ondemand_price_reason: keeper_unreachable`; an attempt that got a node always has `ondemand_price` or an `ondemand_price_reason` |
| `start_hour_utc`, `start_weekday_utc` | hour `0`-`23` and weekday `Mon`-`Sun` of `start`, UTC |
| `backfill` | `false` |

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
by `start`, keep one line per `run` (and attempt), and a live alert must filter on `backfill:false`.

Values the record lacks (records written before the cost fields) are filled in:

- `ondemand_price` from Keeper now (`"ondemand_price_source": "current"`), also for a stored `null` with the
  reason `keeper_unreachable` or `keeper_unknown_sku`; any other stored price or reason stays (`launch`). `family`
  and `series` likewise.
- `useful_s` and `setup_s` from the `start` and `end` of the `meta.json` files with an exit code of 0 or more of
  the run id under `data/<vendor>/<sku>/<variant>/<task>/`, each assigned to the attempt that was open when the task
  started. Only results still in the tree count (a replaced result and the vLLM probe keys are missing); `null`
  when the run has no `meta.json` there and for a DBaaS run, so its `wasted_usd`, the run's `wasted_usd` and the
  counterfactual are `null` too. A `Completed` attempt is useful as a whole either way.

## Queries

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
