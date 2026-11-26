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
| `database` | `null` for a server run; for DBaaS the database object (`vendor`, `id`, `engineVersion`, `region`, `vcpus`, `memoryGib`, `usdPerHour`) |
| `timing` | the latest attempt's `api_start`, `api_end`, `machine_start`, `node_registered`, `node_ready` (ISO UTC; only those known) |
| `attempts[]` | one entry per node the run used, oldest first |

An attempt:

| Field | Meaning |
|---|---|
| `attempt` | number, from 1 |
| `capacity_type` | `spot` or `on-demand`; `null` for an attempt that never got a node |
| `region`, `zone` | where the node ran; only on the attempt the run's status carried the region of (usually the last), absent otherwise |
| `price` | the node's price per hour in USD as a string, same availability as `region` |
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
