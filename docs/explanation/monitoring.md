# Monitoring

Monitoring is a cluster-platform concern: one small stack serves every workload, and k8s-inspector plugs into it. It
comes after the first OCI results are published, before vendors are switched over in bulk ([roadmap](../roadmap.md)).
Until then, failures are visible through `kubectl`, the controller's logs.

## Recommended stack (decision K28)

| Need | Component | Approximate memory |
|---|---|---|
| Metrics storage and scraping | VictoriaMetrics single-node (scrapes Prometheus targets itself, no separate agent) | 100-200 MiB |
| Kubernetes object metrics | kube-state-metrics (with custom-resource metrics for NodeClaims and `BenchmarkRun`s) | ~50 MiB |
| Logs | VictoriaLogs single-node | ~100 MiB |
| Log shipping | Fluent Bit DaemonSet, **not** on bench nodes | ~20-30 MiB per node |
| Alert rules | vmalert | ~30 MiB |
| Notifications | Alertmanager | ~30 MiB |

About 0.4-0.6 GiB in total on the `apps` pool, nothing on control planes or bench nodes. The figures are estimates to
be measured. A Prometheus + Loki + Alertmanager stack does the same with roughly twice the memory; the VictoriaMetrics
components are Prometheus-compatible, so rules and dashboards carry over either way. A Grafana instance is optional.

Bench nodes are excluded on purpose: their benchmark outputs and resource-tracker data already go to object storage,
and any agent would disturb the measurements.

## Transient API errors

Transient API errors (429, 5xx, connection errors, timeouts, a 404 on the `BenchmarkRun` resource before the CRD is established) never stop a controller thread: the runner's initial LIST, resync and watches retry with exponential backoff (1 s to 60 s, jittered, `Retry-After` honoured) and log at warning level, the planner and publisher log the failed cycle and run the next one (the planner logs each run it creates as `planned run` with its `vendor`, `sku`, `reason`, `regions` and the popularity `score`, and every SKU it skips or defers as `autoPlan skips ...` / `autoPlan defers ...`); only a fatal configuration error (such as a missing bucket) ends `serve`.

## What k8s-inspector exposes

`/metrics` on port 9090 of the controller (Prometheus format):

| Metric | Labels |
|---|---|
| `k8s_inspector_runs` | vendor, phase |
| `k8s_inspector_tasks_total` | vendor, task, result (ok, failed, timeout, lost) |
| `k8s_inspector_run_attempts_total` | vendor, capacity_type (`none`: the attempt never got a node), outcome (completed, interrupted, node_lost, provisioning_failed, launch_failed, spot_unavailable, spot_rejected, cancelled, harness_error) |
| `k8s_inspector_node_age_seconds` | vendor, sku, run |
| `k8s_inspector_launch_failures_total` | vendor, reason (ICE, QUOTA, DAILY_LIMIT, VENDOR, LAUNCH_REJECTED, BOOT_FAILURE, ANCHOR_FAILED, NO_OFFERING, OTHER): runs that ended because no node could be launched, counted by the controller |
| `k8s_inspector_spot_tries_total` | vendor, result: spot-only attempts of `SpotUntilExhausted` and `SpotOnly` runs by how they ended: `capacity` (an attempt that allowed spot alone got a spot node), `unavailable` (no spot capacity while the quota had room: a counted try), `quota_busy` (a quota refusal, or an `ICE` while the spot quota showed no room: not a try) |
| `k8s_inspector_spot_waiting_skus` | vendor: SKUs with counted tries that are not exhausted (waiting for a slot), of vendors in a spot-only mode; derived from the `k8s-inspector-spot` ConfigMap at each scrape |
| `k8s_inspector_spot_exhausted_total` | vendor: SKUs of `SpotUntilExhausted` vendors that became exhausted ([configuration](../reference/configuration.md#spot-capacity-modes)), counted once per SKU and first try; after a controller restart a SKU that is still exhausted counts again (a counter reset) |
| `k8s_inspector_launch_deferred_skus` | vendor, reason: SKUs in the launch ConfigMap |
| `k8s_inspector_launch_blocked` | vendor, reason: those of them whose retry is still ahead (the planner skips them) |
| `k8s_inspector_launch_paused` | vendor: 1 while the BOOT_FAILURE breaker pauses the vendor's autoPlan ([launch retries](launch-retries.md#vendor-breaker)) |
| `k8s_inspector_autoplan_vendor_skipped_total` | vendor, reason (`offered_types_unusable`: SCarpenter's offered-types listing is missing, unreadable or stale): planner cycles that skipped the vendor for it |
| `k8s_inspector_publish_backlog` | (tasks done, not yet in git, of SKUs that may be published) |
| `k8s_inspector_unpublishable_tasks_total` | (finished tasks given up on: no `lscpu` result to pick a variant from) |
| `k8s_inspector_publish_lag_seconds` | (age of the oldest done, unpublished task; 0 when none) |
| `k8s_inspector_last_publish_timestamp_seconds` | (last publish cycle without error; the process start until the first one) |
| `k8s_inspector_spend_usd_per_hour` | vendor (sum of running node prices) |
| `k8s_inspector_dbaas_instances` | vendor: managed databases that exist: DBaaS runs whose database is not yet destroyed, plus leaked stacks the sweeper sees (0 for a vendor without any) |
| `k8s_inspector_dbaas_oldest_age_seconds` | vendor: age of the oldest of them (a stack's age since its first update, whatever retries or runs of the same name came after). Stacks of a vendor the ConfigMap does not know are `vendor="unknown"` |
| `k8s_inspector_dbaas_leftover_stack_failures_total` | vendor: runs that ended `-1 DBAAS_LEFTOVER_STACK` because the stack of an earlier run of the same name could not be destroyed in 3 attempts (operator action needed) |
| `k8s_inspector_dbaas_tagged_databases` | vendor: databases the vendor's inventory Job (every 30 minutes) found by their `sc_inspector_run` tag, also those no state records |
| `k8s_inspector_dbaas_inventory_last_success_timestamp_seconds` | vendor: when its inventory Job last succeeded (the controller's start time until the first success: a vendor that never succeeds alerts 2 h after the start); the tagged metrics are missing while the latest one failed |
| `k8s_inspector_dbaas_tagged_oldest_age_seconds` | vendor: age of the oldest of them since its creation in the cloud |
| `k8s_inspector_task_duration_seconds_sum`, `_count` | vendor, task |

The `*_total` and `_sum`/`_count`
series count what the current process has seen and restart from zero with it.

Logs are structured JSON lines with `run`, `vendor`, `sku`, `task` fields.

Each counted spot try ([spot capacity modes](../reference/configuration.md#spot-capacity-modes)) logs one `spot_try` line
(`event` `spot_try`) at info level with `run` (the run id), `vendor`, `sku`, `series`, `family` (`null` when Keeper cannot be asked), `start`
(the attempt's start, ISO), `result` (`unavailable`), `counted` (`true`), `launch_errors` (the cloud's error messages
SCarpenter reported for the attempt), `tried` (`{offerings, regions, zones}` SCarpenter tried, from its error summary; `null`
when the message has none), `quota` (the spot quota entries at that time, as in the [run record](../reference/run-records.md)),
and `tries`, `slots_tried`, `slots_per_week` (the SKU's coverage so far). A second line gives the coverage in words
(`x8i.96xlarge: 41/56 slots tried, spot available <7% (95%)`). Attempts that are not tries (`quota_busy`) only show in the
`k8s_inspector_spot_tries_total` metric.

The `run_attempt_cost` line of each attempt carries `first_try_at` and `capacity_wait_s`: how long the run, with the SKU's
counted spot tries before it, tried before the attempt got its node ([definitions](../reference/run-records.md#cost-log-lines)).
Its `run` is the `run` of the `spot_try` lines of that run.

## Alerts

| Alert | Condition | Independent of the controller? |
|---|---|---|
| **Bench node running too long** | a node in a bench pool older than its run's expected duration + 2 h, or older than 50 h regardless (past `expireAfter` plus grace) (from kube-state-metrics node creation time and pool label) | yes |
| **DBaaS database running too long** | `k8s_inspector_dbaas_tagged_oldest_age_seconds` or `k8s_inspector_dbaas_oldest_age_seconds` above 8 h (the databases are tagged with run id and expiry: look for `sc_inspector_run` in the cloud console). Alert only, nothing is deleted from the tag | no: also alert on the metrics being absent |
| **DBaaS inventory stale** | `time() - k8s_inspector_dbaas_inventory_last_success_timestamp_seconds` above 2 h for a vendor: the tagged-database check is blind for it | no |
| Bench NodeClaim without a run | NodeClaim in a bench pool with no anchor for 30 min | yes |
| Controller down | no `/metrics` scrape for 10 min | yes |
| Publish stalled | backlog > 0 and no successful publish for 1 h | no |
| Vendor skipped | `increase(k8s_inspector_autoplan_vendor_skipped_total[15m]) > 0` for 1 h: the planner has planned nothing for the vendor for over an hour | no |
| Failure spike | failed or lost tasks for a vendor above a threshold per day | no |
| Spot interruptions high | per vendor per day above a threshold | no |
| Spend | `spend_usd_per_hour` above the vendor cap | no |
| Leaked database | managed database older than 8 h | no |

The first two alerts replace an automatic out-of-cluster force-deleter (decision K22): a node that outlives every
teardown layer is a bug to look at, and an alert reaches a human within minutes. If alerts show that such leaks
happen and are slow to handle, an automatic sweeper can be added later.

Alerts go through Alertmanager to the `#alerts` channel of the Spare Cores Slack (K28).

## UI

A web UI (for example Headlamp) is postponed (decision K36). The `BenchmarkRun` printer columns
([objects](../reference/objects.md#benchmarkrun)) already make `kubectl get benchmarkruns` a readable run overview, and
any later UI can show the same columns. Printer columns use `.spec`/`.status` paths only, which UIs render reliably.
