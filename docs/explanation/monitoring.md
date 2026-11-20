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

## What k8s-inspector exposes

`/metrics` on port 9090 of the controller (Prometheus format):

| Metric | Labels |
|---|---|
| `k8s_inspector_runs` | vendor, phase |
| `k8s_inspector_tasks_total` | vendor, task, result (ok, failed, timeout, lost) |
| `k8s_inspector_run_attempts_total` | vendor, capacity_type, outcome (completed, interrupted, node_lost, provisioning_failed, cancelled) |
| `k8s_inspector_node_age_seconds` | vendor, sku, run |
| `k8s_inspector_publish_backlog` | (tasks done, not yet in git) |
| `k8s_inspector_publish_lag_seconds` | (age of the oldest done, unpublished task; 0 when none) |
| `k8s_inspector_last_publish_timestamp_seconds` | (last publish cycle without error; the process start until the first one) |
| `k8s_inspector_spend_usd_per_hour` | vendor (sum of running node prices) |
| `k8s_inspector_task_duration_seconds_sum`, `_count` | vendor, task |

`k8s_inspector_dbaas_instances` (vendor, age bucket) arrives with the DBaaS phase. The `*_total` and `_sum`/`_count`
series count what the current process has seen and restart from zero with it.

Logs are structured JSON lines with `run`, `vendor`, `sku`, `task` fields.

## Alerts

| Alert | Condition | Independent of the controller? |
|---|---|---|
| **Bench node running too long** | a node in a bench pool older than its run's expected duration + 2 h, or older than 50 h regardless (past `expireAfter` plus grace) (from kube-state-metrics node creation time and pool label) | yes |
| Bench NodeClaim without a run | NodeClaim in a bench pool with no anchor for 30 min | yes |
| Controller down | no `/metrics` scrape for 10 min | yes |
| Publish stalled | backlog > 0 and no successful publish for 1 h | no |
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
