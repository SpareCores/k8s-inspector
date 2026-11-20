# State model

## Where state lives

| State | Store | Written by | Lifetime |
|---|---|---|---|
| What is running now: run lease, plan, attempt, current step, per-task progress | Kubernetes: `BenchmarkRun` (spec + status), anchor and step Pods | controller | until the run is published, plus 24 h |
| Task outputs and the per-task result (`status.json`) | object storage, at the final location | `sc-run` (outputs), controller (`status.json`) | bucket lifecycle (as today's task logs) |
| Published results, history, retry counters, failure records | git: sc-inspector-data `meta.json` files | publisher (the only git writer) | forever |
| Spot interruption statistics per SKU | ConfigMap `k8s-inspector-spot` | controller | best effort; losing it only means trying spot again |
| Offerings, capacity and quota errors | SCarpenter | SCarpenter | |
| Settings | ConfigMap `k8s-inspector` ([configuration](../reference/configuration.md)) | platform | |

There is no database.

## How a restarted controller knows what is ongoing

The legacy inspector committed a placeholder `meta.json` before creating a VM and treated that commit as the lock
("started within the last 2 hours: do not start again"). k8s-inspector does not use git as a lock. Work in progress is
whatever the API server holds:

- **The lease is the `BenchmarkRun` object.** Its name is a pure function of the SKU
  (`<vendor>-<sku slug>-<crc32 of vendor/sku>`), so creating a second run of the same SKU fails with `AlreadyExists`.
  The API server's create is atomic; no timeout, no TTL guesswork. The run object is deleted only after all its tasks
  are published (plus a 24 h grace for inspection), so "has a `BenchmarkRun`" also covers "finished but not yet in git".
- **Progress is the run's status plus its Pods.** On start, and after every watch reconnect, the controller LISTs
  `BenchmarkRun`s in its namespace and Pods with label `inspector.sparecores.com/run`, rebuilds its in-memory view,
  and continues each run from where its objects say it is: the step in flight is read from `status.step` and never
  planned again; a step Pod that ended while the controller was down is collected; an anchor without a step in flight
  gets its next step; a vanished anchor is an interrupted attempt.
- **Every write is idempotent.** Object names are deterministic (`AlreadyExists` = already done); `status.json`
  and the uploads are overwrites of the same keys; git rendering is a pure function of object storage and the tree.
  Two controller replicas overlapping briefly (`strategy: Recreate` during a node failure) cannot create duplicates.

## Cost and speed at our scale

The controller's view is one watch per kind (BenchmarkRuns and labelled Pods in one namespace) with a full re-LIST
every 5 minutes as a resync.

| Quantity | Typical | Peak |
|---|---|---|
| Runs in flight | 5-20 | ~50 (per-vendor caps) |
| Objects watched (runs + anchors + step Pods kept until the run ends) | 50-200 | ~1,000 |
| Size of a full LIST | well under 1 MB | a few MB |
| LIST latency | milliseconds | tens of milliseconds |

This is negligible for the API server and the controller (a few MB of memory). The watch also removes the idle time a
poll would add between steps ([run model](run-model.md#step-overhead)). Kubernetes `etcd` and the watch cache are built
for orders of magnitude more objects.

## What git still records

`meta.json` in sc-inspector-data keeps its schema and meaning, but it is written **once per finished task**, by the
publisher, never as a placeholder:

- the task result: `start`, `end`, `exit_code`, `error_msg`, `task_hash`, `version`, `kernel_version`, byte counts,
  `outputs`, `region`, `spot`;
- history the planner needs: `start_retries` (incremented on each `-4`), the last failure and its time (backoff for
  `-1` and `-4`), and the hash (rerun on change);
- new fields ignored by sc-crawler: `run_id`, `image` (digest), `zone`, `capacity_type`, `price`, `node_os`,
  `kubelet`, `runtime`, `nodeclass_hash`, `spot_attempts`.

The planner reads git (what is done and when) and Kubernetes (what is running); together they replace the 2-hour
start window, the boot placeholders and the stale-start reconcile of sc-inspector.

## Disaster recovery

If the cluster (and with it every `BenchmarkRun`) is lost, finished tasks are still in object storage with their
`status.json`. `k8s-inspector publish --rescan --since 7d` lists the recent date prefixes of the routed SKUs and
publishes anything missing from git (the stale-start guard skips what git already has). Runs that were in flight are
simply planned again; their nodes are released by the anchor idle timeout and the NodePool expiry.
