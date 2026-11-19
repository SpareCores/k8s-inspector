# Separation from other workloads

The cluster also runs web services and applications. Benchmarks must neither disturb them nor be disturbed.

| Concern | Mechanism |
|---|---|
| Nothing else lands on a bench node | taint `frankenscube/bench=true:NoSchedule`; only the anchor tolerates it; step Pods are bound by `nodeName`. Cluster DaemonSets with blanket tolerations get `frankenscube/pool NotIn [bench]`, so no log or metrics agent runs on bench nodes. |
| Bench Pods land only on bench nodes | the anchor selects `karpenter.sh/nodepool: bench-<vendor>`; steps follow the anchor |
| The controller runs with the apps | toleration `frankenscube/apps` only; never on control planes (`CriticalAddonsOnly`) |
| Benchmarks cannot starve the cluster | per-vendor `maxInFlight` and `maxUsdPerHour` in the controller; NodePool `limits` per bench pool; `ResourceQuota` (`pods: 60`) in `inspector`; PriorityClass `benchmark` (low, `preemptionPolicy: Never`) |
| Privileged Pods only where intended | namespace `inspector` with Pod Security `privileged`; the `ValidatingAdmissionPolicy` and RBAC in [security](security.md#admission-and-rbac) |
| Benchmark traffic does not cross the cluster network | `hostNetwork: true`; outputs go straight to object storage |

## The controller's node

The controller is a long-running Deployment (1 replica, `strategy: Recreate`) on the `apps` NodePool (decision K2).
It is not a CronJob: the `apps` pool scales from zero, and a job every few minutes would launch and remove a node each
time. The Pod carries `karpenter.sh/do-not-disrupt: "true"`, because the `apps` pool consolidates underutilized nodes
and a restart in the middle of a git push is wasted work.

The `apps` pool should run on UpCloud, where the project has the most credits. UpCloud bills by the hour with a
monthly cap, so one always-on small node costs a fixed amount, while launching and removing nodes within an hour
would pay a full hour each time; the long-running Deployment avoids that churn. Two preconditions, both outside this
repository: SCarpenter's UpCloud lane has not yet run against the real cloud, and the vendor list of the `apps` pool is
cluster platform configuration. Until both are done, the controller runs on the current `apps` vendor.

Resources: requests `cpu: 100m`, `memory: 512Mi`, limit `memory: 1Gi`, an emptyDir of 4 GiB for the git checkout and
the sc-data SQLite file.
