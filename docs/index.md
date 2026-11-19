# Documentation map

The documentation follows [Diátaxis](https://diataxis.fr/): explanation says why, reference says what exactly,
how-to guides say how to do one operator task, tutorials walk through a first use.

Status: **design**. Pages marked *planned* describe procedures that will exist once the code does.

## Explanation (architecture and rationale)

| Page | Topic |
|---|---|
| [Overview](explanation/overview.md) | what the system does, its parts, one run end to end |
| [Run model](explanation/run-model.md) | one run = one node = one anchor Pod plus one Pod per step; step overhead |
| [State model](explanation/state.md) | where state lives, how a restarted controller knows what is running |
| [Output pipeline](explanation/outputs.md) | spool, presigned multipart upload to the final location, git publishing |
| [Spot strategy](explanation/spot.md) | using spot capacity without endless retries |
| [Managed databases](explanation/dbaas.md) | how DBaaS benchmarks provision and remove a database |
| [GPU nodes](explanation/gpu.md) | NVIDIA drivers on Flatcar, runtime, driver selection |
| [Security](explanation/security.md) | credentials, instance metadata, privileged benchmarks |
| [Node profile and comparability](explanation/node-profile.md) | bench node settings and how results compare with the VM era |
| [Separation from other workloads](explanation/separation.md) | taints, namespaces, admission, RBAC |
| [Monitoring](explanation/monitoring.md) | metrics, logs and alerts |
| [Alternatives considered](explanation/alternatives.md) | options rejected and why |

## Reference

| Page | Topic |
|---|---|
| [Feature mapping](reference/feature-mapping.md) | every sc-inspector feature and its k8s-inspector equivalent |
| [Output contract](reference/output-contract.md) | object storage layout and the sc-inspector-data layout sc-crawler reads |
| [Exit codes](reference/exit-codes.md) | `exit_code` values and retry rules |
| [Tasks](reference/tasks.md) | task definition fields and the task hash |
| [Kubernetes objects](reference/objects.md) | `BenchmarkRun` schema, labels, annotations, Pod specs |
| [Configuration](reference/configuration.md) | controller settings |
| [Vendors](reference/vendors.md) | spot, eviction notice, billing granularity, DBaaS, lane status per vendor |

## How-to guides

| Page | Status |
|---|---|
| [Start a forced run](how-to/run-forced.md) | planned |
| [Rerun a task](how-to/rerun-task.md) | planned |
| [Add a vendor](how-to/add-vendor.md) | planned |

## Tutorials

| Page | Status |
|---|---|
| [First OCI benchmark run](tutorials/first-oci-run.md) | planned (outline) |

## Planning

- [Roadmap](roadmap.md): phases and exit criteria
- [Decisions](decisions.md): numbered decisions K1… with status
