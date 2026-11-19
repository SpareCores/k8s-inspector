# k8s-inspector

k8s-inspector runs the Spare Cores server benchmarks on a Kubernetes cluster whose nodes are launched by
[SCarpenter](https://github.com/SpareCores/scarpenter). It replaces
[sc-inspector](https://github.com/SpareCores/sc-inspector) and publishes the same results to
[sc-inspector-data](https://github.com/SpareCores/sc-inspector-data), which sc-crawler reads.

One long-running controller decides which instance types need which benchmarks, asks Kubernetes for one fresh node
per run, runs the benchmark containers on it one after another, uploads every output straight to its final place in
object storage, and commits the results to git. Benchmark nodes run no agent, no git and no cloud credentials.

**Status: design.** Nothing is implemented yet. The documentation describes the target design.

## Documentation

- [Documentation map](docs/index.md)
- [Architecture overview](docs/explanation/overview.md)
- [Roadmap](docs/roadmap.md) and [decisions](docs/decisions.md)

## Components

| Component | Language | Runs where |
|---|---|---|
| `k8s-inspector` controller (plan, run, publish) | Python (latest stable CPython) | one Deployment replica on the cluster's `apps` NodePool |
| `sc-run` wrapper and uploader | Go, static, standard library only | inside every benchmark container on the benchmark node |
| `k8s-inspector-dbaas` provisioner | Python + Pulumi | short-lived Jobs, only for managed-database benchmarks |

The Go version is pinned in `go.mod` (`go 1.22`); CI reads it from there and the Dockerfile builder is `golang:1.22`.
Bumping Go means bumping `go.mod` and the Dockerfile together.
