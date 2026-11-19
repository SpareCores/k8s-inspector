# Tutorial: first OCI benchmark run

*Planned outline. It will be written against the first working build.*

You will benchmark one small OCI instance type end to end and find its results in sc-inspector-data.

1. **Look around.** `kubectl -n inspector get deploy,cm`; `kubectl get nodepools bench-oci`; what the controller
   would plan (`k8s-inspector plan --dry-run`).
2. **Pick a SKU.** A cheap flexible shape size, for example `VM.Standard.E5.Flex.1-8`.
3. **Start a forced run** with only the probe step and two quick tasks (`--tasks lscpu,stressngfull`).
4. **Watch the node arrive.** `kubectl get benchmarkruns,nodeclaims -w`: Provisioning → Running, the node's labels.
5. **Watch a step.** `kubectl logs -f` on the step Pod; the spool on the node; objects appearing in storage.
6. **See the release.** Phase Releasing → the node and NodeClaim disappear.
7. **Find the results.** The commit in sc-inspector-data; `meta.json` fields; the raw outputs in object storage.
8. **Break something on purpose.** Run `stressngfull` with a 1-minute timeout override and read the `-6` record.
9. **Clean up.** Nothing to clean: the `BenchmarkRun` goes away 24 h after publishing.
