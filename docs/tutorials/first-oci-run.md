# Tutorial: first OCI benchmark run

*Planned outline. It will be written against the first working build.*

You will benchmark one small OCI instance type end to end and find its results in sc-inspector-data.

1. **Look around.** `kubectl -n inspector get deploy,cm`: the platform-managed ConfigMap `k8s-inspector` (key
   `config.yaml`) must enable `oci`; changes to it apply within ~30 s, no restart. `kubectl get nodepools bench`
   shows the bench NodePool (shared by all vendors).
2. **Pick a SKU.** A cheap flexible shape size, for example `VM.Standard.E5.Flex.1-8`.
3. **Start a forced run** with two quick tasks, on demand (`--tasks lscpu,stressngfull`):

   ```sh
   kubectl -n inspector exec deploy/k8s-inspector -- \
     k8s-inspector run oci VM.Standard.E5.Flex.1-8 --force --on-demand --tasks lscpu,stressngfull
   ```

   Without `--tasks`, a run covers all server tasks except `stressnglongrun` (Keeper allows it only on an SKU
   allowlist; it runs 26 h on demand, so name it in `--tasks` only when you want it). The command prints the
   selected tasks.
4. **Watch the node arrive.** `kubectl get benchmarkruns,nodeclaims -w`: Provisioning → Running, the node's labels.
5. **Watch a step.** `kubectl logs -f` on the step Pod; the spool on the node; objects appearing in storage.
6. **See the release.** Phase Releasing → the node and NodeClaim disappear.
7. **Find the results.** The commit in sc-inspector-data; `meta.json` fields; the raw outputs in object storage. Pushed results under `data/oci/...` flow into sc-crawler and the published data, so publish a SKU only after its outputs parse correctly and match the legacy layout.
8. **Break something on purpose.** Run `stressngfull` with a 1-minute timeout override and read the `-6` record.
9. **Clean up.** Nothing to clean. With `publish` off for the vendor (the default) the run stays in phase `Publishing`
   and is deleted `keepRunHours` (default 24 h) after its last task; its outputs stay in object storage. To run the
   same SKU again sooner, delete the `BenchmarkRun` (`kubectl -n inspector delete benchmarkrun <run name>`).
