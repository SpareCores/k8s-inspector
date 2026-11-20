# Start a forced run

*Planned: describes the CLI once it exists.*

A forced run benchmarks one SKU now, skipping Keeper eligibility and the "is it due" check. It is how OCI SKUs are
benchmarked while Keeper's eligibility rules do not allow inspector runs on OCI SKUs.

## Before you start

- The vendor is enabled in the ConfigMap `k8s-inspector` (managed by the platform) and has a bench NodePool.
- The SKU is an instance type SCarpenter offers: `kubectl get nodepools bench-oci -o yaml` shows the pool;
  the type name is the vendor's `api_reference` (for OCI flexible shapes `<shape>.<ocpus>-<gb>`).

## Steps

1. Start the run:

   ```sh
   kubectl -n inspector exec deploy/k8s-inspector -- \
     k8s-inspector run oci VM.Standard.E5.Flex.2-16 --force
   ```

   Add `--tasks lscpu,stressngfull` to run only some tasks, `--on-demand` to skip spot, `--hold 2h` to keep the node
   after the last step for debugging.

2. Follow it:

   ```sh
   kubectl -n inspector get benchmarkruns -w
   kubectl -n inspector logs -f -l inspector.sparecores.com/run=<run name>,inspector.sparecores.com/role=step
   ```

3. When the phase is `Done`, the results are in sc-inspector-data (check them first: they flow into sc-crawler and the published data) under `data/oci/VM.Standard.E5.Flex.2-16/`, and
   the raw outputs under `logs/inspect/oci/VM.Standard.E5.Flex.2-16/<date>/<run_id>/`.

## Cancel or pause

- Cancel a run: `k8s-inspector cancel <run name>` sets the annotation `inspector.sparecores.com/cancel: "true"`
  and returns; the controller then records the unfinished tasks as `-4 CANCELLED`, releases the node and keeps the
  `BenchmarkRun` (phase `Cancelled`). Check with `k8s-inspector status`. Deleting the `BenchmarkRun`
  with `kubectl` also releases the node but drops the record.
- Pause a vendor: set `vendors.<vendor>.maxInFlight: 0` in the ConfigMap `k8s-inspector` (key `config.yaml`, managed by the platform; applies within ~30 s without a restart). Running runs finish.

## Debug a node

With `--hold`, start a debug Pod on the node and use the host's tools:

```sh
kubectl debug node/<node> --profile=sysadmin --attach=false -- sleep 3600
kubectl exec -it <debug pod> -- chroot /host
```
