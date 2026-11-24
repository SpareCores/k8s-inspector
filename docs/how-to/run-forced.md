# Start a forced run

*Planned: describes the CLI once it exists.*

`k8s-inspector run` benchmarks one SKU now, whether or not its tasks are due. With `--force` it also skips Keeper's
eligibility check, and its launch failures are recorded as `-1` results instead of launch backoff entries ([launch
retries](../explanation/launch-retries.md)); the enabled vendor, `maxInFlight` and `maxUsdPerHour` checks apply either way.
It is for operators: to rerun a SKU or a task now, or to run a SKU Keeper blocks. The controller picks SKUs itself when `autoPlan` is on for the
vendor ([autoPlan](../reference/configuration.md#autoplan));
a forced run of such a SKU takes the SKU's lease like any other run, so autoPlan does not start it twice.

## Before you start

- The vendor is enabled in the ConfigMap `k8s-inspector` (managed by the platform) and has a block in the `bench` SCNodeClass.
- The SKU is an instance type SCarpenter offers: `kubectl get nodepools bench -o yaml` shows the pool;
  the type name is the vendor's `api_reference` (for OCI flexible shapes `<shape>.<ocpus>-<gb>`).

## Steps

1. Start the run:

   ```sh
   kubectl -n inspector exec deploy/k8s-inspector -- \
     k8s-inspector run oci VM.Standard.E5.Flex.2-16 --force
   ```

   Without `--tasks`, a run benchmarks every server task except the DBaaS tasks and the Keeper-gated ones
   (`keeper_gated` in the catalogue: today `stressnglongrun`, which Keeper only allows on a short SKU allowlist). The command prints the selected task list before the run name.
   To include it, name it: `--tasks stressnglongrun` (or add it to a longer list). It runs 26 hours and is always
   on demand (no spot), so it costs a day of the node: use it deliberately.

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
  and returns; the controller then records the unfinished tasks as `-4 CANCELLED` (not published: the SKU's
  previous results stay), releases the node (all the run's NodeClaims are deleted, initialized ones included, so a node that is still booting goes too) and keeps the `BenchmarkRun` (phase `Cancelled`). The SKU is planned
  again once the cancelled run is purged, `keepRunHours` later; to stop that, pause the vendor or block the SKU.
  Check with `k8s-inspector status`. Deleting the `BenchmarkRun` with `kubectl` releases the node only by deleting the anchor, so a node
  that is still booting stays until Karpenter removes it, and it drops the record.
- Pause a vendor: set `vendors.<vendor>.maxInFlight: 0` in the ConfigMap `k8s-inspector` (key `config.yaml`, managed by the platform; applies within ~30 s without a restart). Running runs finish.

## Debug a node

With `--hold`, start a debug Pod on the node and use the host's tools:

```sh
kubectl debug node/<node> --profile=sysadmin --attach=false -- sleep 3600
kubectl exec -it <debug pod> -- chroot /host
```

## Managed database

A DBaaS run benchmarks one managed PostgreSQL ([DBaaS](../explanation/dbaas.md)). The vendor needs `dbaas.vendors.<vendor>`
networks in the ConfigMap and `K8SI_DBAAS_IMAGE` must be set:

```sh
kubectl -n inspector exec deploy/k8s-inspector -- \
  k8s-inspector run upcloud --database 1x1xCPU-1GB-10GB --force
```

The database is Keeper's `api_reference`. The database's region defaults to the cheapest one that has a client region
with a network (`--region <region>` picks one); the client's region defaults to the database's, else a region of the same
Keeper city (`--client-region <region>` picks one, and needs a network in the ConfigMap), and the client server to the one the picker chooses (the optional `<sku>` argument names one). Without
`--force` Keeper must find the database eligible. The vendor's `dbaasMaxInFlight` and `dbaasMaxUsdPerHour` apply (a forced run is refused with the reason). Follow it with
`kubectl -n inspector get benchmarkruns -w` and `get jobs`; the run ends with the database destroyed and the finalizer gone.
`k8s-inspector cancel <run>` cancels it and destroys the database. `--tasks` and `--on-demand` do not apply: the run
always benchmarks the DBaaS task, on demand.
