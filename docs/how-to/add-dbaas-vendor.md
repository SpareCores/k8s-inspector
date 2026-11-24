# Add a DBaaS vendor

A vendor's managed PostgreSQL is created by one Python module in the provisioner image. The controller does everything
else: it picks the database, region and client ([DBaaS](../explanation/dbaas.md)), runs the module in the Job
`dbaas-up-<run>`, bootstraps the database from the client node, benchmarks it and destroys the stack. You write the
module and its tests, add the provider package to the image, and the platform provides networks and credentials.

## The contract

`dbaas/k8s_inspector_dbaas/vendors/<vendor>.py` (`<vendor>` is Keeper's vendor id) exposes one function:

```python
def provision(spec: DbSpec) -> DbOutputs:
    """Declare the Pulumi resources of one database; return how to connect to it."""
```

It runs inside a Pulumi program (`pulumi.Resource`s, `pulumi.Output`s). `fake.py` is the smallest example.

**`DbSpec`** (`spec.py`; the controller's JSON in `DBAAS_SPEC`, the password in `DBAAS_PASSWORD`):

| Field | Meaning |
|---|---|
| `vendor`, `database_id`, `engine_version` | Keeper's `api_reference` of the database SKU (the vendor's own name for it) and the PostgreSQL major version |
| `region`, `zone` | the client node's region (the database goes there) and zone (`None` when unknown; use it to place the database next to the client, or ignore it) |
| `run`, `run_id`, `expires_at` | the run, its id and the time after which the database is a leak |
| `labels` | `sc_inspector_run` and `sc_inspector_expires`, already label-safe (lowercase, digits, `-`, `_`): apply them as tags or labels wherever the vendor supports them |
| `network` | the platform's opaque references for this vendor and region (VPC, subnet, subnet group, delegated subnet, private DNS zone ...): the mapping from the ConfigMap, unchanged |
| `record` | Keeper's record of the database: `vcpus`, `memory_amount` (MiB), `storage_size`, `storage_extra_min/max`, `compatible_storage_ids`, `api_reference_object` (vendor-specific plan data) ...; `spec.vcpus` and `spec.memory_gib` read the first two |
| `password_output()` | the admin password as a Pulumi secret. Use it, never `spec.password`, in a resource, or it lands in the state in plain text |

**`DbOutputs`**: `host`, `admin_user`, `admin_db`, `port` (5432), `sslmode` (`require`; use the vendor's real policy:
`prefer` or `disable` only when it cannot do better), and optionally `user` and `grant_role`. `admin_user` is the user the
vendor created and the bootstrap connects as, to `admin_db`, a database that exists from the start (`postgres`,
`defaultdb`). When the workload user must differ (Cloud SQL: `postgres` cannot create a database it can then use), set
`user`; the bootstrap creates it with the run's password and the `grant_role` (`cloudsqlsuperuser`) and lets it create
the database `bench`. Fields may be Pulumi outputs.

What the module must do:

- **Reachable from the client node**: the database joins the client's private network (`spec.network`), with no public
  access; the client's network range is allowed through the vendor's firewall or security group.
- **Standalone, no HA**, the cheapest storage that gives the I/O budget below, and the vendor's default
  `synchronous_commit` (the benchmark measures durable commits).
- **Sized from Keeper**: storage from `k8s_inspector_dbaas.storage_plan.plan(spec.memory_gib, spec.vcpus, profile)`, where
  `profile` is a `DiskProfile` with the vendor's documented numbers for the disk type (or `None` when the vendor only takes
  a size). The plan gives every vendor the same I/O budget per vCPU. Snapping to what the vendor accepts (size steps, bands,
  a flavour's minimum or maximum) is the module's job.
- **Tagged**: `spec.labels` where the vendor has tags or labels (they are how a leaked database is found).
- **Decide nothing static about the catalogue**: no tables of instance types, SKUs, regions or per-vendor pins in the
  module. Everything the module needs about the database is in `spec.record`; what is missing there is a Keeper gap to
  report, not a table to write.
- **Fail loudly and early**: a SKU or region the vendor refuses is an exception (the run ends `-1` and is retried by the
  usual bounded rules). The Azure quota pre-flight belongs here ([DBaaS](../explanation/dbaas.md#quota-pre-flight)).
- **Destroyable without the program's inputs**: `down` runs `pulumi destroy` from the state only; the module is not run.

## Steps

1. Port the vendor's program from sc-runner's `managed_db` resources to `provision`, using `spec` instead of the
   catalogue and the legacy environment. Add the provider package to the `vendors` dependency group
   (`uv add --group vendors pulumi-<provider>` in `dbaas/`); `uv.lock` pins it, and the image build installs its Pulumi
   plugin. A bridged or Terraform-based provider works the same way.
2. Add `dbaas/tests/test_<vendor>.py`. Use Pulumi's mocks (`pulumi.runtime.set_mocks`) to check the resources and the
   outputs, no cloud. Test the sizing and the snapping with plain unit tests.
3. Build the image (`docker build dbaas`) and try the module with the `fake` flow first: `python -m k8s_inspector_dbaas up`
   with `PULUMI_BACKEND_URL=file:///work/state`, a passphrase and a `DBAAS_SPEC`; then `down`.
4. Ask the platform for the objects below, enable the vendor (`enabled: true`, `publish`), and smoke-test one database with
   a forced run, preferably a small one ([start a forced run](run-forced.md#managed-database)). Check that the database is
   gone afterwards: `kubectl -n inspector get jobs`, the cloud console, `k8s_inspector_dbaas_instances`.
5. Set `vendors.<vendor>.dbaasAutoPlan: true` when the forced runs are clean. Every database Keeper allows is benchmarked;
   there are no price caps.

## What the platform provides

For every vendor (sc-infra, not this repository):

| Object | Purpose |
|---|---|
| ConfigMap `k8s-inspector`, `dbaas.vendors.<vendor>.networks.<region>` | the vendor module's network references for each region where a DBaaS network exists. A region without an entry is never used. Infrastructure outputs, not catalogue data |
| Secret `k8s-inspector-dbaas-<vendor>` (optional) | the vendor's cloud credentials as environment variables of its Jobs (`envFrom`). The Job's own `AWS_*` and `PULUMI_*` variables belong to the state backend, so a vendor that needs AWS credentials uses names the module reads itself (or an explicit provider configuration), never `AWS_ACCESS_KEY_ID` |
| ServiceAccount (optional), named in `dbaas.vendors.<vendor>.serviceAccount` | workload identity for the vendor's cloud API instead of a credential Secret |
| Secret `k8s-inspector-dbaas-state`, key `passphrase` (once) | the secrets provider of the Pulumi stacks |
| Bucket permissions (once) | the controller's role also needs read, write, list and delete under `dbaas-state/` (below `K8SI_KEY_PREFIX`), and the Jobs use the same role (their ServiceAccount token, audience `sts.amazonaws.com`) |
| The image digest (once) | `K8SI_DBAAS_IMAGE` on the controller Deployment, `ghcr.io/sparecores/k8s-inspector-dbaas@sha256:...` |

The controller needs no other cloud rights: the Jobs hold them. The Jobs and the controller are told apart in the
cloud by the credentials, so give the Job's identity only what creating and destroying one database needs (databases,
their users and firewall entries, in the DBaaS networks).
