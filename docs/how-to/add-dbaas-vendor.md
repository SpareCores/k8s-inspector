# Add a DBaaS vendor

A vendor's managed PostgreSQL is created by one Python module in the provisioner image. The controller does everything
else: it picks the database, region and client ([DBaaS](../explanation/dbaas.md)), runs the module in the Job
`dbaas-up-<run>`, bootstraps the database from the client node, benchmarks it and destroys the stack. You write the
module and its tests, add the provider package to the image, and the platform provides networks and credentials.

## The contract

`dbaas/k8s_inspector_dbaas/vendors/<vendor>.py` (`<vendor>` is Keeper's vendor id) exposes two functions:

```python
def provision(spec: DbSpec) -> DbOutputs:
    """Declare the Pulumi resources of one database; return how to connect to it."""

def inventory() -> list[dict]:
    """The vendor's databases that carry the `sc_inspector_run` tag or label: {id, run, expires_at, created_at}."""
```

`provision` runs inside a Pulumi program (`pulumi.Resource`s, `pulumi.Output`s). `inventory` runs outside Pulumi, in the
periodic Job `dbaas-inventory-<vendor>` (same credentials and ServiceAccount as `up` and `down`, no state bucket). It
is how a database that no state records is found (see [DBaaS](../explanation/dbaas.md#pulumi-state-and-leaks)); times
are RFC 3339 and `run` is the tag's value. The Job also has `DBAAS_REGIONS`, the comma-separated, sorted keys of
`dbaas.vendors.<vendor>.networks` (empty when none): the only regions with databases. A vendor whose API is per region
lists exactly those (`aws.py`), never the regions an API returns, which include regions that are down or unused; it
fails naming the region when one fails. `fake.py` is the smallest example of both.

A module may add `cleanup(run: str) -> None`. `down` calls it with the BenchmarkRun name (all it, and the sweeper,
know; the Job also has `DBAAS_VENDOR`) after the Pulumi destroy and before it removes the state (a failure keeps the
state, so the next `down` calls it again). Use it when the provider can lose a resource that bills: Terraform-based
providers often clear a resource's id when its create fails or times out, so the state does not record it. Tag the
resource with `spec.run` in `provision` (a run name is a DNS label of at most 63 characters, valid as a tag or label
value); `cleanup` finds the resources by that tag, deletes them, waits a few minutes for them to be gone (the Job's
deadline is 2 h and the destroy may have used most of it; a longer deletion fails the Job and the next `down` waits
again) and does nothing when nothing is left.

**`DbSpec`** (`spec.py`; the controller's JSON in `DBAAS_SPEC`, the password in `DBAAS_PASSWORD`):

| Field | Meaning |
|---|---|
| `vendor`, `database_id`, `engine_version` | Keeper's `api_reference` of the database SKU (the vendor's own name for it) and the PostgreSQL major version |
| `region`, `client_region`, `zone` | the database's region (Keeper's `api_reference` of a database price row) and the client node's region, which is `region` unless the vendor's database regions are not its server regions (OVH: the platform sets `clientRegionByCity: true` for the vendor and the controller then picks a region of the same Keeper city), and the client's zone (`None` when unknown; use it to place the database next to the client, or ignore it) |
| `run`, `run_id`, `expires_at` | the run, its id and the time after which the database is a leak |
| `labels` | `sc_inspector_run` and `sc_inspector_expires`, already label-safe (lowercase, digits, `-`, `_`): apply them as tags or labels wherever the vendor supports them |
| `create_timeout`, `delete_timeout` | Pulumi duration strings (`"50m"`, `"110m"`), 10 minutes shorter than the up and down Jobs' deadlines (`dbaas.createMinutes` and 2 h): use them as `custom_timeouts` |
| `network` | the platform's opaque references for this vendor and the client's region (VPC, subnet, subnet group, delegated subnet, private DNS zone ...): the ConfigMap's `networks.<client region>` mapping, unchanged. For a vendor that needs an allowlist on a private network (UpCloud, Vultr) the platform puts the client CIDRs in this mapping, and the module applies them |
| `client_addresses` | the client's public addresses (NodeClaim and Node SCarpenter annotations plus the Node's `ExternalIP`, deduplicated) as `/32` or `/128` (`list[str]`, empty when the node has none). Only for a vendor whose nodes have no private network (OVH): the module allows exactly these on a public endpoint. A module that needs them and gets none raises a clear error |
| `record` | Keeper's record of the database: `vcpus`, `memory_amount` (MiB), `storage_size`, `storage_extra_min/max`, `compatible_storage_ids`, `api_reference_object` (vendor-specific plan data) ...; `spec.vcpus` and `spec.memory_gib` read the first two |
| `password_output()` | the admin password as a Pulumi secret. Use it, never `spec.password`, in a resource, or it lands in the state in plain text |

**`DbOutputs`**: `host`, `admin_user`, `admin_db`, `port` (5432), `sslmode` (`require`; use the vendor's real policy:
`prefer` or `disable` only when it cannot do better), and optionally `user`, `grant_role` and `password`. `password` is for
a vendor that generates the admin password and cannot be given one (OVH): a Pulumi secret, which the controller uses as
`SC_DB_PASSWORD` instead of its own and which travels to it in plain text in the termination message
([DBaaS](../explanation/dbaas.md#the-admin-password)); a vendor that takes the password never sets it. `admin_user` is the user the
vendor created and the bootstrap connects as, to `admin_db`, a database that exists from the start (`postgres`,
`defaultdb`). When the workload user must differ (Cloud SQL: `postgres` cannot create a database it can then use), set
`user`; the bootstrap creates it with the run's password and the `grant_role` (`cloudsqlsuperuser`) and lets it create
the database `bench`. Fields may be Pulumi outputs.

What the module must do:

- **Reachable from the client node**: private network first. The database joins the client's private network
  (`spec.network`), with no public access; the client's network range (the CIDRs the platform put in `spec.network`) is
  allowed through the vendor's firewall or security group. Only where the vendor's nodes have no private network (OVH
  today) the database goes on a public endpoint with TLS (`sslmode` `require` or stronger) and an IP allowlist of
  `spec.client_addresses`, and nothing else.
- **Explicit provider**: create the vendor's provider yourself with `region=spec.region` (the database's) and take its credentials from the
  environment (the vendor's Secret or workload identity), never as provider arguments: those are stored in the state, and
  `down` must keep working after a key is rotated. The Job has no `AWS_*` variables of the state backend (it uses a
  profile), so an AWS provider takes its own credentials from names the module reads itself.
- **Timeouts**: every slow resource gets `ResourceOptions(custom_timeouts=pulumi.CustomTimeouts(create=spec.create_timeout,
  delete=spec.delete_timeout))`. Pulumi then fails by itself before the Job's deadline and records the resource, so a
  timeout never leaves a database the state does not know. Do not invent other numbers.
- **Deterministic names**: name every resource from `spec.run` (the vendor's naming limits permitting), so a retry or a
  destroy finds what an earlier attempt created.
- **Standalone, no HA**, the cheapest storage that gives the I/O budget below, and the vendor's default
  `synchronous_commit` (the benchmark measures durable commits).
- **Sized from Keeper**: storage from `k8s_inspector_dbaas.storage_plan.plan(spec.memory_gib, spec.vcpus, profile)`, where
  `profile` is a `DiskProfile` with the vendor's documented numbers for the disk type (or `None` when the vendor only takes
  a size). The plan gives every vendor the same I/O budget per vCPU. Snapping to what the vendor accepts (size steps, bands,
  a flavour's minimum or maximum) is the module's job.
- **Tagged**: `spec.labels` where the vendor has tags or labels (they are how a leaked database is found, by `inventory`).
  A vendor without tags names the database with the run id so that `inventory` can find it by its name.
- **Decide nothing static about the catalogue**: no tables of instance types, SKUs, regions or per-vendor pins in the
  module. Everything the module needs about the database is in `spec.record`; what is missing there is a Keeper gap to
  report, not a table to write.
- **Fail loudly and early**: a SKU or region the vendor refuses is an exception (the run ends `-1` and is retried by the
  usual bounded rules). The Azure quota pre-flight belongs here ([DBaaS](../explanation/dbaas.md#quota-pre-flight)).
- **Destroyable without the program's inputs**: `down` runs `pulumi destroy` from the state only; the module is not run.
  If the state has pending operations (a killed `up`), `down` fails and keeps the state for an operator.

## Steps

1. Port the vendor's program from sc-runner's `managed_db` resources to `provision`, using `spec` instead of the
   catalogue and the legacy environment. Add the provider package to the `vendors` dependency group
   (`uv add --group vendors pulumi-<provider>` in `dbaas/`); `uv.lock` pins it, and the image build installs its Pulumi
   plugin. A bridged or Terraform-based provider works the same way.
2. Add `dbaas/tests/test_<vendor>.py`. Use Pulumi's mocks (`pulumi.runtime.set_mocks`) to check the resources and the
   outputs, the explicit provider with `region=spec.region`, the `custom_timeouts` from `spec`, the tags, and `inventory`
   with a fake client, no cloud. Test the sizing and the snapping with plain unit tests.
3. Build the image (`docker build dbaas`) and try the module with the `fake` flow first: `python -m k8s_inspector_dbaas up`
   with `PULUMI_BACKEND_URL=file:///work/state`, a passphrase and a `DBAAS_SPEC`; then `down`.
4. Ask the platform for the objects below, enable the vendor (`enabled: true`, `publish`), and smoke-test one database with
   a forced run, preferably a small one ([start a forced run](run-forced.md#managed-database)). Check that the database is
   gone afterwards: `kubectl -n inspector get jobs`, the cloud console, `k8s_inspector_dbaas_instances`.
5. Set `vendors.<vendor>.dbaasAutoPlan: true` when the forced runs are clean. Every database Keeper allows is benchmarked;
   `dbaasMaxInFlight` (default 1) and, when wanted, `dbaasMaxUsdPerHour` bound the vendor's DBaaS runs
   ([limits](../explanation/dbaas.md#limits)).

## What the platform provides

For every vendor (sc-infra, not this repository):

| Object | Purpose |
|---|---|
| ConfigMap `k8s-inspector`, `dbaas.vendors.<vendor>.clientRegionByCity` | `true` only for a vendor whose database regions are not its server regions (OVH); default `false` |
| ConfigMap `k8s-inspector`, `dbaas.vendors.<vendor>.networks.<region>` | the vendor module's network references for each region where a DBaaS network exists. A region without an entry is never used. Infrastructure outputs, not catalogue data |
| Secret `k8s-inspector-dbaas-<vendor>` (optional) | the vendor's cloud credentials as environment variables of its Jobs (`envFrom`), for `up`, `down` and `inventory`. The state backend uses an AWS profile (`AWS_CONFIG_FILE`) and sets no `AWS_*` credential variable, so the vendor's variables are the vendor's |
| ServiceAccount (optional), named in `dbaas.vendors.<vendor>.serviceAccount` | workload identity for the vendor's cloud API instead of a credential Secret. With `K8SI_DBAAS_ROLE_ARN` set, a projected ServiceAccount token (audience `sts.amazonaws.com`) is at `/var/run/secrets/aws/token` in `up`, `down` and `inventory` alike, which a module may use for its own web-identity login; the state backend variables exist only in `up` and `down` |
| Allowlist CIDRs in `networks.<region>` (vendors that need them) | the client nodes' address ranges, for OVH, UpCloud and Vultr |
| Secret `k8s-inspector-dbaas-state`, key `passphrase` (once) | the secrets provider of the Pulumi stacks |
| Role for the Jobs (once) | a role of its own, `K8SI_DBAAS_ROLE_ARN` on the controller Deployment: read, write, list and delete under `dbaas-state/` (below `K8SI_KEY_PREFIX`) and nothing else, trusted for the ServiceAccount `k8s-inspector-dbaas` and every vendor ServiceAccount (their token, audience `sts.amazonaws.com`). The Jobs do not use the controller's bucket role; the controller's role needs the same `dbaas-state/` access for the cleanup after a destroy. The bucket and prefix are not shared between clusters |
| The image digest (once) | `K8SI_DBAAS_IMAGE` on the controller Deployment, `ghcr.io/sparecores/k8s-inspector-dbaas@sha256:...` |

The controller needs no other cloud rights: the Jobs hold them. The Jobs and the controller are told apart in the
cloud by the credentials, so give the Job's identity only what creating and destroying one database needs (databases,
their users and firewall entries, in the DBaaS networks).
