# Vendors

Facts that shape planning per vendor. Sources: vendor documentation, sc-crawler's vendor data, SCarpenter's driver
references. Entries marked "verify" disagree between sources or are not documented.

| Vendor | SCarpenter lane | Spot | Reclaim notice | VM billing | Managed PostgreSQL |
|---|---|---|---|---|---|
| AWS | Supported | yes | 2 min (+ rebalance hint) | per second, 60 s minimum | RDS; 10 min minimum after create |
| OCI | Supported | preemptible, ~50 % off flexible shapes | 2 min | per second, 1 min minimum (VM) | PostgreSQL DB System |
| GCP | Preview (smoke run passed) | Spot VMs | 0 s (default) or 2 min, then 30 s | per second, 1 min minimum | Cloud SQL |
| Azure | Preview (smoke run passed) | Spot | 30 s, best effort | per minute (sc-crawler: 300 s minimum, verify) | Flexible Server |
| OVHcloud | Preview (smoke run passed) | no | — | per second after 1 min in EU; US hourly (verify) | Public Cloud Databases, hourly |
| Alibaba Cloud | Preview (no real-cloud run) | yes | 5 min | per second; minimum 10/5/2 min for 1/2/4+ vCPU | RDS PostgreSQL |
| Hetzner | Preview (no real-cloud run) | no | — | hourly | none |
| UpCloud | Preview (no real-cloud run) | no | — | hourly, 672 h monthly cap | Managed PostgreSQL, hourly |
| Vultr | Preview (no real-cloud run) | no | — | hourly, 672 h monthly cap | Managed Databases (billing unverified) |

Consequences:

- **Hourly vendors** (Hetzner, UpCloud, Vultr, possibly OVHcloud US): stopping a VM does not stop billing there, so
  keep `maxInFlight` and `maxUsdPerHour` low. The planner does not batch work: a SKU with due tasks starts as soon as
  the vendor has room (it fills the room with whichever ranked SKUs fit, up to `maxNewRunsPerCycle` per cycle) ([autoPlan](configuration.md#autoplan)).
- **Spot**: only where the table says yes, per the [spot strategy](../explanation/spot.md).
- **DBaaS**: run only where the managed database exists; hourly database billing makes a failed attempt cost an hour
  ([DBaaS](../explanation/dbaas.md)).
- **Keeper**: Keeper serves the OCI SKUs and applies its normal eligibility rules, so autoPlan picks them like any
  vendor's. Forced runs are for operators ([start a forced run](../how-to/run-forced.md)).

## UpCloud DBaaS

`dbaas/k8s_inspector_dbaas/vendors/upcloud.py`. UpCloud calls its regions zones; the database goes to the client's zone.

- **Network**: `dbaas.vendors.upcloud.networks.<zone>` holds `network_id` (the SDN private network that the bench nodes of
  the zone have an interface on) and `cidr` (its range). The database joins that network with no public access, and its IP
  filter is `cidr`. The host the benchmark connects to is the service's private endpoint.
- **Plans**: Keeper's `api_reference`. A flexible plan (`rdb.*`) takes compute, one node, storage and the cheapest backup
  tier it allows (`mini`, `regular`, `extended`). The storage is the schema-derived size ([DBaaS](../explanation/dbaas.md)),
  clamped to the plan's bands and snapped to their step, both from `api_reference_object.plan_storage` of Keeper's
  record. A legacy plan (`1x...`) is passed by name and includes its storage.
- **Credentials**: the Secret `k8s-inspector-dbaas-upcloud` holds `UPCLOUD_TOKEN` of an API sub-account for the Jobs. The
  provider and the inventory read it from the environment; UpCloud tokens have no scopes, so the sub-account's permissions
  are the limit.
- **Admin user** `scadmin`, database `defaultdb`, `sslmode=require`. Services carry the labels `sc_inspector_run` and
  `sc_inspector_expires`; `inventory` lists them with `GET /1.3/database`.

## Vultr DBaaS

`dbaas/k8s_inspector_dbaas/vendors/vultr.py`. Vultr Managed Databases for PostgreSQL, through the Pulumi package `ediri-vultr` (a community provider bridged from
Vultr's Terraform provider; its releases trail the Terraform provider, and the plugin binary exists for linux amd64 and
arm64).

- **Plan**: the database id is the plan (`vultr-dbaas-<tier>-...`), which fixes vCPUs, RAM and disk, so the module sizes
  no storage. A plan whose Keeper `ha` has no `none` (the business and premium tiers) is refused.
- **Network**: the database joins the bench VPC of the region (`networks.<region>.vpc_id`) and is reached on its private
  host. `networks.<region>.trusted_ips` is the VPC's CIDR, the only range allowed to connect; `sslmode` is `require`.
- **User**: `vultradmin` on `defaultdb`, both read back from the API. The provider sets the run's password in a second
  call after the database is active.
- **Credentials**: the API key in the Secret `k8s-inspector-dbaas-vultr` (`VULTR_API_KEY`), the key of a sub-user of its
  own. Vultr keys have no scope below the ACLs: the account's API access control must admit the cluster's egress
  addresses. The module keeps the key out of the provider arguments, so it is not in the Pulumi state.
- **Tag**: Vultr has one tag per database, so the module writes the run labels into it (`sc_inspector_expires:<time>:sc_inspector_run:<id>`);
  `inventory` lists `GET /v2/databases` and reads them back. There is no region argument on the provider: the region is
  on the database.

## OCI DBaaS

OCI Database with PostgreSQL (`dbaas/k8s_inspector_dbaas/vendors/oci.py`, Pulumi provider `pulumi-oci`). There is no legacy
implementation; the module is written against the provider's `oci.psql.DbSystem`.

- **Database**: one DB system with one node (no read replicas, no HA). Shape, OCPUs and memory are Keeper's
  `api_reference_object` of the database (`PostgreSQL.<shape>` with `instance_ocpu_count` and
  `instance_memory_size_in_gbs`); Keeper's `vcpus` are twice the OCPUs.
- **Storage**: database optimized storage has no size to choose, only a performance tier (guaranteed IOPS regardless of
  size). The module takes the lowest tier of the record's `compatible_storage_ids` (75,000 IOPS), far above the I/O budget
  of `storage_plan` (25 write IOPS per vCPU). Storage is AD-local, in the availability domain of the client node: the
  nearest thing to the single-zone setup of the other vendors. Regional storage writes every commit to a second
  availability domain, which a durable-commit benchmark would measure as latency.
- **Network**: the DB system has a private endpoint in a private, IPv4-only subnet of the region's bench VCN (OCI creates DB
  systems only in subnets without IPv6, so not in the bench nodes' subnet), with the network security group the platform names
  in `networks.<region>` (`compartment_id`, `subnet_id`, `nsg_ids`; the group admits TCP 5432 from the bench nodes). No
  allowlist is applied; the security group decides.
- **Credentials**: the Job's identity is an API key (Secret `k8s-inspector-dbaas-oci`: `TF_VAR_tenancy_ocid`,
  `TF_VAR_user_ocid`, `TF_VAR_fingerprint`, `TF_VAR_private_key`, which the provider and the OCI SDK both read) whose
  policy manages `postgres-db-systems` in the bench compartment and nothing else. `inventory` also reads
  `K8SI_OCI_COMPARTMENT_ID` and `K8SI_OCI_REGIONS` from that Secret.
- **Admin user**: `scadmin` with the run's password, database `postgres`, `sslmode=require`. Automated backups are off
  (`NONE`): a benchmark database holds nothing to restore, and backups would outlive the DB system.
- **Tags**: the run labels are free-form tags; `inventory` lists the compartment's DB systems of the configured regions
  that carry `sc_inspector_run`.

## GCP DBaaS

`dbaas/k8s_inspector_dbaas/vendors/gcp.py`. Cloud SQL for PostgreSQL. The database id is the Cloud SQL tier (`db-perf-optimized-N-8`,
`db-c4a-highmem-4`, `db-n1-standard-2`).

- **Instance**: standalone (`ZONAL`) in the client's zone, deletion protection off, backups disabled explicitly, disk
  autoresize off, the run's labels and the run name (`sc_inspector_run_name`) as `userLabels`. The edition follows the tier name (performance and memory optimized
  tiers and C4A are Enterprise Plus, the others Enterprise); Enterprise Plus gets the data cache switched off.
- **Network**: private IP only, through private services access on the bench VPC. The platform creates the allocated
  range and the service networking connection once per VPC; `networks.<region>` holds `network`
  (`projects/<project>/global/networks/<vpc>`) and `allocated_range` (the range's name).
- **Storage**: Hyperdisk Balanced when Keeper lists `cloudsql-hyperdisk` as the tier's only storage (C4 and C4A),
  otherwise PD-SSD. PD-SSD sizes come from the storage plan with the documented pd-ssd rates (IOPS and throughput grow
  with the size, capped per VM by its vCPUs); Hyperdisk keeps its default IOPS and throughput and gets the
  schema-derived size.
- **Users**: `postgres` with the run's password is the admin the bootstrap connects as; the bootstrap creates `scadmin`
  with the role `cloudsqlsuperuser` as the workload user (`postgres` cannot create a database it can then use).
- **Credentials**: environment of the Jobs from the Secret `k8s-inspector-dbaas-gcp`: `GOOGLE_PROJECT` and
  `GOOGLE_CREDENTIALS`, the workload identity federation configuration (no secret in it) of a service account that may
  only manage Cloud SQL instances and users. `inventory` lists every instance of the project through the Cloud SQL
  Admin API and keeps those with the `sc_inspector_run` label.
- **Names**: `sc-<run id>`. Cloud SQL keeps an instance name unavailable for up to a week after its deletion; the run id
  is unique per run and the same in all its attempts, whereas the BenchmarkRun name repeats for a client SKU.
- **Untracked instance**: the Terraform-based provider forgets an instance whose create fails or times out, so `down`
  runs the module's `cleanup(run)` after the destroy: it lists the project's instances through the Cloud SQL Admin API,
  deletes those labelled `sc_inspector_run_name=<run>` (the BenchmarkRun name) and waits up to 5 minutes until they are
  gone; a slower deletion fails the Job and the next `down` waits again. The run-name match assumes one cluster (one
  state bucket) per GCP project: a second cluster's runs in the same project would share the label namespace.
