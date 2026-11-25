# Managed databases (DBaaS)

The `pgbench_postgres_dbaas_ro_durable` benchmark measures a vendor-managed PostgreSQL from a client in the same
private network. This is the only live workload with a second paid resource next to the node. A vendor moves from the
legacy pipeline when its provisioner module exists and its DBaaS autoPlan is on ([roadmap](../roadmap.md)).

## Requirements

1. **Coverage**: support as many managed-database kinds as possible.
2. **Lifetime**: the database exists only while its benchmark runs.
3. **Imbalanced capacity**: a database without a client (or a client without a database) must not sit and bill.
4. **Footprint**: nothing heavy running all the time; the control planes have little headroom.

Free prerequisites (private networks, subnets, delegated subnets, DNS zones) are created once per targeted region by
the cluster platform. The benchmark creates only the database (and its user/firewall entry).

## Options

| Option | Coverage | Lifetime / imbalance control | Always-on footprint | Verdict |
|---|---|---|---|---|
| **Job running Pulumi** (programs ported from sc-runner, state in object storage) | Pulumi's bridged providers come from the Terraform providers, and Pulumi can also use any Terraform provider directly, so coverage ≈ Terraform: 8 of 9 vendors (Hetzner has no DBaaS) | full: the controller orders the steps and enforces every deadline | none (Pods only while creating or destroying) | **recommended** |
| Pulumi Kubernetes Operator v2 | same programs | `destroyOnFinalize`, but retries are its own; the controller still needs deadlines | ~128 MiB manager + a workspace Pod per stack | second choice if declarative objects are wanted later |
| Flux tofu-controller | any Terraform provider | `destroyResourcesOnDeletion`; create failures retry forever unless we delete | controller + Flux source-controller | pre-1.0, community-maintained; needs the programs rewritten in HCL |
| Crossplane (upjet providers) | 8 kinds, OVHcloud and Alibaba only community-grade | finalizer delete; a leaked create stops reconciliation | core + one provider Pod per cloud (hundreds of MiB each) + CRD memory in the API server | too heavy for one database at a time |
| HCP Terraform Operator | any Terraform provider | runs in HCP Terraform | 1 Pod + an external SaaS in the failure path | adds a paid SaaS dependency |
| ACK / Config Connector / Azure Service Operator | one cloud each | native | 1 Pod per cloud | three APIs and nothing for the other vendors |

## The flow (decision K11)

A DBaaS run is a `BenchmarkRun` with a `database` block (`vendor`, `id`, `engineVersion`, `region`, optionally
`clientRegion`, `client`, and the database's `vcpus` and `memoryGib`). `region` is the database's region; `clientRegion` is the
client node's and defaults to `region` (they differ where a vendor's database regions are not its server regions, see
[Which database, where, with which client](#which-database-where-with-which-client)). The run is named after the database (its `spec.sku` is the database id), so there is one run per database; `status.clientSku` records the SKU of the client node the cluster launched. Order: **client node first,
database second; on the way out, database destroy and node release in parallel.**

1. **Client node.** The anchor is created in the client's region (`topology.kubernetes.io/region`) with a cheap
   small x86_64 type that the cluster picks (below), always on demand: a lost client would leave a database billing
   with nobody to use it. If no node comes up, nothing was created that bills by the hour and no database is created.
2. **Database.** When the anchor is Running, the controller writes `status.database.phase: Creating`, creates the
   Secret `<run>-db` with a generated password and the Job `dbaas-up-<run>` (image `k8s-inspector-dbaas`, `apps` pool).
   The Job runs the vendor's Pulumi program ([vendor contract](../how-to/add-dbaas-vendor.md)) for the stack
   `k8s-dbaas-<run>`; the password is passed as an environment variable from the Secret and used as a Pulumi secret. The
   Job has no Kubernetes API access: it reports host, port, admin user, admin database and sslmode in its termination
   message, and the controller writes them, with the password, to the Secret `<run>-db` as the `SC_DB_*` variables. A
   vendor that generates the admin password reports it too, in plain text ([the admin password](#the-admin-password)); the controller then deletes the Job.
   Creation has a deadline of 60 minutes (`dbaas.createMinutes`), which is the Job's `activeDeadlineSeconds`. The vendor
   module sets Pulumi `customTimeouts` 10 minutes shorter (`spec.create_timeout`), so Pulumi fails by itself and records
   the resource. The Job's Pod is annotated `karpenter.sh/do-not-disrupt` and has a 15 minute termination grace period:
   on SIGTERM the provisioner passes SIGINT to the Pulumi CLI, which finishes the operation in flight and writes it to
   the state. An operation that starts just before the custom timeout may still be running when the grace period ends;
   Kubernetes then SIGKILLs the Pod, which leaves pending operations in the state. The down Job reports them (see
   Removal) and an operator clears them.
3. **Bootstrap.** The controller starts a Pod on the client node (phase `Bootstrapping`) that runs
   [`dbaas_bootstrap.py`](../../controller/k8s_inspector/dbaas_bootstrap.py) in the benchmark image (it has Python and
   psycopg, so no extra image is pulled; the script comes from the ConfigMap `<run>-boot`). It ports legacy
   `wait_db_ready`: it waits (up to an hour) until the database accepts connections, creates the workload user when the
   vendor's admin user is not it (`grant_role` of the vendor's outputs), creates the database `bench`, probes whether
   `synchronous_commit` can be set per session, applies the durability mode and collects the PostgreSQL settings (legacy
   `pg_repro`) into `/k8si/dbaas/postgres.json`.
4. **Benchmark.** Phase `Ready`: the only step of the run is the pgbench task (`pgbench_postgres_dbaas_ro_durable`, no
   hardware probes: the client is not what is benchmarked), with `SC_DB_*` from the Secret (`envFrom`) and the
   benchmark's own variables from the database's size (`SC_DB_VCPUS`, `SC_PROFILE_VUS` ...). The task hash is the
   legacy one. The container uploads `postgres.json` next to its output (`collect`), and the publisher merges it into
   the `stdout` it writes to `dbaas/<vendor>/<id>/postgres/<version>/standalone/<task>/`: the `postgres` block and the
   `sslmode` and `sync_commit_session_settable` fields, as the legacy runner did.
5. **Removal.** When the benchmark is over for any reason (success, failure, timeout, OOM, lost node, cancel, delete, a
   failed creation), the controller starts `dbaas-down-<run>` and releases the client node in the same reconcile pass.
   The down Job runs on the `apps` pool and does not need the node. **A running `dbaas-up` Job is never deleted**, not
   by a cancel, a delete, a lost node or a failed run: the destroy waits until the up Job ends within its own deadline
   and its Pod is gone, because killing Pulumi in the middle of a create could leave a database the state never
   recorded. Then the down Job cancels a stale lock, refreshes the state, destroys the stack and removes it
   (`pulumi destroy --refresh --remove`). If the state has pending operations (an `up` that was killed hard), the down
   Job does not guess: it fails with a termination message that says an operator must look for the resource in the
   vendor's console and clear the pending creates, and it keeps the state. A failed destroy is retried until it
   succeeds, waiting 1, 2, 4 ... 30 minutes (`status.database.retryAfter`). Only then the controller deletes what
   Pulumi left in the bucket (backups and history of the stack), the Secret, the down Job and the finalizer. The down
   Job is labelled with the run, not owned by it, so the run's foreground deletion never waits for it; the controller
   removes only its own finalizer, with a JSON patch.

`status.database.phase` is the state: `Clearing`, `Creating`, `Bootstrapping`, `Ready`, `Destroying`, `Destroyed`; absent
means nothing was created. `Clearing` is a stack of an earlier run of the same name that is destroyed first, while the run is still `Pending`: the
client node is requested only after that. The down Job is retried like any other, up to 3 failures (for instance pending
operations); then the run ends with a `-1` result `DBAAS_LEFTOVER_STACK: <stack> could not be destroyed (<reason>);
operator action needed` and holds no database (`status.database` is cleared, so nothing occupies the vendor's DBaaS
slot). The usual retry cap applies (three `-1`, then `-5`), so the planner stops by itself. The stack is left to the
sweeper and the operator: it counts in `k8s_inspector_dbaas_instances`, aged from its first update (the oldest Pulumi
history file), so the 8 h alert fires however often the run is created again, and
`k8s_inspector_dbaas_leftover_stack_failures_total{vendor}` counts these give-ups. A run cancelled or deleted while it
is `Clearing` records nothing: it created no database. The phase is written before what it stands for is created, and the Job names follow from the run
name, so a controller that restarts anywhere continues: it creates a missing Job again and runs the destroy of every
run that is over.

### Which database, where, with which client

The planner works like the server planner ([autoPlan](../reference/configuration.md#dbaas-autoplan)) and reuses its
rules; every input is Keeper's data, nothing is a static table:

- **Candidates** are the databases Keeper lists as ACTIVE for the vendor (`/databases`) and eligible for the task
  (`/inspector/databases/task_block_reason_codes`; fail closed). The PostgreSQL version is `dbaas.engineVersion` when the
  database offers it, else its newest.
- **Timing** files go to `dbaas/<vendor>/<id>/postgres/<version>/standalone/timing/<run_id>/` (names in
  [output-contract.md](../reference/output-contract.md)). The controller records them in `status.database.timing` as
  the phases happen; the publisher writes them once the run's results are published, and again when the destroy
  finishes (`api_destroy_start`, `api_destroy_end`).
- **Due** is decided from `dbaas/<vendor>/<id>/postgres/<version>/standalone/<task>/meta.json` in git with the same
  function as for servers: a changed task hash, `-1` after 2 h, `-4` after 2 h and 6 h, `-5` after three failures in a
  row, never again for the other results. There is no schedule directory and no variant.
- **Region**: the database's regions with a standalone on-demand price (Keeper's `none` level, else `single-zone`) that
  have a client region, cheapest first; after a failed attempt the database region whose client region that attempt used
  goes last, so the retry tries another. The client regions of a database region, in the order they are tried: the
  region itself when the ConfigMap has a network for it (`dbaas.vendors.<vendor>.networks.<client region>`), and only
  then, if it has none and the vendor sets `dbaas.vendors.<vendor>.clientRegionByCity` (default false), the regions
  with the same Keeper country and `city` that have one; the first with a client wins. For most vendors the database
  and server regions are the same names, so the region is its own client region and the flag stays off. OVH's are not
  (databases in `GRA`, servers in `GRA7`, `GRA9`, `GRA11`), and the city ties them, so OVH sets it. The run records `database.region`
  and, when it differs, `database.clientRegion`. An operator may name both (`run --region`, `--client-region`); an
  explicit client region is an override with no city check, with or without `--region`.
- **Client**: the anchor requires SCarpenter's labels, not a SKU: this vendor and the client region, on-demand,
  `kubernetes.io/arch` amd64 (the benchmark image is amd64 only), `vcpus` 2 to 4, `memory-mib` 4096 to 8192,
  `accelerator-count` `DoesNotExist` (SCarpenter leaves it undefined on types without an accelerator) and `cpu-allocation`
  Dedicated or Shared (the bounds are constants in `dbaas_client.py`). Karpenter launches the cheapest offered type that
  fits: the pgbench client is not what is benchmarked and the workload is server heavy, so its speed does not matter,
  and a type that is blocked only in that region (no vCPU quota of its family, say) is passed over without a wait. An
  operator's `run --database <id> <sku>` replaces the labels with `api-reference In [<sku>]` (`database.client`).
  A launch failure (no type fits, ICE, quota, boot failure) of a DBaaS client writes no per-SKU backoff, which would
  hold back server runs of the same SKU: the run ends `-1` (`DBAAS_CLIENT_FAILED: <reason>`) with the client region
  recorded, so the next plan tries another database region first. After three failed NodeClaims of an attempt the run
  gives up (a server run after two). Like a command that does not start, `DBAAS_CLIENT_FAILED` (and
  `DBAAS_LEFTOVER_STACK`) counts as a start retry: the third in a row ends as `-5`. This needs the failure to be
  published, which it is not while an older good result of the database exists (a failure never replaces a good
  result): then the planner retries after the usual wait, without region rotation and without the cap.
- **One node per run**: every anchor, of server and DBaaS runs, requires the node label `inspector.sparecores.com/run`
  with the run's name (the bench NodePool allows the key with an `Exists` requirement, so Karpenter stamps it on the
  node), so no other run's Pod schedules on the node and a run's NodeClaims and Nodes are recognised by the label. An
  anchor made before the label existed has none: its NodeClaims are matched by vendor and SKU, and an anchor that
  finds no node ends with the usual provisioning deadline. A database has one run, found by `spec.database.id`,
  whatever the run is named.
- **Limits**: `dbaasMaxInFlight` and `dbaasMaxUsdPerHour` of the vendor ([below](#limits)).

### The network

The database joins the client node's private network. The cluster platform creates that network once per vendor and
region (free prerequisites: networks, subnets, delegated subnets, DNS zones) and describes it in the ConfigMap
`k8s-inspector`, `dbaas.vendors.<vendor>.networks.<client region>`: an opaque mapping, keyed by the region of the client
node (the bench region), that the controller passes unchanged to the vendor module (`spec.network`). A region without an
entry is not used as a client region. The same section names an optional ServiceAccount
for workload identity. The Job's cloud credentials come from the optional Secret `k8s-inspector-dbaas-<vendor>`
(`envFrom`).

The rule: **private network first.** Where the vendor's databases can join a private network that the client node is on,
the database goes there, and an allowlist the vendor needs (UpCloud, Vultr) is the platform's client CIDRs in the
`network` mapping. **Only where the vendor's nodes have no private network (OVH today)** the database gets a public
endpoint with TLS and an IP allowlist of the client node's public addresses. The controller reads them from the client's
SCarpenter annotations (`scarpenter.sparecores.com/public-ipv4`, `public-ipv6`) on the NodeClaim and the Node, and the Node's
`ExternalIP` addresses, when it creates the up Job and passes them as `DbSpec.client_addresses` (`/32` for IPv4,
`/128` for IPv6). SCarpenter writes the annotation when the vendor returns the address at create, otherwise with its next
sweep (about two minutes), so the controller waits for an address before it creates the up Job: it checks again every
15 seconds for up to 5 minutes from the start of `Creating`, then creates the Job anyway with an empty list, so a vendor
that needs no address is never blocked and one that needs it fails with a clear error.

### The admin password

The controller generates the run's password into the Secret `<run>-db` as `K8SI_RUN_PASSWORD`, which is never
overwritten, and the up and down Jobs get it as `DBAAS_PASSWORD` by `secretKeyRef`. `SC_DB_PASSWORD`, the one the
benchmark and the bootstrap use, is the same until a vendor that generates the admin password itself and cannot be given
one (OVH) returns it as `DbOutputs.password`, a Pulumi secret: the controller then writes that one to `SC_DB_PASSWORD`.

The Jobs have no API access by design, so the up Job reports through its termination message, and the vendor-generated
password is in it in plain text. The exposure is accepted:

- The controller deletes the up Job and its Pod as soon as it has read the outputs and written the run's Secret, so the
  password does not linger in Pod status.
- While it is there, Pod status is readable by the controller and by humans with `view` on the `inspector` namespace,
  not by workload Pods (they have no service-account tokens).
- The database only accepts the client node's address or the private network, requires TLS, holds no data and lives
  about an hour.
- The password also sits in the Secret `<run>-db`, which the same readers could otherwise reach.

Pod logs and events do not carry it: Pulumi prints secrets as `[secret]`. It is also in the Pulumi state, encrypted by
the stack passphrase like every secret there.

### Limits

DBaaS runs have their own per-vendor limits, next to the server ones and counted only over DBaaS runs (a DBaaS run does
not count against `maxInFlight` and `maxUsdPerHour`, and a server run does not count against these):

- `dbaasMaxInFlight` (default 1, 0 pauses): DBaaS runs that hold a client node or a database that is not yet destroyed.
- `dbaasMaxUsdPerHour` (default unset: no cap): the hourly on-demand price of database plus client node, summed over the
  vendor's DBaaS runs in flight. The price of a run is recorded in its `spec.database.usdPerHour` when it is created: the
  database's standalone price in the chosen region from Keeper's price data, plus the `min_price_ondemand` of the dearest Keeper server in the client region that matches the same labels
  (the most the cluster may launch). A new run is refused when the running ones plus its own price would pass the cap, so the
  planner tries the next database; a forced run is refused with the reason. With a cap set, a price that Keeper does not
  know (a client it does not list) is refused too.

The planner creates at most one new DBaaS run per vendor and cycle. The checks are not atomic: a forced run and an
autoPlan cycle at the same instant can both pass, so the vendor may briefly exceed a limit by one run.

### Pulumi state and leaks

Pulumi state is in the controller's bucket (`K8SI_BUCKET`), below `K8SI_KEY_PREFIX`, in `dbaas-state/`, in Pulumi's file
(DIY) backend. One stack per run, named `k8s-dbaas-<run>` in the project `k8s-inspector-dbaas`; its state is
`dbaas-state/.pulumi/stacks/k8s-inspector-dbaas/k8s-dbaas-<run>.json`. Secrets in the state (the database password) are
encrypted with the passphrase in the Secret `k8s-inspector-dbaas-state` (key `passphrase`). The prefix has no expiry
lifecycle rule; a stack's state is deleted only after its destroy succeeded. The bucket and prefix must not be shared
between clusters: a sweeper that finds a stack it has no run for destroys it.

The Jobs reach the bucket through their own identity, not the controller's: the role `K8SI_DBAAS_ROLE_ARN` (web
identity, for the ServiceAccount `k8s-inspector-dbaas`) limited to `dbaas-state/`. The backend URL is
`s3://<bucket>/<prefix>dbaas-state?region=<r>&awssdk=v2&profile=k8si-state`, and the Job writes an AWS config file with
that one profile (role and token file) and points `AWS_CONFIG_FILE` at it. No `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`
or `AWS_REGION` is set, so the state credentials never reach the vendor's providers. A vendor module creates its own
provider explicitly and takes its credentials from the environment (the optional Secret), never as provider arguments,
which Pulumi stores in the state and which would go stale when a key is rotated.

Deadlines: database create 60 min, bootstrap 60 min of waiting, benchmark 120 min (the task timeout), destroy retried
until it succeeds. The anchor's deadline covers all of them and the controller keeps its hold alive meanwhile.

Guarantees:

- **Controller restart:** the run phase is in `status.database` and the names follow from the run name, so the
  controller resumes and runs `down`.
- **Create failure or timeout:** `down` runs anyway, after the up Job has ended; the Pulumi state holds what was already
  created. The task ends `-1` with `DBAAS_CREATE_FAILED`, `DBAAS_CREATE_TIMEOUT` or `DBAAS_BOOTSTRAP_FAILED`
  ([exit codes](../reference/exit-codes.md)), which the usual retry rules bound.
- **Create failure leaves an untracked database:** a provider may drop a resource from the state when its create fails
  or times out. `down` therefore calls the vendor module's optional `cleanup(run)` (the BenchmarkRun name, which the
  controller's and the sweeper's `down` Jobs both have) after the destroy, which deletes what the run's tag still
  matches. A module without that function relies on the state alone.
- **`BenchmarkRun` deleted:** a finalizer keeps the object until `down` succeeded; deleting an active run cancels it.
- **Run gone, stack left:** the sweeper (every 10 minutes) lists the stacks `k8s-dbaas-*` in the bucket; one whose first update is older than
  `dbaas.sweepAfterHours` (6 h) and whose run does not exist is destroyed by a `dbaas-down-<run>` Job that belongs to no run,
  and retried every cycle until it succeeds. The sweeper lists the bucket whatever the configuration says, also when
  `dbaas.vendors` is empty or a vendor was removed from it: such a stack is destroyed with the vendor's ServiceAccount
  and Secret by their names (the default ServiceAccount when `dbaas.vendors.<vendor>` is gone). A stack of a vendor the
  ConfigMap does not know at all is only reported (the metrics with `vendor="unknown"`, and a log line): the Job would have
  no credentials. A new run whose stack name already has a state file destroys it first, before its client node (`Clearing`).
- **Database the state never recorded** (Job killed between the cloud create and the state write; the up Job is never
  deleted, but a node can die): every database is tagged with the run id (`sc_inspector_run`) and an expiry time
  (`sc_inspector_expires`), passed to the vendor module, which applies them where the vendor supports tags. The module's
  `inventory()` lists the vendor's databases that carry the tag. The sweeper runs it every 30 minutes for each configured
  vendor in a Job `dbaas-inventory-<vendor>` (same credentials and ServiceAccount as up and down), and the controller
  exports `k8s_inspector_dbaas_tagged_databases{vendor}` and `k8s_inspector_dbaas_tagged_oldest_age_seconds{vendor}`.
  The monitoring alerts when the oldest is older than 8 h ([monitoring](monitoring.md)). The metrics
  `k8s_inspector_dbaas_instances` and `k8s_inspector_dbaas_oldest_age_seconds` count the databases the controller knows
  about. An inventory Job that fails or reports something unreadable drops the vendor's tagged series (a transient failure makes them disappear until the next good inventory), and
  `k8s_inspector_dbaas_inventory_last_success_timestamp_seconds{vendor}` (the controller's start time until the first success, so a vendor that never succeeds alerts 2 h after the start)
  alerts when it is older than 2 h. This is alert-only; no tag-driven automatic delete.

Retries follow the normal `-1`/`-4` backoff with the cap of three, instead of today's retry every 2 h forever.

Billing note: UpCloud and OVHcloud bill databases by the hour, AWS RDS has a 10-minute minimum, so a failed attempt
costs that minimum; creating the database only after the client node is up avoids the common failure.

### OVHcloud

The module is `vendors/ovh.py`; its credentials are the OAuth2 service account of the Secret `k8s-inspector-dbaas-ovh`
(`OVH_ENDPOINT`, `OVH_CLIENT_ID`, `OVH_CLIENT_SECRET`, `OVH_CLOUD_PROJECT_SERVICE`).

- **Regions**: OVH's database regions (`GRA`, `BHS`, `DE`) are not its compute regions (`GRA11`, `BHS5`, `DE1`); Keeper lists
  both, in the same city. The database is created in `spec.region`; the platform's network entry is keyed by
  `spec.client_region`, the client node's compute region of that city (`clientRegionByCity: true`).
- **Network**: SCarpenter's OVH nodes have a public address only (no vRack), so the database is reached through its public
  endpoint (TLS only). Its IP restriction allows the IPv4 /32 entries of `spec.client_addresses` and nothing else; with none, the module
  fails with a clear error, because OVH denies every client without a restriction.
- **Plan and flavor** come from Keeper's `api_reference_object`; the plans Keeper lists run as OVH sells them, so some include replica nodes (`1 + max_read_replicas` nodes, which OVH requires); Keeper's listing and eligibility decide which plans run. The disk
  is the schema-derived size, within the flavor's included size and maximum (Keeper's `storage_size` and
  `storage_extra_max`), in steps of 10 GiB.
- **Admin user**: `avnadmin` on `defaultdb`. OVH generates its password and it cannot be chosen; the module returns it in
  `DbOutputs.password`.
- **Tag**: OVH databases have no tags, so the description carries the run id and the expiry (`inventory` reads them).

### Operators

`k8s-inspector run <vendor> --database <id> [--region <region>] [--client-region <region>] [<client sku>] [--force]` (one client SKU instead of the labels) starts a DBaaS run of one
database now ([forced run](../how-to/run-forced.md#managed-database)); `k8s-inspector plan --dry-run` also prints the
database it would start.

## Quota pre-flight

Before creating a database on Azure, the provisioner checks that the subscription has vCPU headroom in the SKU's quota
family. The family key comes from the vendor API (Azure ARM Resource SKUs `family`, for example `standardDadv6Family`),
never from sc-data `family` or `series`: those are catalogue classifications (workload class and normalized hardware
line), not quota keys. A family the usages API does not list (Flexible Server lists only some families, none for v6
SKUs) is not an error: it is logged at info level and only the regional cores quota is checked.

The pre-flight runs in the `azure` vendor module before the first resource (`azure_preflight.py`), with the Job's workload
identity. It refuses, with a message that names the cause, when the subscription is restricted for the offer in the
region, the region does not list the SKU (or lists it as disabled), the region lacks the storage the plan needs, or the
regional cores or the family quota are short. These refusals mean the database cannot be created in this region
whatever happens, so they are a `RegionUnavailable` error: its message starts with `DBAAS_REGION_UNAVAILABLE:`, which
survives Pulumi's wrapping of the exception. The up Job fails at once and the run ends `-1` with
`DBAAS_REGION_UNAVAILABLE: <cause>` (any other create failure is `DBAAS_CREATE_FAILED`), with the client region
recorded. The next plan puts that region last, and the result counts as a start retry like `DBAAS_CLIENT_FAILED`, so the
third in a row ends as `-5`. Only the last failed region is remembered, so with two unavailable regions the plan
alternates between them until the cap ends it. A vendor module raises `RegionUnavailable` for what is region-fatal. A check that cannot be made
(ARM unreachable, an unexpected answer) is logged and does not stop the create; the client node's zone is then requested
as is.

## Vendor notes

### AWS (Amazon RDS for PostgreSQL)

One single-AZ RDS instance (`db.*` class from Keeper's `api_reference`) on gp3, encrypted, not publicly accessible, with
no automated backups, no final snapshot and no deletion protection. The platform provides, per bench region, a DB subnet
group over the bench VPC's subnets and a security group that allows 5432 from the bench nodes
(`networks.<region>`: `subnet_group`, `security_group_id`). The instance goes to the client's availability zone when
`zone` is a zone name of the region. RDS for PostgreSQL fixes the gp3 I/O of a volume below 400 GiB at 3000 IOPS and
125 MiB/s; a database whose I/O budget is higher gets a 400 GiB volume (baseline 12000 IOPS and 500 MiB/s), and explicit
IOPS and throughput only above that. The Jobs use the ServiceAccount `k8s-inspector-dbaas-aws` (web identity) and the
Secret `k8s-inspector-dbaas-aws` with `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`. The IAM role is limited to
creating PostgreSQL single-AZ instances tagged `ManagedBy=k8s-inspector`, describing instances and deleting tagged
ones, in any region, so it can delete any tagged `sc-*` instance, not only the current run's; `inventory` reads the tags of the instances of every enabled region
and fails (after logging the region) when one cannot be listed, so the staleness alert fires.

The instance identifier is `sc-<run_id>` (unique per run, stable across its attempts).

Comparability: the storage jumps to 400 GiB (12000 IOPS and 500 MiB/s baseline) once the planned I/O budget exceeds
3000 IOPS or 125 MiB/s. The legacy AWS setup effectively ran 3000 IOPS and 125 MiB/s, so results of large instances are not
directly comparable with it.

### Azure (Flexible Server for PostgreSQL)

- **Network**: Flexible Server with private access (VNet integration). In every bench region the platform creates a
  `dbaas` subnet delegated to `Microsoft.DBforPostgreSQL/flexibleServers` in the bench VNet (a separate address range,
  so the node subnet is untouched); one global private DNS zone `privatelink.postgres.database.azure.com` is linked to
  every bench VNet. `networks.<region>` holds `resource_group`, `subnet_id` and `private_dns_zone_id`. The server is
  created in the dedicated DBaaS resource group (the Job's identity may write servers only there), named from the run
  id (a globally unique DNS name that a rerun of the same client SKU does not repeat), and has no public access, so no
  allowlist is needed.
- **Identity**: Azure workload identity. The federated credential of a user-assigned managed identity trusts the Job's
  ServiceAccount `k8s-inspector-dbaas-azure`; the Job's projected token (audience `sts.amazonaws.com`, the one the state
  profile uses) is the client assertion. The Secret `k8s-inspector-dbaas-azure` holds the identity's client id, tenant and
  subscription ids and `ARM_USE_OIDC` / `ARM_OIDC_TOKEN_FILE_PATH` for the provider.
- **Sizing**: the tier comes from the `GP_`, `MO_` or `B_` prefix of Keeper's `api_reference_object.sku_name`, the compute
  size is Keeper's `api_reference`. Premium SSD v2 when Keeper's `compatible_storage_ids` has `ManagedDiskV2`, with the
  plan's IOPS and throughput raised to the P30 equivalent (5000 IOPS, 200 MB/s, the legacy floor); else Premium SSD (Burstable) at the P30
  tier. The disk size is the plan's, within Keeper's `storage_extra_min`/`storage_extra_max`.
- **Admin**: `scadmin` on `postgres`; it may create the database `bench`, so there is no separate workload user.
  `sslmode=require`.
- **Zone**: the client node's zone when the SKU is offered there, else Azure chooses.
