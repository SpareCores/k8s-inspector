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

A DBaaS run is a `BenchmarkRun` with a `database` block (`vendor`, `id`, `engineVersion`, `region` and the database's
`vcpus` and `memoryGib`). Its SKU is the client server, so the run's lease is the client's. Order: **client node first,
database second; on the way out, database destroy and node release in parallel.**

1. **Client node.** The anchor is created in the database's region (`topology.kubernetes.io/region`) with a cheap
   client type picked from Keeper's server data (below), always on demand: a lost client would leave a database billing
   with nobody to use it. If no node comes up, nothing was created that bills by the hour and no database is created.
2. **Database.** When the anchor is Running, the controller writes `status.database.phase: Creating`, creates the
   Secret `<run>-db` with a generated password and the Job `dbaas-up-<run>` (image `k8s-inspector-dbaas`, `apps` pool).
   The Job runs the vendor's Pulumi program ([vendor contract](../how-to/add-dbaas-vendor.md)) for the stack
   `k8s-dbaas-<run>`; the password is passed as an environment variable from the Secret and used as a Pulumi secret. The
   Job has no Kubernetes API access: it reports host, port, admin user, admin database and sslmode in its termination
   message, and the controller writes them, with the password, to the Secret `<run>-db` as the `SC_DB_*` variables.
   Creation has a deadline of 60 minutes (`dbaas.createMinutes`), which is the Job's `activeDeadlineSeconds`. The vendor
   module sets Pulumi `customTimeouts` 10 minutes shorter (`spec.create_timeout`), so Pulumi fails by itself and records
   the resource. The Job's Pod is annotated `karpenter.sh/do-not-disrupt` and has a 15 minute termination grace period:
   on SIGTERM the provisioner passes SIGINT to the Pulumi CLI, which finishes the operation in flight and writes it to
   the state.
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
means nothing was created. `Clearing` is a stack of an earlier run of the same name that is destroyed first; the run
then starts over, without a result for the database. The phase is written before what it stands for is created, and the Job names follow from the run
name, so a controller that restarts anywhere continues: it creates a missing Job again and runs the destroy of every
run that is over.

### Which database, where, with which client

The planner works like the server planner ([autoPlan](../reference/configuration.md#dbaas-autoplan)) and reuses its
rules; every input is Keeper's data, nothing is a static table:

- **Candidates** are the databases Keeper lists as ACTIVE for the vendor (`/databases`) and eligible for the task
  (`/inspector/databases/task_block_reason_codes`; fail closed). The PostgreSQL version is `dbaas.engineVersion` when the
  database offers it, else its newest.
- **Due** is decided from `dbaas/<vendor>/<id>/postgres/<version>/standalone/<task>/meta.json` in git with the same
  function as for servers: a changed task hash, `-1` after 2 h, `-4` after 2 h and 6 h, `-5` after three failures in a
  row, never again for the other results. There is no schedule directory and no variant.
- **Region**: the database's regions with a standalone on-demand price (Keeper's `none` level, else `single-zone`), that
  have a DBaaS network in the ConfigMap (`dbaas.vendors.<vendor>.networks.<region>`), cheapest first; after a failed
  attempt the region of that attempt goes last, so the retry tries another.
- **Client**: Keeper's servers in that region that are x86_64 (the benchmark image is amd64 only), without
  accelerator, with 2-4 vCPUs and 4-8 GiB, dedicated CPUs when there are any (else shared), cheapest after a Tukey price
  fence (Q3 + 1.5 IQR over the region's own candidates), without SKUs SCarpenter does not offer, that wait for a launch
  retry or that already have a run. Same sizing as the legacy `rank_dbaas_client_instances`, so results stay comparable.
- **Limits**: `dbaasMaxInFlight` and `dbaasMaxUsdPerHour` of the vendor ([below](#limits)).

### The network

The database joins the client node's private network. The cluster platform creates that network once per vendor and
region (free prerequisites: networks, subnets, delegated subnets, DNS zones) and describes it in the ConfigMap
`k8s-inspector`, `dbaas.vendors.<vendor>.networks.<region>`: an opaque mapping that the controller passes unchanged to the
vendor module (`spec.network`). A region without an entry is not used. The same section names an optional ServiceAccount
for workload identity. The Job's cloud credentials come from the optional Secret `k8s-inspector-dbaas-<vendor>`
(`envFrom`). `DbSpec` has no client address: for a vendor that needs an allowlist (OVH, UpCloud, Vultr, OCI) the platform
puts the client CIDRs in the `network` mapping, and the module applies them.

### Limits

DBaaS runs have their own per-vendor limits, next to the server ones and counted only over DBaaS runs (a DBaaS run does
not count against `maxInFlight` and `maxUsdPerHour`, and a server run does not count against these):

- `dbaasMaxInFlight` (default 1, 0 pauses): DBaaS runs that hold a client node or a database that is not yet destroyed.
- `dbaasMaxUsdPerHour` (default unset: no cap): the hourly on-demand price of database plus client node, summed over the
  vendor's DBaaS runs in flight. The price of a run is recorded in its `spec.database.usdPerHour` when it is created: the
  database's standalone price in the chosen region from Keeper's price data, plus the client's `min_price_ondemand`
  from Keeper's server data. A new run is refused when the running ones plus its own price would pass the cap, so the
  planner tries the next database; a forced run is refused with the reason. With a cap set, a price that Keeper does not
  know (a client it does not list) is refused too.

The planner creates at most one new DBaaS run per vendor and cycle.

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
- **`BenchmarkRun` deleted:** a finalizer keeps the object until `down` succeeded; deleting an active run cancels it.
- **Run gone, stack left:** the sweeper (every 10 minutes) lists the stacks `k8s-dbaas-*` in the bucket; one older than
  `dbaas.sweepAfterHours` (6 h) whose run does not exist is destroyed by a `dbaas-down-<run>` Job that belongs to no run,
  and retried every cycle until it succeeds. The sweeper lists the bucket whatever the configuration says, also when
  `dbaas.vendors` is empty or a vendor was removed from it: such a stack is destroyed with the vendor's ServiceAccount
  and Secret by their names (the default ServiceAccount when `dbaas.vendors.<vendor>` is gone). A stack of a vendor the
  ConfigMap does not know at all is only reported (the metrics with `vendor="unknown"`, and a log line): the Job would have
  no credentials. A new run whose stack name already has a state file destroys it first (`Clearing`).
- **Database the state never recorded** (Job killed between the cloud create and the state write; the up Job is never
  deleted, but a node can die): every database is tagged with the run id (`sc_inspector_run`) and an expiry time
  (`sc_inspector_expires`), passed to the vendor module, which applies them where the vendor supports tags. The module's
  `inventory()` lists the vendor's databases that carry the tag. The sweeper runs it every 30 minutes for each configured
  vendor in a Job `dbaas-inventory-<vendor>` (same credentials and ServiceAccount as up and down), and the controller
  exports `k8s_inspector_dbaas_tagged_databases{vendor}` and `k8s_inspector_dbaas_tagged_oldest_age_seconds{vendor}`.
  The monitoring alerts when the oldest is older than 8 h ([monitoring](monitoring.md)). The metrics
  `k8s_inspector_dbaas_instances` and `k8s_inspector_dbaas_oldest_age_seconds` count the databases the controller knows
  about. This is alert-only; no tag-driven automatic delete.

Retries follow the normal `-1`/`-4` backoff with the cap of three, instead of today's retry every 2 h forever.

Billing note: UpCloud and OVHcloud bill databases by the hour, AWS RDS has a 10-minute minimum, so a failed attempt
costs that minimum; creating the database only after the client node is up avoids the common failure.

### Operators

`k8s-inspector run <vendor> --database <id> [--region <region>] [<client sku>] [--force]` starts a DBaaS run of one
database now ([forced run](../how-to/run-forced.md#managed-database)); `k8s-inspector plan --dry-run` also prints the
database it would start.

## Quota pre-flight

Before creating a database on Azure, the provisioner checks that the subscription has vCPU headroom in the SKU's quota
family. The family key comes from the vendor API (Azure ARM Resource SKUs `family`, for example `standardDadv6Family`),
never from sc-data `family` or `series`: those are catalogue classifications (workload class and normalized hardware
line), not quota keys. A family the usages API does not list (Flexible Server lists only some families, none for v6
SKUs) is not an error: it is logged at info level and only the regional cores quota is checked.
