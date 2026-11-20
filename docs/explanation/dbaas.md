# Managed databases (DBaaS)

The `pgbench_postgres_dbaas_ro_durable` benchmark measures a vendor-managed PostgreSQL from a client in the same
private network. This is the only live workload with a second paid resource next to the node. It is a later phase;
the legacy pipeline keeps it until then ([roadmap](../roadmap.md)).

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

## Recommended flow (decision K11)

A DBaaS run is a `BenchmarkRun` with a `database` block. Order: **client node first, database second, database
removed first.**

1. **Client node.** The anchor is created in the database's region (`topology.kubernetes.io/region`) with a cheap
   client type (ported `companion_picker`). If no node comes up, nothing was created that bills by the hour.
2. **Database.** When the anchor is Running, the controller creates a Job `dbaas-up-<run>` (image
   `k8s-inspector-dbaas`: Pulumi + only the needed providers; one stack per run, state in object storage). The
   controller generates the password and passes it as secret config; the Job writes host and port to a Secret owned by
   the run. The node waits on a cheap client meanwhile (a few cents for the 5-15 minutes a database takes).
3. **Benchmark.** The pgbench step runs with `SC_DB_*` from that Secret.
4. **Removal.** Right after the pgbench step ends (success or not), the controller runs `dbaas-down-<run>` and only then
   releases the node.

Deadlines: database create 60 min (today's wait), benchmark 120 min, destroy retried until it succeeds. On every
failure path the controller runs `down` first. Every database is tagged with the run id and an expiry time.

Leak protection: the controller lists stacks `k8s-dbaas-*` older than 6 h without a live run and runs `down`; the
monitoring alerts on managed databases older than 8 h ([monitoring](monitoring.md)). Retries follow the normal
`-1`/`-4` backoff with the cap of three, instead of today's retry every 2 h forever.

Billing note: UpCloud and OVHcloud bill databases by the hour, AWS RDS has a 10-minute minimum, so a failed attempt
costs that minimum; creating the database only after the client node is up avoids the common failure.

## Quota pre-flight

Before creating a database on Azure, the provisioner checks that the subscription has vCPU headroom in the SKU's quota
family. The family key comes from the vendor API (Azure ARM Resource SKUs `family`, for example `standardDADSv6Family`),
never from sc-data `family` or `series`: those are catalogue classifications (workload class and normalized hardware
line), not quota keys. A family the usages API does not list (Flexible Server lists only some families, none for v6
SKUs) is not an error: it is logged at info level and only the regional cores quota is checked.
