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
  the vendor has room ([autoPlan](configuration.md#autoplan)).
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
