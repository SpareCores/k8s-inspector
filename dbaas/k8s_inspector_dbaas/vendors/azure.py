"""The `azure` vendor: Azure Database for PostgreSQL Flexible Server in the bench VNet (private access).

Port of sc-runner's `azure_dbaas.py`, without the VM: the server joins the delegated subnet of the region's bench VNet
and registers in the region's private DNS zone, both made by the platform (`spec.network`, the ConfigMap's
`dbaas.vendors.azure.networks.<region>`):

- `resource_group`: the region's bench resource group, which holds the server (the identity may write there only);
- `subnet_id`: the subnet delegated to `Microsoft.DBforPostgreSQL/flexibleServers`;
- `private_dns_zone_id`: the private DNS zone linked to the VNet (`privatelink.postgres.database.azure.com`).

The server has no public access, so the clients' network range needs no allowlist. Credentials: the Job's workload
identity (`ARM_CLIENT_ID`, `ARM_TENANT_ID`, `ARM_SUBSCRIPTION_ID`, `ARM_USE_OIDC`, `ARM_OIDC_TOKEN_FILE_PATH`
from the Secret `k8s-inspector-dbaas-azure`); the provider reads them from the environment. Azure has no per-provider
region: every resource takes `location=spec.region`.
"""

import hashlib
import math
import re
from datetime import UTC, datetime

import pulumi
from pulumi_azure_native import Provider, dbforpostgresql

from k8s_inspector_dbaas import azure_preflight, storage_plan
from k8s_inspector_dbaas.spec import DbOutputs, DbSpec

ADMIN_USER = "scadmin"  # the legacy name; the server's admin may create databases, so no separate workload user
ADMIN_DB = "postgres"
TIERS = {"B": "Burstable", "GP": "GeneralPurpose", "MO": "MemoryOptimized"}  # prefixes of Keeper's `sku_name`
# Premium SSD v2 (the legacy choice, https://learn.microsoft.com/azure/virtual-machines/disks-types#premium-ssd-v2):
# 3000 IOPS and 125 MB/s are free, throughput is at most 0.25 MB/s per IOPS; the SKUs that do not offer it
# (Keeper's `compatible_storage_ids` lacks ManagedDiskV2: Burstable) get Premium SSD, as the legacy did.
PREMIUM_V2 = "ManagedDiskV2"
PREMIUM_V2_BASE_IOPS = 3000
PREMIUM_V2_BASE_MBPS = 125
PREMIUM_V2_MBPS_PER_IOPS = 0.25
PREMIUM_V2_PROFILE = {"max_write_iops": 80_000, "max_write_mbps": 1200, "independently_provisioned": True}
PREMIUM_SIZES_GIB = (32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768)  # Premium SSD disk sizes
PREMIUM_TIER = "P30"  # the legacy performance tier of Premium SSD (5000 IOPS, 200 MB/s) ...
PREMIUM_TIER_MAX_GIB = 1024  # ... which a disk of at most 1 TiB can be set to
NAME_MAX = 63


def sku_of(spec: DbSpec) -> tuple[str, str]:
    """`(name, tier)` for ARM: Keeper's `api_reference` is the compute size (`Standard_D4ds_v5`) and its
    `api_reference_object.sku_name` the classic `GP_`, `MO_` or `B_` prefixed form that names the tier."""
    prefix = spec.record["api_reference_object"]["sku_name"].split("_", 1)[0]
    return spec.database_id, TIERS[prefix]


def uses_premium_v2(spec: DbSpec) -> bool:
    return PREMIUM_V2 in spec.record["compatible_storage_ids"]


def storage_for(spec: DbSpec) -> dbforpostgresql.StorageArgs:
    """The disk of the plan, snapped to what Azure accepts."""
    record = spec.record
    min_gib = math.ceil(float(record.get("storage_extra_min") or 32) * 1e9 / 2**30)  # Keeper: GB
    max_gib = int(float(record["storage_extra_max"]) * 1e9 / 2**30) if record.get("storage_extra_max") else None
    if uses_premium_v2(spec):
        plan = storage_plan.plan(
            spec.memory_gib, spec.vcpus, storage_plan.DiskProfile(min_size_gib=min_gib, **PREMIUM_V2_PROFILE)
        )
        size = plan.storage_gib
        # a disk allows 500 IOPS per GiB above 6 GiB; the plan's 64 GiB and up is far above 3000
        iops = max(PREMIUM_V2_BASE_IOPS, plan.iops or 0)
        mbps = min(max(PREMIUM_V2_BASE_MBPS, plan.throughput_mb_s or 0), iops * PREMIUM_V2_MBPS_PER_IOPS)
        args = {"type": "PremiumV2_LRS", "iops": iops, "throughput": int(mbps)}
    else:
        size = next(
            s
            for s in PREMIUM_SIZES_GIB
            if s >= max(min_gib, storage_plan.plan(spec.memory_gib, spec.vcpus).storage_gib)
        )
        args = {"type": "Premium_LRS", "tier": PREMIUM_TIER if size <= PREMIUM_TIER_MAX_GIB else None}
    if max_gib is not None and size > max_gib:
        raise ValueError(f"{spec.database_id}: the disk of {size} GiB is above the SKU's maximum of {max_gib} GiB")
    return dbforpostgresql.StorageArgs(storage_size_gb=size, **args)


def server_name(run: str) -> str:
    """The server name (and DNS label): lowercase letters, digits and `-`, at most 63; a longer run name is cut and
    suffixed with a hash of itself, so that two runs never share a name."""
    name = re.sub(r"[^a-z0-9-]", "-", f"sc-{run}".lower())
    if len(name) > NAME_MAX:
        name = f"{name[: NAME_MAX - 9]}-{hashlib.sha256(run.encode()).hexdigest()[:8]}"
    return name


def zone_of(spec: DbSpec, supported: list[str]) -> str | None:
    """The availability zone of the client node (`westeurope-1` or `1`) when the SKU offers it there, else None: Azure
    picks one."""
    zone = (spec.zone or "").rsplit("-", 1)[-1]
    return zone if zone in supported else None


def provision(spec: DbSpec) -> DbOutputs:
    sku, tier = sku_of(spec)
    storage = storage_for(spec)
    edition = PREMIUM_V2 if uses_premium_v2(spec) else "ManagedDisk"
    zones = azure_preflight.check(spec.region, sku, spec.vcpus, edition)  # raises before anything is created
    network = spec.network
    provider = Provider(
        "azure"
    )  # credentials from the ARM_* environment, never as arguments (they would go to the state)
    server = dbforpostgresql.Server(
        "server",
        server_name=server_name(spec.run),
        resource_group_name=network["resource_group"],
        location=spec.region,
        version=spec.engine_version,
        sku=dbforpostgresql.SkuArgs(name=sku, tier=tier),
        storage=storage,
        administrator_login=ADMIN_USER,
        administrator_login_password=spec.password_output(),
        availability_zone=zone_of(spec, zones),
        network=dbforpostgresql.NetworkArgs(
            delegated_subnet_resource_id=network["subnet_id"],
            private_dns_zone_arm_resource_id=network["private_dns_zone_id"],
        ),
        tags=spec.labels,
        opts=pulumi.ResourceOptions(
            provider=provider,
            custom_timeouts=pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout),
        ),
    )
    return DbOutputs(host=server.fully_qualified_domain_name, admin_user=ADMIN_USER, admin_db=ADMIN_DB)


def inventory() -> list[dict]:
    """The Flexible Servers of the subscription that carry `sc_inspector_run`."""
    path = f"/subscriptions/{azure_preflight.subscription_id()}/providers/Microsoft.DBforPostgreSQL/flexibleServers"
    found = []
    for server in azure_preflight.arm_list(path, azure_preflight.API_VERSION):
        tags = server.get("tags") or {}
        if "sc_inspector_run" in tags:
            expires = datetime.strptime(tags["sc_inspector_expires"], "%Y%m%dt%H%Mz").replace(tzinfo=UTC)
            found.append(
                {
                    "id": server["id"],
                    "run": tags["sc_inspector_run"],
                    "expires_at": expires.isoformat().replace("+00:00", "Z"),
                    "created_at": server["systemData"]["createdAt"],
                }
            )
    return found
