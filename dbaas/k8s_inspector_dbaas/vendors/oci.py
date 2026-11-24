"""The `oci` vendor: OCI Database with PostgreSQL, one standalone DB system (one node) in the bench VCN.

`spec.network` (the platform's `dbaas.vendors.oci.networks.<region>`): `compartment_id`, `subnet_id` (the bench subnet
of the region) and `nsg_ids` (network security groups of the DB system; one that lets the client nodes reach port 5432).
The DB system has only a private endpoint in that subnet, so no allowlist is applied here.

Sizing comes from Keeper's record: `api_reference_object` holds the flex shape and its OCPU and memory, which OCI takes
as they are. OCI sizes no disk: storage is database optimized, grows with the data and has a performance tier
(guaranteed IOPS regardless of size). The lowest tier Keeper lists for the database (75,000 IOPS) is far above the
I/O budget of `storage_plan` (25 write IOPS per vCPU), so no larger tier is ever needed.

Storage is AD-local, in the availability domain of the client node: the closest to the single-zone setup of the other
vendors. Regionally durable storage replicates every write to other availability domains, which adds a network round
trip to each durable commit and would make the commit benchmark measure OCI's multi-AD durability, not a standalone
database. Keeper's price does not depend on the choice.

The admin user is created with the DB system (`scadmin`, database `postgres`), and the platform's Secret
`k8s-inspector-dbaas-oci` carries the API key of the Jobs' identity as the `TF_VAR_*` variables the OCI provider and
the OCI SDK of `inventory` read. `inventory` also needs `K8SI_OCI_COMPARTMENT_ID` and `K8SI_OCI_REGIONS` (comma
separated) from that Secret: it runs without a spec, so it cannot ask `spec.network`.
"""

import os
import re
from datetime import UTC, datetime

import oci
import pulumi
import pulumi_oci

from k8s_inspector_dbaas.spec import DbOutputs, DbSpec

ADMIN_USER = "scadmin"
ADMIN_DB = "postgres"
TAG_RUN = "sc_inspector_run"
TAG_EXPIRES = "sc_inspector_expires"
CREDENTIAL_ENV = {  # OCI SDK config key -> the variable the Secret provides (the provider reads the same ones)
    "tenancy": "TF_VAR_tenancy_ocid",
    "user": "TF_VAR_user_ocid",
    "fingerprint": "TF_VAR_fingerprint",
    "key_content": "TF_VAR_private_key",
}


def provision(spec: DbSpec) -> DbOutputs:
    compartment_id, subnet_id, nsg_ids = _network(spec)
    shape, ocpus, memory_gib = _shape(spec.record)
    provider = pulumi_oci.Provider(f"{spec.run}-provider", region=spec.region)  # credentials from the environment
    domains = pulumi_oci.identity.get_availability_domains(
        compartment_id=compartment_id, opts=pulumi.InvokeOptions(provider=provider)
    )
    availability_domain = pick_availability_domain([d.name for d in domains.availability_domains], spec.zone)
    db = pulumi_oci.psql.DbSystem(
        spec.run,
        compartment_id=compartment_id,
        display_name=spec.run,
        db_version=spec.engine_version,
        shape=f"PostgreSQL.{shape}",
        instance_count=1,
        instance_ocpu_count=ocpus,
        instance_memory_size_in_gbs=memory_gib,
        system_type="OCI_OPTIMIZED_STORAGE",
        storage_details=pulumi_oci.psql.DbSystemStorageDetailsArgs(
            system_type="OCI_OPTIMIZED_STORAGE",
            is_regionally_durable=False,
            availability_domain=availability_domain,
            iops=str(_iops_tier(spec.record)),
        ),
        network_details=pulumi_oci.psql.DbSystemNetworkDetailsArgs(subnet_id=subnet_id, nsg_ids=nsg_ids),
        credentials=pulumi_oci.psql.DbSystemCredentialsArgs(
            username=ADMIN_USER,
            password_details=pulumi_oci.psql.DbSystemCredentialsPasswordDetailsArgs(
                password_type="PLAIN_TEXT", password=spec.password_output()
            ),
        ),
        # No automated backups: a benchmark database holds nothing to restore, and backups would outlive the DB system.
        management_policy=pulumi_oci.psql.DbSystemManagementPolicyArgs(
            backup_policy=pulumi_oci.psql.DbSystemManagementPolicyBackupPolicyArgs(kind="NONE"),
        ),
        freeform_tags=spec.labels,
        opts=pulumi.ResourceOptions(
            provider=provider,
            custom_timeouts=pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout),
        ),
    )
    return DbOutputs(
        host=db.network_details.apply(lambda n: n.primary_db_endpoint_private_ip),
        admin_user=ADMIN_USER,
        admin_db=ADMIN_DB,
    )


def _network(spec: DbSpec) -> tuple[str, str, list[str]]:
    try:
        n = spec.network
        return n["compartment_id"], n["subnet_id"], list(n["nsg_ids"])
    except KeyError as e:
        raise ValueError(f"oci network of {spec.region} lacks {e}: {spec.network}") from e


def _shape(record: dict) -> tuple[str, int, int]:
    """The flex shape, OCPUs and memory (GiB) of Keeper's `api_reference_object`."""
    try:
        ref = record["api_reference_object"]
        return ref["shape"], int(ref["instance_ocpu_count"]), int(ref["instance_memory_size_in_gbs"])
    except KeyError as e:
        raise ValueError(
            f"the Keeper record has no {e} in api_reference_object: {record.get('api_reference_object')}"
        ) from e


def _iops_tier(record: dict) -> int:
    """The lowest performance tier of `compatible_storage_ids` (`optimized-75k-iops` is 75,000 IOPS)."""
    tiers = [
        int(m[1]) * 1000
        for i in record.get("compatible_storage_ids") or []
        if (m := re.fullmatch(r"optimized-(\d+)k-iops", i))
    ]
    if not tiers:
        raise ValueError(
            f"the Keeper record lists no optimized-<n>k-iops storage: {record.get('compatible_storage_ids')}"
        )
    return min(tiers)


def pick_availability_domain(names: list[str], zone: str | None) -> str:
    """The availability domain of the client's `zone` (its name ends with the zone, case-insensitive, e.g.
    `xYzA:EU-FRANKFURT-1-AD-1`), else the first one in name order, so a retry picks the same one."""
    if not names:
        raise ValueError("the compartment lists no availability domain")
    names = sorted(names)
    if zone:
        for name in names:
            if name.lower().endswith(zone.lower()):
                return name
        pulumi.log.warn(f"no availability domain matches the client zone {zone!r} among {names}: using {names[0]}")
    return names[0]


def inventory() -> list[dict]:
    """The DB systems of the compartment in every region that carry the run tag, deleted ones excepted."""
    config = {key: os.environ[env] for key, env in CREDENTIAL_ENV.items()}
    compartment_id = os.environ["K8SI_OCI_COMPARTMENT_ID"]
    found = []
    for region in os.environ["K8SI_OCI_REGIONS"].split(","):
        client = oci.psql.PostgresqlClient({**config, "region": region.strip()})
        for system in oci.pagination.list_call_get_all_results_generator(
            client.list_db_systems, "record", compartment_id=compartment_id
        ):
            tags = system.freeform_tags or {}
            if TAG_RUN in tags and system.lifecycle_state != "DELETED":
                found.append(
                    {
                        "id": system.id,
                        "run": tags[TAG_RUN],
                        "expires_at": _expires_at(tags.get(TAG_EXPIRES)),
                        "created_at": system.time_created.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                    }
                )
    return found


def _expires_at(label: str | None) -> str | None:
    """RFC 3339 of the `sc_inspector_expires` tag (`20261121t0905z`)."""
    if not label:
        return None
    return datetime.strptime(label.upper(), "%Y%m%dT%H%MZ").replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
