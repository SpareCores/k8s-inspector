"""The `gcp` vendor: Cloud SQL for PostgreSQL, one standalone zonal instance with a private IP only.

`spec.database_id` is the Cloud SQL tier (`db-perf-optimized-N-8`, `db-c4a-highmem-4`, `db-n1-standard-2`). The
platform's `spec.network` (ConfigMap `dbaas.vendors.gcp.networks.<region>`) names the bench VPC and the allocated range
of its private services access (the peering with Google's service network, made once per VPC):

    network: projects/<project>/global/networks/<vpc>
    allocated_range: <name of the allocated IP range>

Credentials and the project come from the environment, never from provider arguments: `GOOGLE_CREDENTIALS` (the
workload identity federation configuration of the vendor's service account, no secret in it) and `GOOGLE_PROJECT`.

`cleanup(run)` deletes the run's instances through the Cloud SQL Admin API after the Pulumi destroy: the provider
drops an instance from the state when its create fails or times out, so the state alone cannot find it. `down` knows
the BenchmarkRun name only, so the instance carries it as the label `sc_inspector_run_name` (the name is a DNS label
of at most 63 characters, valid as a label value).

The instance's `postgres` user has the run's password. It cannot create a database that it can then use, so the
workload user `scadmin` is created by the bootstrap with the role `cloudsqlsuperuser` (`DbOutputs.user`, `grant_role`).
"""

import json
import os
import time
from datetime import UTC, datetime

import google.auth
import pulumi
import pulumi_gcp as gcp
from google.auth.transport.requests import AuthorizedSession

from k8s_inspector_dbaas.spec import DbOutputs, DbSpec
from k8s_inspector_dbaas.storage_plan import DiskProfile, plan

RUN_LABEL = "sc_inspector_run"  # the run id
RUN_NAME_LABEL = "sc_inspector_run_name"  # the BenchmarkRun name, which `down` and the sweeper have
EXPIRES_LABEL = "sc_inspector_expires"
EXPIRES_FORMAT = "%Y%m%dt%H%Mz"  # the controller's label-safe timestamp
SQLADMIN = "https://sqladmin.googleapis.com/v1"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
MAX_NAME_LENGTH = 98  # Cloud SQL instance names
# `down` runs after a successful destroy, which may have used its whole delete timeout of the Job's 2 h deadline
# (`DOWN_DEADLINE_SECONDS` less the margin): a few minutes of waiting fit in the margin. A deletion that takes longer
# fails the Job; the next `down` finds the instance in PENDING_DELETE and waits again.
CLEANUP_WAIT_SECONDS = 300
CLEANUP_POLL_SECONDS = 15

ADMIN_USER = "postgres"
WORKLOAD_USER = "scadmin"
GRANT_ROLE = "cloudsqlsuperuser"
ENTERPRISE_PLUS_PREFIXES = ("db-perf-optimized-", "db-memory-optimized-", "db-c4a-")

PD_SSD = "PD_SSD"
HYPERDISK_BALANCED = "HYPERDISK_BALANCED"
# https://cloud.google.com/compute/docs/disks/performance (pd-ssd): IOPS and throughput come with the size, and the VM
# caps them by its vCPU count. Hyperdisk Balanced is left at its default IOPS and throughput, which exceed the plan.
PD_SSD_PROFILE = DiskProfile(
    min_size_gib=10,
    max_write_iops=30_000,
    max_write_mbps=400,
    independently_provisioned=False,
    write_iops_per_gib=30,
    write_mbps_per_gib=0.48,
    write_iops_per_vcpu=800,
    write_mbps_per_vcpu=6,
)


def instance_name(run_id: str) -> str:
    """`sc-<run id>`. The run id is unique per run and the same in all its attempts, unlike the BenchmarkRun name, which
    repeats for a client SKU; Cloud SQL keeps a deleted instance's name unavailable for up to a week."""
    name = f"sc-{run_id.lower()}"
    if len(name) > MAX_NAME_LENGTH:
        raise ValueError(f"run id {run_id!r} is too long for a Cloud SQL instance name ({MAX_NAME_LENGTH} characters)")
    return name


def edition(tier: str) -> str:
    """Cloud SQL naming: performance and memory optimized tiers and C4A are Enterprise Plus, the rest Enterprise."""
    return "ENTERPRISE_PLUS" if tier.startswith(ENTERPRISE_PLUS_PREFIXES) else "ENTERPRISE"


def disk_type(record: dict) -> str:
    """Hyperdisk Balanced when Keeper lists it as the tier's only storage (C4 and C4A), else Persistent Disk SSD."""
    storage = record.get("compatible_storage_ids") or []
    return HYPERDISK_BALANCED if storage == ["cloudsql-hyperdisk"] else PD_SSD


def provision(spec: DbSpec) -> DbOutputs:
    tier = spec.database_id
    kind = disk_type(spec.record)
    storage = plan(spec.memory_gib, spec.vcpus, PD_SSD_PROFILE if kind == PD_SSD else None)
    name = instance_name(spec.run_id)

    # the project and the credentials are the provider's own environment (GOOGLE_PROJECT, GOOGLE_CREDENTIALS)
    provider = gcp.Provider("gcp", region=spec.region)
    zone = spec.zone
    settings = gcp.sql.DatabaseInstanceSettingsArgs(
        tier=tier,
        edition=edition(tier),
        availability_type="ZONAL",
        location_preference=gcp.sql.DatabaseInstanceSettingsLocationPreferenceArgs(zone=zone) if zone else None,
        disk_size=storage.storage_gib,
        disk_type=kind,
        disk_autoresize=False,
        deletion_protection_enabled=False,
        backup_configuration=gcp.sql.DatabaseInstanceSettingsBackupConfigurationArgs(enabled=False),
        user_labels={**spec.labels, RUN_NAME_LABEL: spec.run},
        ip_configuration=gcp.sql.DatabaseInstanceSettingsIpConfigurationArgs(
            ipv4_enabled=False,
            private_network=spec.network["network"],
            allocated_ip_range=spec.network["allocated_range"],
            ssl_mode="ALLOW_UNENCRYPTED_AND_ENCRYPTED",  # the default: TLS is offered, and the client asks for it
        ),
        # Enterprise Plus turns the data cache on by default: off, so the scores compare with the other vendors
        data_cache_config=gcp.sql.DatabaseInstanceSettingsDataCacheConfigArgs(data_cache_enabled=False)
        if edition(tier) == "ENTERPRISE_PLUS"
        else None,
    )
    instance = gcp.sql.DatabaseInstance(
        "db",
        name=name,
        database_version=f"POSTGRES_{spec.engine_version.split('.')[0]}",
        region=spec.region,
        deletion_protection=False,
        root_password=spec.password_output(),
        settings=settings,
        opts=pulumi.ResourceOptions(
            provider=provider,
            custom_timeouts=pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout),
        ),
    )
    return DbOutputs(
        host=instance.private_ip_address,
        admin_user=ADMIN_USER,
        admin_db="postgres",
        sslmode="require",
        user=WORKLOAD_USER,
        grant_role=GRANT_ROLE,
    )


def _session() -> tuple[AuthorizedSession, str]:
    """An authorized session and the instances URL of the project, from the environment."""
    credentials, _ = google.auth.load_credentials_from_dict(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return AuthorizedSession(credentials), f"{SQLADMIN}/projects/{os.environ['GOOGLE_PROJECT']}/instances"


def _instances(session: AuthorizedSession, url: str) -> list[dict]:
    """Every Cloud SQL instance of the project. The list filter of the API cannot test that a label exists, so callers
    check the labels."""
    found, token = [], None
    while True:
        response = session.get(url, params={"pageToken": token} if token else None, timeout=60)
        response.raise_for_status()
        body = response.json()
        found += body.get("items", [])
        token = body.get("nextPageToken")
        if not token:
            return found


def cleanup(run: str) -> None:
    """Delete the run's instances (those labelled with the BenchmarkRun name) after the Pulumi destroy, and wait until
    they are gone. The provider forgets an instance whose create timed out or failed (it clears the resource id), so the
    state may not record an instance that bills. An instance that is already being deleted is only waited for, and one
    that vanishes between the list and the delete (404) is gone all the same."""
    session, url = _session()
    deadline = time.monotonic() + CLEANUP_WAIT_SECONDS
    while True:
        left = [item for item in _instances(session, url) if _labels(item).get(RUN_NAME_LABEL) == run]
        if not left:
            return
        for item in left:
            if item.get("state") != "PENDING_DELETE":
                response = session.delete(f"{url}/{item['name']}", timeout=60)
                if response.status_code != 404:
                    response.raise_for_status()
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"{', '.join(item['name'] for item in left)} still exist(s) after {CLEANUP_WAIT_SECONDS} s"
            )
        time.sleep(CLEANUP_POLL_SECONDS)


def inventory() -> list[dict]:
    """The project's Cloud SQL instances that carry the run label."""
    session, url = _session()
    return [_tagged(item) for item in _instances(session, url) if RUN_LABEL in _labels(item)]


def _labels(item: dict) -> dict:
    return item.get("settings", {}).get("userLabels") or {}


def _tagged(item: dict) -> dict:
    labels = _labels(item)
    stamp = labels.get(EXPIRES_LABEL)  # a missing expiry is unknown, and the instance is reported all the same
    expires = datetime.strptime(stamp, EXPIRES_FORMAT).replace(tzinfo=UTC) if stamp else None
    return {
        "id": item["name"],
        "run": labels[RUN_LABEL],
        "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ") if expires else None,
        "created_at": item["createTime"],  # RFC 3339 with fractional seconds
    }
