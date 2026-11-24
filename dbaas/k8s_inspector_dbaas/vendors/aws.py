"""The `aws` vendor: Amazon RDS for PostgreSQL, one single-AZ instance on gp3 storage.

`spec.network` (from the platform, per region): `subnet_group` (a DB subnet group over the bench VPC's subnets) and
`security_group_id` (allows 5432 from the bench nodes). The instance is private (no public access) and has no final
snapshot, no automated backups and no deletion protection, so a destroy leaves nothing behind.

Credentials: the AWS provider and boto3 read the environment of the Job. The platform's Secret
`k8s-inspector-dbaas-aws` holds `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` (the vendor ServiceAccount's token);
the region is the provider's `region`.
"""

import hashlib
import logging
import re
from datetime import UTC, datetime

import boto3
import pulumi
import pulumi_aws as aws

from k8s_inspector_dbaas.spec import DbOutputs, DbSpec
from k8s_inspector_dbaas.storage_plan import DiskProfile, plan

log = logging.getLogger(__name__)

ADMIN_USER = "scadmin"
ADMIN_DB = "postgres"
RUN_TAG = "sc_inspector_run"
EXPIRES_TAG = "sc_inspector_expires"
EXPIRES_FORMAT = "%Y%m%dt%H%Mz"  # the controller's label-safe stamp of `sc_inspector_expires`

# RDS for PostgreSQL gp3 (https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Storage.html): the IOPS and
# throughput of a volume below 400 GiB are fixed at 3000 and 125 MiB/s and cannot be provisioned; from 400 GiB the
# baseline is 12000 and 500 MiB/s, and more can be provisioned up to 64000 and 4000.
SMALL_VOLUME_GIB = 400
SMALL_BASELINE_IOPS, SMALL_BASELINE_MBPS = 3000, 125
LARGE_BASELINE_IOPS, LARGE_BASELINE_MBPS = 12000, 500
GP3 = DiskProfile(min_size_gib=20, max_write_iops=64000, max_write_mbps=4000, independently_provisioned=True)


def rds_storage(spec: DbSpec) -> tuple[int, int | None, int | None]:
    """`(allocated GiB, provisioned IOPS, provisioned MiB/s)` for the database; IOPS and throughput are None when the
    volume's baseline already gives the planned I/O budget. A budget above the 3000 IOPS / 125 MiB/s of a small volume
    makes the volume 400 GiB, where the baseline is 4 times higher."""
    storage = plan(spec.memory_gib, spec.vcpus, GP3)
    iops, mbps = storage.iops or 0, storage.throughput_mb_s or 0
    size = storage.storage_gib
    if iops <= SMALL_BASELINE_IOPS and mbps <= SMALL_BASELINE_MBPS:
        return size, None, None
    size = max(size, SMALL_VOLUME_GIB)
    if iops <= LARGE_BASELINE_IOPS and mbps <= LARGE_BASELINE_MBPS:
        return size, None, None
    return size, max(iops, LARGE_BASELINE_IOPS), max(mbps, LARGE_BASELINE_MBPS)


def identifier(run_id: str) -> str:
    """The DB instance identifier of the run: 1-63 lowercase letters, digits and single hyphens, starting with a
    letter. It derives from `spec.run_id` (unique per run), not from the BenchmarkRun name, which repeats for a client
    SKU: a run that starts while an earlier delete is still running cannot clash. A name that does not fit keeps its
    head and gets a hash."""
    name = re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", f"sc-{run_id}".lower())).strip("-")
    if len(name) > 63:
        name = f"{name[:54].rstrip('-')}-{hashlib.sha256(run_id.encode()).hexdigest()[:8]}"
    return name


def provision(spec: DbSpec) -> DbOutputs:
    provider = aws.Provider("aws", region=spec.region)
    name = identifier(spec.run_id)
    gib, iops, mbps = rds_storage(spec)
    timeouts = pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout)
    # next to the client when the zone is a zone name of the region (the subnet group has a subnet in every zone)
    zone = spec.zone if spec.zone and spec.zone.startswith(spec.region) else None
    instance = aws.rds.Instance(
        "db",
        identifier=name,
        engine="postgres",
        engine_version=spec.engine_version,
        instance_class=spec.database_id,
        allocated_storage=gib,
        storage_type="gp3",
        iops=iops,
        storage_throughput=mbps,
        storage_encrypted=True,
        username=ADMIN_USER,
        password=spec.password_output(),
        port=5432,
        db_subnet_group_name=spec.network["subnet_group"],
        vpc_security_group_ids=[spec.network["security_group_id"]],
        availability_zone=zone,
        publicly_accessible=False,
        multi_az=False,
        backup_retention_period=0,
        skip_final_snapshot=True,
        delete_automated_backups=True,
        deletion_protection=False,
        auto_minor_version_upgrade=True,
        apply_immediately=True,
        tags={**spec.labels, "Name": name, "ManagedBy": "k8s-inspector"},
        opts=pulumi.ResourceOptions(provider=provider, custom_timeouts=timeouts),
    )
    return DbOutputs(host=instance.address, port=instance.port, admin_user=ADMIN_USER, admin_db=ADMIN_DB)


def inventory() -> list[dict]:
    """The tagged instances of every enabled region. The credentials are the Job's environment. A region that cannot
    be listed, or an unreadable expiry tag, fails the whole call after logging which, so the staleness alert fires and
    a partial list does not hide leaks."""
    found = []
    session = boto3.session.Session(region_name="us-east-1")
    for region in session.client("ec2").describe_regions()["Regions"]:
        name = region["RegionName"]
        try:
            found += _region_instances(session, name)
        except Exception:
            log.exception("listing RDS instances failed in region %s", name)
            raise
    return found


def _region_instances(session: boto3.session.Session, region: str) -> list[dict]:
    found = []
    rds = session.client("rds", region_name=region)
    for page in rds.get_paginator("describe_db_instances").paginate():
        for db in page["DBInstances"]:
            tags = {tag["Key"]: tag["Value"] for tag in db.get("TagList", [])}
            if RUN_TAG in tags:
                found.append(
                    {
                        "id": db["DBInstanceArn"],
                        "run": tags[RUN_TAG],
                        "expires_at": _expires_at(tags.get(EXPIRES_TAG), db["DBInstanceArn"]),
                        "created_at": _rfc3339(db.get("InstanceCreateTime")),
                    }
                )
    return found


def _expires_at(stamp: str | None, database: str) -> str | None:
    if not stamp:
        return None
    try:
        return _rfc3339(datetime.strptime(stamp, EXPIRES_FORMAT).replace(tzinfo=UTC))
    except ValueError as err:
        raise ValueError(f"{database}: tag {EXPIRES_TAG}={stamp!r} is not a {EXPIRES_FORMAT} stamp") from err


def _rfc3339(when: datetime | None) -> str | None:
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if when else None
