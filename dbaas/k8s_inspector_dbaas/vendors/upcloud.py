"""UpCloud Managed Databases for PostgreSQL.

`spec.network` (ConfigMap `dbaas.vendors.upcloud.networks.<zone>`): `network_id`, the UUID of the SDN private network
the bench nodes of the zone have an interface on, and `cidr`, its range (the IP filter: only the client nodes'
network may connect; there is no public access). UpCloud's regions are zones, so `spec.region` is the zone of both
the database and the network. The provider has no region argument and reads its token from `UPCLOUD_TOKEN`; it is
never passed as an argument, so it stays out of the state.

Plans are Keeper's `api_reference`. Flexible plans (`rdb.*`) take compute, storage and backup tier separately: the
storage bands, their step and the allowed backup tiers are in `spec.record["api_reference_object"]` (Keeper's record).
Legacy bundled plans take only the plan name. All databases are standalone (one node).
"""

import hashlib
import json
import os
import urllib.request
from datetime import datetime

import pulumi
import pulumi_upcloud as upcloud

from k8s_inspector_dbaas import storage_plan
from k8s_inspector_dbaas.spec import DbOutputs, DbSpec

API = "https://api.upcloud.com/1.3"
ADMIN_USER = "scadmin"
ADMIN_DB = "defaultdb"  # the database UpCloud creates with the service
BACKUP_TIERS = ("mini", "regular", "extended")  # cheapest first
RUN_LABEL = "sc_inspector_run"
EXPIRES_LABEL = "sc_inspector_expires"


def service_name(run: str) -> str:
    """The hostname prefix (3 to 30 characters, unique in the account): a hash of the run, so a retry finds it."""
    return "scdb-" + hashlib.sha256(run.encode()).hexdigest()[:16]


def rfc3339(stamp: str) -> str:
    """The expiry label (`20261120t1600z`) as an RFC 3339 time; empty when the label is missing or malformed."""
    try:
        return datetime.strptime(stamp, "%Y%m%dt%H%Mz").strftime("%Y-%m-%dT%H:%M:00Z")
    except ValueError:
        return ""


def backup_tier(allowed: list[str]) -> str:
    """The cheapest backup tier the plan allows."""
    for tier in BACKUP_TIERS:
        if tier in allowed:
            return tier
    raise ValueError(f"the plan allows no known backup tier: {allowed}")


def snap_storage_gib(gib: int, step: int, bands: list[tuple[int, int]]) -> int:
    """Round up to the step and into a storage band; between two bands: the start of the next, above all: the top."""
    gib = -(-max(step, gib) // step) * step
    for low, high in bands:
        if low <= gib <= high:
            return gib
    for low, _ in bands:
        if gib < low:
            return low
    return bands[-1][1]


def flexible_plan_args(spec: DbSpec) -> dict:
    """compute, node count, storage and backup tier of an `rdb.*` plan, from Keeper's plan data."""
    plan = spec.record["api_reference_object"]
    storage = plan.get("plan_storage") or {}
    bands = [(int(o["base_gib"]), int(o["max_gib"])) for o in storage.get("options") or []]
    if not bands:
        raise ValueError(f"Keeper has no plan_storage bands for {spec.database_id}")
    wanted = storage_plan.plan(spec.memory_gib, spec.vcpus).storage_gib
    wanted = max(min(wanted, bands[-1][1]), bands[0][0])
    return {
        "plan_compute": spec.database_id,
        "plan_node_count": 1,
        "plan_storage_gib": snap_storage_gib(wanted, int(storage.get("step_gib") or 10), bands),
        "plan_backups": backup_tier(plan.get("plan_backups") or []),
    }


def plan_args(spec: DbSpec) -> dict:
    if spec.database_id.startswith("rdb."):
        return flexible_plan_args(spec)
    return {"plan": spec.database_id}  # a legacy plan includes its storage


def _connection(components: list, networks: list) -> tuple[str, int]:
    """Host and port of the service's `pg` endpoint that the SDN network reaches.

    UpCloud lists each component per route: `dynamic` (the service hostname, which resolves to the private address
    on an attached SDN network), `public` (the `public-` hostname) and, where it exists, `private`. A `private` route
    wins; otherwise `dynamic` is the private endpoint. `public` is never used: the filters keep it closed. A service
    without an attached network is an error, as its `dynamic` host would be the public address."""
    if not networks:
        raise RuntimeError("the UpCloud database is not attached to the SDN network")
    pg = {c["route"]: c for c in components or [] if c["component"] == "pg"}
    for route in ("private", "dynamic"):
        if route in pg:
            return pg[route]["host"], int(pg[route]["port"])
    raise RuntimeError(f"the UpCloud database has no private or dynamic pg component: {sorted(pg)}")


def provision(spec: DbSpec) -> DbOutputs:
    provider = upcloud.Provider("upcloud")
    timeouts = pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout)
    db = upcloud.ManagedDatabasePostgresql(
        "db",
        name=service_name(spec.run),
        title=spec.run,
        zone=spec.region,
        labels=spec.labels,
        termination_protection=False,
        networks=[
            upcloud.ManagedDatabasePostgresqlNetworkArgs(
                family="IPv4", name="bench", type="private", uuid=spec.network["network_id"]
            )
        ],
        properties=upcloud.ManagedDatabasePostgresqlPropertiesArgs(
            version=spec.engine_version,
            admin_username=ADMIN_USER,
            admin_password=spec.password_output(),
            public_access=False,
            ip_filters=[spec.network["cidr"]],
            automatic_utility_network_ip_filter=False,
        ),
        opts=pulumi.ResourceOptions(provider=provider, custom_timeouts=timeouts),
        **plan_args(spec),
    )
    connection = pulumi.Output.all(db.components, db.networks).apply(lambda v: _connection(*v))
    return DbOutputs(
        host=connection.apply(lambda c: c[0]),
        port=connection.apply(lambda c: c[1]),
        admin_user=ADMIN_USER,
        admin_db=ADMIN_DB,
    )


def label_dict(labels: list | dict | None) -> dict[str, str]:
    """The API's labels as a dict; they come as a list of `{key, value}` (the documented-by-use format) or as a dict."""
    if isinstance(labels, dict):
        return labels
    return {label["key"]: label["value"] for label in labels or []}


def inventory() -> list[dict]:
    """The PostgreSQL services that carry the run label (`UPCLOUD_TOKEN`)."""
    found, offset = [], 0
    while True:
        headers = {"Authorization": f"Bearer {os.environ['UPCLOUD_TOKEN']}"}
        request = urllib.request.Request(f"{API}/database?limit=100&offset={offset}", headers=headers)
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 (a fixed https URL)
            page = json.load(response)
        for service in page:
            labels = label_dict(service.get("labels"))
            if service.get("type") == "pg" and RUN_LABEL in labels:
                found.append(
                    {
                        "id": service["uuid"],
                        "run": labels[RUN_LABEL],
                        "expires_at": rfc3339(labels.get(EXPIRES_LABEL, "")),
                        "created_at": service["create_time"],
                    }
                )
        if len(page) < 100:
            return found
        offset += 100
