"""OVHcloud Public Cloud Databases for PostgreSQL.

The OVH provider reads its credentials from the environment (the Secret `k8s-inspector-dbaas-ovh`: `OVH_ENDPOINT`,
`OVH_CLIENT_ID`, `OVH_CLIENT_SECRET`, `OVH_CLOUD_PROJECT_SERVICE`), and has no region setting: the region is the
database node's.

The benchmark nodes of OVH have a public address only (SCarpenter attaches nodes to `Ext-Net`, no vRack), so the
database is reached through its public endpoint, which OVH closes to everyone until an IP restriction allows a client:
`spec.client_addresses` are the only entries. Connections are TLS-only.

`spec.region` is the database's region (`GRA`); the client node is in a compute region of the same city (`GRA11`),
which the controller picks from Keeper's region data.

OVH generates the password of the admin user `avnadmin` (it cannot be chosen), so the module returns it in
`DbOutputs.password`.

The plan and the flavor are `spec.record["api_reference_object"]`. A plan with replicas (business, enterprise,
production, advanced) has `1 + max_read_replicas` nodes, which OVH requires.
"""

import json
import os
import re
import urllib.parse
import urllib.request
from datetime import UTC, datetime

import pulumi
import pulumi_ovh as ovh

from k8s_inspector_dbaas.spec import DbOutputs, DbSpec
from k8s_inspector_dbaas.storage_plan import plan

ADMIN_USER = "avnadmin"  # exists from the start; its password is reset to a generated one
ADMIN_DB = "defaultdb"
DISK_STEP_GIB = 10  # `FlexDiskSizeInvalid` otherwise
RUN_TAG = "sc_inspector_run"
EXPIRES_TAG = "sc_inspector_expires"
# endpoint -> (API base, OAuth2 token URL)
ENDPOINTS = {
    "ovh-eu": ("https://eu.api.ovh.com/1.0", "https://www.ovh.com/auth/oauth2/token"),
    "ovh-ca": ("https://ca.api.ovh.com/1.0", "https://ca.ovh.com/auth/oauth2/token"),
    "ovh-us": ("https://api.us.ovhcloud.com/1.0", "https://us.ovhcloud.com/auth/oauth2/token"),
}


def disk_gib(spec: DbSpec) -> int:
    """The planned size, raised to the flavor's included size and lowered to its maximum (Keeper's `storage_size`
    and `storage_extra_max`), in steps of 10 GiB."""
    floor = int(spec.record["storage_size"])
    ceiling = floor + int(spec.record["storage_extra_max"])
    wanted = max(plan(spec.memory_gib, spec.vcpus).storage_gib, floor)
    stepped = -(-wanted // DISK_STEP_GIB) * DISK_STEP_GIB
    return max(min(stepped, ceiling) // DISK_STEP_GIB * DISK_STEP_GIB, floor)


def description(labels: dict[str, str]) -> str:
    """OVH databases have no tags: the description carries them, for `inventory`."""
    return f"{RUN_TAG}={labels[RUN_TAG]} {EXPIRES_TAG}={labels[EXPIRES_TAG]}"


def allowed_ips(spec: DbSpec) -> list[dict[str, str]]:
    if not spec.client_addresses:
        raise ValueError("OVH denies every client until an IP restriction allows one: the spec has no client address")
    return [
        {"ip": ip if "/" in ip else f"{ip}/32", "description": f"{RUN_TAG} {spec.run_id}"[:255]}
        for ip in spec.client_addresses
    ]


def provision(spec: DbSpec) -> DbOutputs:
    restrictions = allowed_ips(spec)
    provider = ovh.Provider("ovh")
    timeouts = pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout)
    api = spec.record["api_reference_object"]
    nodes = 1 + int(spec.record.get("max_read_replicas") or 0)
    database = ovh.cloudproject.Database(
        spec.run,
        engine="postgresql",
        version=spec.engine_version,
        plan=api["plan"],
        flavor=api["flavor"],
        description=description(spec.labels),
        disk_size=disk_gib(spec),
        deletion_protection=False,
        ip_restrictions=restrictions,
        nodes=[{"region": spec.region}] * nodes,
        opts=pulumi.ResourceOptions(provider=provider, custom_timeouts=timeouts),
    )
    admin = ovh.cloudprojectdatabase.PostgresSqlUser(
        f"{spec.run}-admin",
        cluster_id=database.id,
        name=ADMIN_USER,
        password_reset=spec.run_id,
        opts=pulumi.ResourceOptions(
            provider=provider, custom_timeouts=timeouts, additional_secret_outputs=["password"]
        ),
    )
    endpoint = database.endpoints.apply(_postgres_endpoint)
    return DbOutputs(
        host=endpoint["domain"],
        port=endpoint["port"],
        admin_user=ADMIN_USER,
        admin_db=ADMIN_DB,
        password=pulumi.Output.secret(admin.password),
    )


def _postgres_endpoint(endpoints) -> dict:
    """The connection endpoint of the PostgreSQL component (the first one with a domain when none is named so)."""
    usable = [e for e in endpoints or [] if e.get("domain") and e.get("port")]
    named = [e for e in usable if e.get("component") == "postgresql"]
    if not (named or usable):
        raise ValueError("OVH reports no endpoint with a domain and a port")
    chosen = (named or usable)[0]
    return {"domain": chosen["domain"], "port": int(chosen["port"])}


def inventory() -> list[dict]:
    """The PostgreSQL databases of the project whose description carries the run tag (`description`)."""
    base, token_url = ENDPOINTS[os.environ["OVH_ENDPOINT"]]
    project = os.environ["OVH_CLOUD_PROJECT_SERVICE"]
    token = _access_token(token_url)
    listed = []
    for cluster_id in _get(f"{base}/cloud/project/{project}/database/postgresql", token):
        cluster = _get(f"{base}/cloud/project/{project}/database/postgresql/{cluster_id}", token)
        run = re.search(rf"{RUN_TAG}=(\S+)", cluster.get("description") or "")
        expires = re.search(rf"{EXPIRES_TAG}=(\S+)", cluster.get("description") or "")
        if run and expires:
            listed.append(
                {
                    "id": cluster_id,
                    "run": run.group(1),
                    "expires_at": datetime.strptime(expires.group(1), "%Y%m%dt%H%Mz")
                    .replace(tzinfo=UTC)
                    .strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "created_at": cluster["createdAt"],
                }
            )
    return listed


def _access_token(token_url: str) -> str:
    """OAuth2 client credentials of the service account."""
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": os.environ["OVH_CLIENT_ID"],
            "client_secret": os.environ["OVH_CLIENT_SECRET"],
            "scope": "all",
        }
    ).encode()
    with urllib.request.urlopen(urllib.request.Request(token_url, data=body), timeout=30) as response:  # noqa: S310
        return json.load(response)["access_token"]


def _get(url: str, token: str):
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        return json.load(response)
