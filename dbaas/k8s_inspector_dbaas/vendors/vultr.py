"""Vultr Managed Databases for PostgreSQL (package `ediri-vultr`, a community provider bridged from Vultr's Terraform
provider).

`spec.network` (ConfigMap `dbaas.vendors.vultr.networks.<region>`): `vpc_id`, the VPC of the bench nodes in the region,
and `trusted_ips`, the CIDRs allowed to connect (the VPC's range). The database joins the VPC and answers on its private
host. The plan (`spec.database_id`) fixes vCPUs, RAM and disk: there is no storage to size. The default user is
`vultradmin` and the database `defaultdb`; the provider sets the password in a second call after the database is active.

The API key is read from `VULTR_API_KEY` by the provider plugin itself. The Python SDK would copy it into the explicit
provider's arguments, and with them into the state, where it goes stale when the key is rotated, so the module hides the
variable from the SDK while it creates the provider. The plugin runs in the Pulumi CLI's environment and still sees it.
The provider takes no region: it goes on the database.

Vultr's database has one tag, so the run labels are joined into it as `key:value:key:value` (`_tag`), and `inventory`
reads them back.
"""

import contextlib
import json
import os
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime

import ediri_vultr as vultr
import pulumi

from k8s_inspector_dbaas.spec import DbOutputs, DbSpec

API = "https://api.vultr.com/v2"
API_KEY_ENV = "VULTR_API_KEY"
RUN_LABEL = "sc_inspector_run"
EXPIRES_LABEL = "sc_inspector_expires"
PAGE_SIZE = 500


@contextlib.contextmanager
def _sdk_without_api_key() -> Iterator[None]:
    key = os.environ.pop(API_KEY_ENV, None)
    try:
        yield
    finally:
        if key is not None:
            os.environ[API_KEY_ENV] = key


def _tag(labels: dict[str, str]) -> str:
    return ":".join(f"{key}:{value}" for key, value in sorted(labels.items()))


def _labels(tag: str) -> dict[str, str]:
    parts = tag.split(":")
    return dict(zip(parts[::2], parts[1::2], strict=False))


def provision(spec: DbSpec) -> DbOutputs:
    if "none" not in spec.record.get("ha", ["none"]):
        raise ValueError(f"{spec.database_id} is a high-availability plan; the benchmark needs a standalone one")
    network = spec.network
    with _sdk_without_api_key():
        provider = vultr.Provider("vultr")
    database = vultr.Database(
        spec.run,
        database_engine="pg",
        database_engine_version=spec.engine_version,
        plan=spec.database_id,
        region=spec.region,
        label=spec.run,
        tag=_tag(spec.labels),
        vpc_id=network["vpc_id"],
        trusted_ips=network["trusted_ips"],
        password=spec.password_output(),
        opts=pulumi.ResourceOptions(
            provider=provider,
            custom_timeouts=pulumi.CustomTimeouts(create=spec.create_timeout, delete=spec.delete_timeout),
        ),
    )
    return DbOutputs(
        host=database.host,
        port=database.port.apply(int),
        admin_user=database.user,
        admin_db=database.dbname,
    )


def _list_databases() -> Iterator[dict]:
    cursor = ""
    while True:
        url = f"{API}/databases?per_page={PAGE_SIZE}" + (f"&cursor={cursor}" if cursor else "")
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {os.environ[API_KEY_ENV]}"})
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            page = json.load(response)
        yield from page["databases"]
        cursor = page.get("meta", {}).get("links", {}).get("next")
        if not cursor:
            return


def _expires_at(stamp: str | None) -> str | None:
    """`20261121t0905z` (the label form of the expiry) as `2026-11-21T09:05:00Z`; None when the label is missing."""
    if not stamp:
        return None
    return datetime.strptime(stamp, "%Y%m%dt%H%Mz").replace(tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _created_at(stamp: str) -> str:
    """Vultr's `2022-05-09 10:13:31` (UTC, no zone) as `2022-05-09T10:13:31Z`."""
    return datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def inventory() -> list[dict]:
    found = []
    for database in _list_databases():
        labels = _labels(database.get("tag") or "")
        if RUN_LABEL in labels:
            found.append(
                {
                    "id": database["id"],
                    "run": labels[RUN_LABEL],
                    "expires_at": _expires_at(labels.get(EXPIRES_LABEL)),
                    "created_at": _created_at(database["date_created"]),
                }
            )
    return found
