"""Azure pre-flight of a Flexible Server create, and the ARM client the Azure vendor module shares with `inventory`.

Port of the legacy `azure_dbaas_quota.py`. Before anything is created it checks, with the Job's workload identity
(a federated token exchanged for an ARM token, no GitHub OIDC):

- the subscription may use the offer in the region (`OfferRestricted` feature) and the region lists the SKU and it is
  not disabled for the subscription (the location capabilities);
- the region offers the storage the plan needs for the SKU's compute tier;
- the subscription has vCPU headroom: the regional `cores` quota and the SKU's own quota family of the Flexible
  Server usages. The family key is the one of the vendor API (ARM Resource SKUs `family`, e.g. `standardDadv6Family`),
  not sc-data's `family` or `series` (those are catalogue classifications). The usages list only some families (none
  for v6 SKUs): a family it does not list is not an error, only the regional cores are checked.

A failed check raises `PreflightError` with a message that names what is missing; the up Job then fails before a
single resource exists. A check that cannot be made (ARM unreachable, an unexpected answer) is logged and passes (the
client node's zone is then requested as is): ARM itself refuses the create with the real reason.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from pathlib import Path

from k8s_inspector_dbaas.errors import RegionUnavailable, VendorRegionUnavailable

ARM = "https://management.azure.com"
API_VERSION = "2025-08-01"  # Microsoft.DBforPostgreSQL; the version of the pulumi-azure-native Server resource
COMPUTE_API_VERSION = "2021-07-01"  # Microsoft.Compute/skus
LOGIN = "https://login.microsoftonline.com"
ASSERTION_TYPE = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

log = logging.getLogger(__name__)


class PreflightError(RuntimeError):
    """The subscription cannot create this database in this region now."""


class RegionPreflightError(PreflightError, RegionUnavailable):
    """... and never will: the offer is restricted, the SKU or storage is not there, or the quota limit is too low. A
    shortfall of the quota that running servers use up is a plain `PreflightError`: it may be gone later."""


class OfferRestrictedError(RegionPreflightError, VendorRegionUnavailable):
    """The subscription is restricted for the offer in the region: no database of it can be created there."""


def subscription_id() -> str:
    return os.environ["ARM_SUBSCRIPTION_ID"]


def token() -> str:
    """An ARM token for the workload identity: the projected Kubernetes token (`ARM_OIDC_TOKEN_FILE_PATH`) is the client
    assertion of the managed identity named by `ARM_CLIENT_ID` and `ARM_TENANT_ID`, the variables the Pulumi provider
    reads."""
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": os.environ["ARM_CLIENT_ID"],
            "scope": f"{ARM}/.default",
            "client_assertion_type": ASSERTION_TYPE,
            "client_assertion": Path(os.environ["ARM_OIDC_TOKEN_FILE_PATH"]).read_text().strip(),
        }
    )
    request = urllib.request.Request(f"{LOGIN}/{os.environ['ARM_TENANT_ID']}/oauth2/v2.0/token", data=body.encode())
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 (a fixed https URL)
        return json.load(response)["access_token"]


def arm_list(token: str, path: str, api_version: str, **params: str) -> Iterator[dict]:
    """The `value` items of a (paged) ARM list call under `path`, with an ARM token from `token()`."""
    url: str | None = f"{ARM}{path}?{urllib.parse.urlencode({'api-version': api_version, **params})}"
    while url:
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310 (ARM, or its own nextLink)
            page = json.load(response)
        yield from page.get("value", [])
        url = page.get("nextLink")


def _sku_family(token: str, region: str, sku: str) -> str | None:
    """The quota family of a compute size from the ARM Resource SKUs of the region (the listing ends at the SKU)."""
    path = f"/subscriptions/{subscription_id()}/providers/Microsoft.Compute/skus"
    for item in arm_list(token, path, COMPUTE_API_VERSION, **{"$filter": f"location eq '{region}'"}):
        if item.get("name") == sku and item.get("resourceType") == "virtualMachines":
            return item.get("family")
    return None


def sku_capability(capabilities: list[dict], sku: str) -> tuple[dict, dict] | None:
    """`(server edition, SKU)` of the capabilities that list `sku`, or None."""
    for capability in capabilities:
        for edition in capability.get("supportedServerEditions") or []:
            for listed in edition.get("supportedServerSkus") or []:
                if listed.get("name") == sku:
                    return edition, listed
    return None


def offer_restricted(capabilities: list[dict]) -> bool:
    for capability in capabilities:
        for feature in capability.get("supportedFeatures") or []:
            if feature.get("name") == "OfferRestricted" and feature.get("status") == "Enabled":
                return True
        if "restricted" in (capability.get("reason") or "").lower():
            return True
    return False


def headroom(usages: dict[str, dict], family: str | None, vcpus: int) -> tuple[str, bool] | None:
    """Why the quota does not fit `vcpus` more vCPUs, and whether its limit is below `vcpus` (so it never will: not
    just used up by running servers); None: it fits. `usages` maps lowercase quota name -> item."""

    def left(item: dict) -> str:
        return f"{item['limit'] - item['currentValue']} of {item['limit']} left"

    cores = usages.get("cores")
    if cores is None:
        log.warning("the regional cores quota is not listed, the vCPU headroom is not checked")
        return None
    if cores["limit"] - cores["currentValue"] < vcpus:
        return f"{vcpus} vCPUs needed, the regional cores quota has {left(cores)}", cores["limit"] < vcpus
    if family is None:
        log.info("no quota family known for the SKU, only the regional cores are checked")
    elif family.lower() not in usages:
        log.info(
            "the usages do not list the quota family %s (normal for some), only the regional cores are checked", family
        )
    elif (item := usages[family.lower()])["limit"] - item["currentValue"] < vcpus:
        return f"{vcpus} vCPUs needed, quota {family} has {left(item)}", item["limit"] < vcpus
    return None


def check(region: str, sku: str, vcpus: int, storage_edition: str) -> list[str] | None:
    """Raise `PreflightError` when the database cannot be created; else return the availability zones of the SKU
    (the zone of the client node is used only when it is one of them), or None when the check could not be made."""
    try:
        return _check(region, sku, vcpus, storage_edition)
    except PreflightError:
        raise
    except Exception as e:  # the check could not be made: ARM refuses a create it cannot do, with its own reason
        log.warning("azure pre-flight for %s in %s skipped: %s", sku, region, e)
        return None


def _check(region: str, sku: str, vcpus: int, storage_edition: str) -> list[str]:
    base = f"/subscriptions/{subscription_id()}/providers/Microsoft.DBforPostgreSQL/locations/{region}"
    arm_token = token()
    capabilities = list(arm_list(arm_token, f"{base}/capabilities", API_VERSION))
    usages = {
        i["name"]["value"].lower(): i
        for i in arm_list(arm_token, f"{base}/resourceType/flexibleServers/usages", API_VERSION)
    }
    family = _sku_family(arm_token, region, sku)
    if offer_restricted(capabilities):
        raise OfferRestrictedError(
            f"azure pre-flight: PostgreSQL Flexible Server is restricted for this subscription in {region}"
        )
    found = sku_capability(capabilities, sku)
    if found is None:
        raise RegionPreflightError(f"azure pre-flight: {region} does not offer the SKU {sku}")
    edition, listed = found
    if listed.get("status") == "Disabled":
        raise RegionPreflightError(
            f"azure pre-flight: {sku} is disabled in {region}: {listed.get('reason') or 'no reason given'}"
        )
    storages = {s.get("name") for s in edition.get("supportedStorageEditions") or []}
    if storages and storage_edition not in storages:
        raise RegionPreflightError(
            f"azure pre-flight: {region} offers {sku} with {sorted(storages)}, not {storage_edition}"
        )
    if shortage := headroom(usages, family, vcpus):
        reason, never = shortage
        raise (RegionPreflightError if never else PreflightError)(f"azure pre-flight: {sku} in {region}: {reason}")
    return list(listed.get("supportedZones") or [])
