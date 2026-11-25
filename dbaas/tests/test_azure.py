import asyncio
import json

import pulumi
import pytest

from k8s_inspector_dbaas import azure_preflight
from k8s_inspector_dbaas.azure_preflight import PreflightError, RegionPreflightError
from k8s_inspector_dbaas.errors import RegionUnavailable
from k8s_inspector_dbaas.spec import DbSpec
from k8s_inspector_dbaas.vendors import azure

GROUP = "/subscriptions/s/resourceGroups/sc-azure-main-westeurope"
NETWORK = {
    "resource_group": "sc-azure-main-westeurope",
    "subnet_id": f"{GROUP}/providers/Microsoft.Network/virtualNetworks/v/subnets/dbaas",
    "private_dns_zone_id": f"{GROUP}/providers/Microsoft.Network/privateDnsZones/z",
}
RECORD = {
    "vcpus": 4, "memory_amount": 16384, "storage_extra_min": 34, "storage_extra_max": 70369,
    "compatible_storage_ids": ["ManagedDisk", "ManagedDiskV2", "UltraDisk"],
    "api_reference_object": {"sku_name": "GP_Standard_D4ds_v5"},
}  # fmt: skip


def make_spec(**changes) -> DbSpec:
    fields = dict(
        vendor="azure", database_id="Standard_D4ds_v5", engine_version="18", region="westeurope", zone="westeurope-2",
        client_region="westeurope",
        run="azure-standard-d2ds-v5-1234abcd", run_id="r1", expires_at="2026-11-20T16:00:00Z",
        labels={"sc_inspector_run": "r1", "sc_inspector_expires": "20261120t1600z"},
        create_timeout_minutes=50, delete_timeout_minutes=100, network=NETWORK, record=RECORD, password="s3cret-pass",
    )  # fmt: skip
    return DbSpec(**{**fields, **changes})


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        self.resources = []

    def new_resource(self, args):
        self.resources.append(args)
        outputs = dict(args.inputs)
        if "serverName" in outputs:
            outputs["fullyQualifiedDomainName"] = f"{outputs['serverName']}.privatelink.postgres.database.azure.com"
        return args.name + "-id", outputs

    def call(self, args):
        return {}


@pytest.fixture
def mocks():
    asyncio.set_event_loop(asyncio.new_event_loop())  # Python 3.15 has none by default; Pulumi's SDK wants one
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="k8s-inspector-dbaas", stack="test", preview=False)
    return mocks


@pytest.fixture
def checked(monkeypatch):
    """The pre-flight passes and the SKU offers the zones 1 and 2; its calls are recorded."""
    calls = []
    monkeypatch.setattr(azure_preflight, "check", lambda *args: calls.append(args) or ["1", "2"])
    return calls


def registered(mocks, spec):
    """Run `provision` and wait until the mock has seen the resources: `(outputs, provider, server)`, as an Output."""
    out = azure.provision(spec)

    def find(_):
        server = next(r for r in mocks.resources if r.typ.startswith("azure-native:dbforpostgresql"))
        provider = next(r for r in mocks.resources if r.typ == "pulumi:providers:azure-native")
        return out, provider, server

    return out.host.apply(find)


@pulumi.runtime.test
def test_the_server_joins_the_delegated_subnet_without_public_access(mocks, checked):
    spec = make_spec()

    def check(found):
        out, _, server = found
        inputs = server.inputs
        assert inputs["resourceGroupName"] == "sc-azure-main-westeurope" and inputs["location"] == "westeurope"
        assert inputs["serverName"] == "sc-r1" and inputs["version"] == "18"
        assert inputs["sku"] == {"name": "Standard_D4ds_v5", "tier": "GeneralPurpose"}
        assert inputs["network"] == {
            "delegatedSubnetResourceId": NETWORK["subnet_id"],
            "privateDnsZoneArmResourceId": NETWORK["private_dns_zone_id"],
        }
        assert inputs["availabilityZone"] == "2" and inputs["administratorLogin"] == "scadmin"
        assert inputs["tags"] == spec.labels and "highAvailability" not in inputs
        assert checked == [("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2")]
        assert (out.admin_user, out.admin_db, out.port, out.sslmode, out.user) == (
            "scadmin",
            "postgres",
            5432,
            "require",
            None,
        )

    return registered(mocks, spec).apply(check)


@pulumi.runtime.test
def test_credentials_are_no_provider_arguments_and_the_password_is_secret(mocks, checked):
    def check(found):
        _, provider, server = found
        assert set(provider.inputs) <= {
            "environment"
        }  # the ARM_* environment holds the credentials: arguments go to the state
        password = server.inputs["administratorLoginPassword"]
        assert (
            password["value"] == "s3cret-pass" and "4dabf18193072939515e22adb298388d" in password
        )  # Pulumi's secret marker

    return registered(mocks, make_spec()).apply(check)


@pulumi.runtime.test
def test_the_slow_resource_has_the_custom_timeouts_of_the_spec(mocks, checked, monkeypatch):
    options = []
    server = azure.dbforpostgresql.Server
    monkeypatch.setattr(
        azure.dbforpostgresql,
        "Server",
        lambda *args, opts, **kwargs: options.append(opts) or server(*args, opts=opts, **kwargs),
    )

    def check(found):
        assert options[0].custom_timeouts.create == "50m" and options[0].custom_timeouts.delete == "100m"

    return registered(mocks, make_spec()).apply(check)


@pulumi.runtime.test
def test_a_client_zone_the_sku_does_not_offer_is_not_requested(mocks, checked):
    def check(found):
        assert not found[2].inputs.get("availabilityZone")

    return registered(mocks, make_spec(zone="westeurope-3")).apply(check)


def test_a_pre_flight_refusal_is_marked_as_region_unavailable_for_the_controller():
    error = RegionPreflightError("azure pre-flight: restricted in eastus")
    assert isinstance(error, RegionUnavailable)
    assert str(error) == "DBAAS_REGION_UNAVAILABLE: azure pre-flight: restricted in eastus"


def test_a_failed_pre_flight_creates_nothing(mocks, monkeypatch):
    def refuse(*args):
        raise PreflightError("azure pre-flight: no quota")

    monkeypatch.setattr(azure_preflight, "check", refuse)
    with pytest.raises(PreflightError, match="no quota"):
        azure.provision(make_spec())
    assert mocks.resources == []


def test_the_tier_comes_from_the_prefix_of_keepers_sku_name():
    for prefix, tier in (("B", "Burstable"), ("GP", "GeneralPurpose"), ("MO", "MemoryOptimized")):
        record = {**RECORD, "api_reference_object": {"sku_name": f"{prefix}_Standard_X"}}
        assert azure.sku_of(make_spec(record=record))[1] == tier


def test_premium_v2_gets_the_p30_equivalent_at_least_and_the_plan_above_it():
    storage = azure.storage_for(make_spec())
    assert (storage.type, storage.storage_size_gb, storage.iops, storage.throughput) == ("PremiumV2_LRS", 64, 5000, 200)
    big = azure.storage_for(make_spec(record={**RECORD, "vcpus": 128, "memory_amount": 1024 * 1024}))
    assert big.iops == 5000 and big.throughput == 200 and big.storage_size_gb == 151  # 64 GiB schema x 2, over 0.85


def test_a_sku_without_premium_v2_gets_premium_ssd_in_a_valid_size_and_tier():
    record = {**RECORD, "compatible_storage_ids": ["ManagedDisk"], "memory_amount": 4096}
    storage = azure.storage_for(make_spec(record=record))
    assert (storage.type, storage.storage_size_gb, storage.tier, storage.iops) == ("Premium_LRS", 64, "P30", None)
    big = azure.storage_for(make_spec(record={**record, "memory_amount": 1024 * 1024}))
    assert big.storage_size_gb == 256  # 151 snapped up to a Premium SSD size


def test_a_disk_above_the_skus_maximum_is_an_error():
    with pytest.raises(ValueError, match="maximum"):
        azure.storage_for(make_spec(record={**RECORD, "storage_extra_max": 10}))


@pulumi.runtime.test
def test_the_server_is_named_after_the_run_id(mocks, checked):
    def check(found):
        assert found[2].inputs["serverName"] == "sc-r1"

    return registered(mocks, make_spec()).apply(check)


def test_server_names_are_dns_labels_and_unique_when_cut():
    assert azure.server_name("Azure_X.y") == "sc-azure-x-y"
    a, b = azure.server_name("azure-" + "x" * 80 + "-1"), azure.server_name("azure-" + "x" * 80 + "-2")
    assert len(a) <= 63 and len(b) <= 63 and a != b and not a.endswith("-")


def capabilities(status="Available", storages=("ManagedDisk", "ManagedDiskV2"), features=()):
    sku = {"name": "Standard_D4ds_v5", "status": status, "supportedZones": ["1", "2"]}
    edition = {
        "name": "GeneralPurpose",
        "supportedServerSkus": [sku],
        "supportedStorageEditions": [{"name": n} for n in storages],
    }
    return [
        {
            "supportedServerEditions": [edition],
            "supportedFeatures": [{"name": n, "status": "Enabled"} for n in features],
        }
    ]


def usages(cores=(10, 100), family=("standardDdsv5Family", 0, 100)):
    items = [{"name": {"value": "cores"}, "currentValue": cores[0], "limit": cores[1]}]
    if family:
        items.append({"name": {"value": family[0]}, "currentValue": family[1], "limit": family[2]})
    return items


@pytest.fixture
def arm(monkeypatch):
    """Fake ARM: `arm_list(path, ...)` answers from `answers` by the end of the path."""
    answers = {
        "capabilities": capabilities(),
        "usages": usages(),
        "skus": [{"name": "Standard_D4ds_v5", "resourceType": "virtualMachines", "family": "standardDdsv5Family"}],
    }
    monkeypatch.setenv("ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setattr(azure_preflight, "token", lambda: "t")
    monkeypatch.setattr(
        azure_preflight, "arm_list", lambda token, path, api, **params: iter(answers[path.rsplit("/", 1)[1]])
    )
    return answers


def test_the_pre_flight_passes_and_returns_the_zones_of_the_sku(arm):
    assert azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2") == ["1", "2"]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"capabilities": capabilities(features=("OfferRestricted",))}, "restricted"),
        ({"capabilities": [{**capabilities()[0], "reason": "Offer is Restricted"}]}, "restricted"),
        ({"capabilities": []}, "does not offer the SKU"),
        ({"capabilities": capabilities(status="Disabled")}, "disabled"),
        ({"capabilities": capabilities(storages=("ManagedDisk",))}, "not ManagedDiskV2"),
        ({"usages": usages(cores=(98, 100))}, "regional cores quota has 2 of 100"),
        ({"usages": usages(family=("standardDdsv5Family", 98, 100))}, "quota standardDdsv5Family has 2 of 100"),
    ],
)  # fmt: skip
def test_the_pre_flight_refuses_with_the_reason(arm, change, message):
    arm.update(change)
    with pytest.raises(PreflightError, match=message):
        azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2")


@pytest.mark.parametrize(
    ("change", "region_fatal"),
    [
        ({"usages": usages(cores=(98, 100))}, False),  # used up by running servers: it may free up
        ({"usages": usages(family=("standardDdsv5Family", 98, 100))}, False),
        ({"usages": usages(cores=(0, 2))}, True),  # the limit itself is below the 4 vCPUs needed
        ({"usages": usages(family=("standardDdsv5Family", 0, 2))}, True),
    ],
)
def test_only_a_quota_limit_below_the_need_is_region_unavailable(arm, change, region_fatal):
    arm.update(change)
    with pytest.raises(PreflightError) as caught:
        azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2")
    assert isinstance(caught.value, RegionUnavailable) is region_fatal


def test_a_missing_cores_quota_is_logged_and_passes(arm, caplog):
    arm["usages"] = usages(family=None)[:0]
    assert azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2") == ["1", "2"]
    assert "cores quota is not listed" in caplog.text


def test_a_quota_family_the_usages_do_not_list_only_checks_the_regional_cores(arm, caplog):
    arm["usages"] = usages(family=None)
    with caplog.at_level("INFO"):
        assert azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2") == ["1", "2"]
    assert "do not list the quota family standardDdsv5Family" in caplog.text


def test_a_check_that_cannot_be_made_passes(monkeypatch, caplog):
    monkeypatch.setenv("ARM_SUBSCRIPTION_ID", "sub")

    def unreachable(*args, **kwargs):
        raise OSError("no route")

    monkeypatch.setattr(azure_preflight, "arm_list", unreachable)
    assert azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2") is None
    assert "skipped" in caplog.text


def test_an_unexpected_answer_passes_too(arm, caplog):
    arm["usages"] = [{"name": None}]
    assert azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2") is None
    assert "skipped" in caplog.text


def test_the_pre_flight_fetches_one_token(arm, monkeypatch):
    tokens = []
    monkeypatch.setattr(azure_preflight, "token", lambda: tokens.append(1) or "t")
    azure_preflight.check("westeurope", "Standard_D4ds_v5", 4, "ManagedDiskV2")
    assert len(tokens) == 1


def test_an_unchecked_pre_flight_still_requests_the_client_zone():
    assert azure.zone_of(make_spec(zone="westeurope-3"), None) == "3"
    assert azure.zone_of(make_spec(zone=None), None) is None


def test_inventory_lists_the_tagged_servers(monkeypatch):
    monkeypatch.setenv("ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setattr(azure_preflight, "token", lambda: "t")
    created = {"createdAt": "2026-11-20T10:00:00Z"}
    servers = [
        {"id": "/s/a", "tags": {"sc_inspector_run": "r1", "sc_inspector_expires": "20261120t1600z"},
         "systemData": created},
        {"id": "/s/b", "tags": {"team": "x"}, "systemData": created},
        {"id": "/s/c", "systemData": created},
    ]  # fmt: skip
    seen = []
    monkeypatch.setattr(
        azure_preflight, "arm_list", lambda token, path, api, **p: seen.append((path, api)) or iter(servers)
    )
    assert azure.inventory() == [
        {"id": "/s/a", "run": "r1", "expires_at": "2026-11-20T16:00:00Z", "created_at": "2026-11-20T10:00:00Z"}
    ]
    assert seen == [("/subscriptions/sub/providers/Microsoft.DBforPostgreSQL/flexibleServers", "2025-08-01")]
    json.dumps(azure.inventory())


def test_inventory_reports_a_server_with_missing_facts_as_unknown(monkeypatch):
    monkeypatch.setenv("ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setattr(azure_preflight, "token", lambda: "t")
    servers = [{"id": "/s/a", "tags": {"sc_inspector_run": "r1"}}]
    monkeypatch.setattr(azure_preflight, "arm_list", lambda token, path, api, **p: iter(servers))
    assert azure.inventory() == [{"id": "/s/a", "run": "r1", "expires_at": None, "created_at": "1970-01-01T00:00:00Z"}]
