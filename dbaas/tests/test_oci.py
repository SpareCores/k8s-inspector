import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pulumi
import pytest

from k8s_inspector_dbaas.spec import DbSpec
from k8s_inspector_dbaas.vendors import oci as vendor

RECORD = {
    "vcpus": 4,
    "memory_amount": 32768,
    "api_reference_object": {
        "shape": "VM.Standard.E5.Flex",
        "instance_ocpu_count": 2,
        "instance_memory_size_in_gbs": 32,
    },
    "compatible_storage_ids": ["optimized-150k-iops", "optimized-75k-iops", "optimized-225k-iops"],
}
NETWORK = {"compartment_id": "ocid1.compartment.x", "subnet_id": "ocid1.subnet.x", "nsg_ids": ["ocid1.nsg.x"]}
SECRET_SIGNATURE_KEY, SECRET_SIGNATURE = "4dabf18193072939515e22adb298388d", "1b47061264138c4ac30d75fd1eb44270"
ADS = ["xYzA:EU-FRANKFURT-1-AD-3", "xYzA:EU-FRANKFURT-1-AD-1", "xYzA:EU-FRANKFURT-1-AD-2"]


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        self.resources = []
        self.providers = []

    def new_resource(self, args):
        if args.typ == "pulumi:providers:oci":
            self.providers.append(args)
        else:
            self.resources.append(args)
        outputs = dict(args.inputs)
        if args.typ == "oci:Psql/dbSystem:DbSystem":
            outputs["networkDetails"] = {**args.inputs["networkDetails"], "primaryDbEndpointPrivateIp": "10.0.0.9"}
        return args.name + "-id", outputs

    def call(self, args):
        if args.token == "oci:Identity/getAvailabilityDomains:getAvailabilityDomains":
            return {"availabilityDomains": [{"name": n} for n in ADS]}
        return {}


@pytest.fixture
def oci_spec(spec) -> DbSpec:
    return DbSpec(**{**spec.__dict__, "vendor": "oci", "region": "eu-frankfurt-1", "zone": "EU-FRANKFURT-1-AD-2",
                     "network": NETWORK, "record": RECORD, "run": "oci-e5-2-32-1234abcd"})  # fmt: skip


@pytest.fixture
def mocks():
    asyncio.set_event_loop(asyncio.new_event_loop())  # Pulumi's mocks need a current loop
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="p", stack="s", preview=False)
    return mocks


async def resolve(value):
    return await pulumi.Output.from_input(value).future()


async def provisioned(spec):
    """`provision`, with the resources registered (the host waits for the DB system)."""
    outputs = vendor.provision(spec)
    await resolve(outputs.host)
    return outputs


@pulumi.runtime.test
async def test_the_db_system_is_one_standalone_node_with_the_shape_of_the_record(oci_spec, mocks):
    outputs = await provisioned(oci_spec)
    (db,) = mocks.resources
    i = db.inputs
    assert db.name == "oci-e5-2-32-1234abcd" and i["displayName"] == "oci-e5-2-32-1234abcd"
    assert (i["shape"], i["instanceCount"], i["instanceOcpuCount"], i["instanceMemorySizeInGbs"]) == (
        "PostgreSQL.VM.Standard.E5.Flex", 1, 2, 32)  # fmt: skip
    assert i["dbVersion"] == "18" and i["compartmentId"] == "ocid1.compartment.x"
    assert i["networkDetails"] == {"subnetId": "ocid1.subnet.x", "nsgIds": ["ocid1.nsg.x"]}
    assert i["managementPolicy"] == {"backupPolicy": {"kind": "NONE"}}
    assert (await resolve(outputs.host), outputs.admin_user, outputs.admin_db, outputs.port) == (
        "10.0.0.9",
        "scadmin",
        "postgres",
        5432,
    )


@pulumi.runtime.test
async def test_the_storage_is_ad_local_in_the_client_zone_at_the_lowest_tier(oci_spec, mocks):
    await provisioned(oci_spec)
    storage = mocks.resources[0].inputs["storageDetails"]
    assert storage == {"systemType": "OCI_OPTIMIZED_STORAGE", "isRegionallyDurable": False,
                       "availabilityDomain": "xYzA:EU-FRANKFURT-1-AD-2", "iops": "75000"}  # fmt: skip


@pulumi.runtime.test
async def test_the_provider_is_explicit_with_the_region_and_no_credentials(oci_spec, mocks):
    await provisioned(oci_spec)
    (provider,) = mocks.providers
    assert provider.inputs == {"region": "eu-frankfurt-1"}


@pulumi.runtime.test
async def test_tags_timeouts_and_the_secret_password(oci_spec, mocks, monkeypatch):
    options = []
    real = vendor.pulumi_oci.psql.DbSystem

    def spy(*args, opts, **kwargs):
        options.append(opts)
        return real(*args, opts=opts, **kwargs)

    monkeypatch.setattr(vendor.pulumi_oci.psql, "DbSystem", spy)
    await provisioned(oci_spec)
    db = mocks.resources[0]
    assert db.inputs["freeformTags"] == oci_spec.labels
    (opts,) = options
    assert (opts.custom_timeouts.create, opts.custom_timeouts.delete) == ("50m", "100m")
    assert opts.provider is not None
    credentials = db.inputs["credentials"]  # the whole block is a secret: the mocks show Pulumi's secret marker
    assert credentials[SECRET_SIGNATURE_KEY] == SECRET_SIGNATURE
    assert credentials["value"] == {
        "username": "scadmin",
        "passwordDetails": {"password": "s3cret-pass", "passwordType": "PLAIN_TEXT"},
    }


def test_an_incomplete_network_or_record_is_refused(oci_spec):
    with pytest.raises(ValueError, match="subnet_id"):
        vendor._network(DbSpec(**{**oci_spec.__dict__, "network": {"compartment_id": "c", "nsg_ids": []}}))
    with pytest.raises(ValueError, match="instance_ocpu_count"):
        vendor._shape({"api_reference_object": {"shape": "VM.Standard.E5.Flex"}})
    with pytest.raises(ValueError, match="no optimized"):
        vendor._iops_tier({"compatible_storage_ids": ["something"]})


def test_the_lowest_performance_tier_is_chosen():
    assert vendor._iops_tier(RECORD) == 75000
    assert vendor._iops_tier({"compatible_storage_ids": ["optimized-375k-iops", "optimized-450k-iops"]}) == 375000


def test_the_availability_domain_follows_the_zone_else_the_first():
    assert vendor.pick_availability_domain(ADS, "eu-frankfurt-1-ad-3") == "xYzA:EU-FRANKFURT-1-AD-3"
    assert vendor.pick_availability_domain(ADS, "xYzA:EU-FRANKFURT-1-AD-2") == "xYzA:EU-FRANKFURT-1-AD-2"
    assert vendor.pick_availability_domain(ADS, None) == "xYzA:EU-FRANKFURT-1-AD-1"
    assert vendor.pick_availability_domain(ADS, "elsewhere") == "xYzA:EU-FRANKFURT-1-AD-1"
    with pytest.raises(ValueError):
        vendor.pick_availability_domain([], None)


def test_inventory_lists_tagged_db_systems_of_every_region(monkeypatch):
    for env, value in {"TF_VAR_tenancy_ocid": "t", "TF_VAR_user_ocid": "u", "TF_VAR_fingerprint": "f",
                       "TF_VAR_private_key": "k", "K8SI_OCI_COMPARTMENT_ID": "c",
                       "K8SI_OCI_REGIONS": "eu-frankfurt-1, us-ashburn-1"}.items():  # fmt: skip
        monkeypatch.setenv(env, value)
    created = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)

    def system(id, tags, state="ACTIVE"):
        return SimpleNamespace(id=id, freeform_tags=tags, lifecycle_state=state, time_created=created)

    seen = []

    class Client:
        def __init__(self, config):
            seen.append(config["region"])
            self.region = config["region"]

        def list_db_systems(self, **kwargs):
            assert kwargs["compartment_id"] == "c"
            return SimpleNamespace(data=[
                system(f"{self.region}-run", {"sc_inspector_run": "r1", "sc_inspector_expires": "20261120t1600z"}),
                system(f"{self.region}-untagged", {}),
                system(f"{self.region}-gone", {"sc_inspector_run": "r2"}, "DELETED"),
            ], has_next_page=False, next_page=None, headers={})  # fmt: skip

    monkeypatch.setattr(vendor.oci.psql, "PostgresqlClient", Client)
    assert vendor.inventory() == [
        {"id": f"{r}-run", "run": "r1", "expires_at": "2026-11-20T16:00:00Z", "created_at": "2026-11-20T10:00:00Z"}
        for r in ("eu-frankfurt-1", "us-ashburn-1")
    ]
    assert seen == ["eu-frankfurt-1", "us-ashburn-1"]
