import asyncio
import dataclasses
import io
import json

import pulumi
import pulumi_ovh as ovh
import pytest

from k8s_inspector_dbaas import vendors
from k8s_inspector_dbaas.spec import DbSpec
from k8s_inspector_dbaas.vendors import ovh as ovh_vendor

RECORD = {
    "vcpus": 2,
    "memory_amount": 4096,
    "storage_size": 80,
    "storage_extra_max": 160,
    "max_read_replicas": 0,
    "api_reference_object": {"engine": "postgresql", "plan": "essential", "flavor": "db1-4"},
}


class Mocks(pulumi.runtime.Mocks):
    resources: list = []

    def new_resource(self, args):
        self.resources.append((args.typ, args.name, args.inputs))
        state = dict(args.inputs)
        if args.typ == "ovh:CloudProject/database:Database":
            state["endpoints"] = [
                {"component": "pgbouncer", "domain": "pool.example", "port": 20185},
                {"component": "postgresql", "domain": "pg-1.example", "port": 20184},
            ]
        if args.typ == "ovh:CloudProjectDatabase/postgresSqlUser:PostgresSqlUser":
            state["password"] = "generated-by-ovh"
        return args.name + "-id", state

    def call(self, args):
        return {}


@pytest.fixture
def ovh_spec(spec) -> DbSpec:
    return dataclasses.replace(
        spec,
        vendor="ovh",
        database_id="postgresql-essential-db1-4",
        region="GRA",
        client_region="GRA11",
        run="ovh-d2-2-1234abcd",
        record=RECORD,
        client_addresses=["203.0.113.7"],
    )


@pytest.fixture(autouse=True)
def mocks():
    """Pulumi's mocks need an event loop (none is current on Python 3.14+)."""
    asyncio.set_event_loop(asyncio.new_event_loop())
    Mocks.resources.clear()
    pulumi.runtime.set_mocks(Mocks(), "k8s-inspector-dbaas", "test", preview=False)
    yield
    asyncio.get_event_loop().close()


def spy(monkeypatch, module, name):
    calls = []
    real = getattr(module, name)

    def wrapper(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(module, name, wrapper)
    return calls


@pulumi.runtime.test
def test_the_database_is_standalone_tagged_allowlisted_and_has_the_flavor_of_keeper(ovh_spec):
    outputs = ovh_vendor.provision(ovh_spec)

    def check(_):
        (inputs,) = [i for t, _, i in Mocks.resources if t.endswith(":Database")]
        assert (inputs["plan"], inputs["flavor"], inputs["version"], inputs["engine"]) == (
            "essential",
            "db1-4",
            "18",
            "postgresql",
        )
        assert inputs["nodes"] == [{"region": "GRA"}]
        assert inputs["diskSize"] == 80
        assert inputs["ipRestrictions"][0]["ip"] == "203.0.113.7/32"
        assert inputs["description"] == "sc_inspector_run=r20261120t1000-aaaa sc_inspector_expires=20261120t1600z"
        assert inputs["deletionProtection"] is False

    return pulumi.Output.all(outputs.host).apply(check)


@pulumi.runtime.test
def test_the_outputs_are_the_postgresql_endpoint_and_the_generated_admin_password(ovh_spec):
    outputs = ovh_vendor.provision(ovh_spec)

    def check(values):
        host, port, password = values
        assert (host, port, password) == ("pg-1.example", 20184, "generated-by-ovh")
        assert (outputs.admin_user, outputs.admin_db, outputs.sslmode) == ("avnadmin", "defaultdb", "require")

    return pulumi.Output.all(outputs.host, outputs.port, outputs.password).apply(check)


@pulumi.runtime.test
async def test_the_admin_user_is_avnadmin_and_the_password_is_a_secret(ovh_spec):
    outputs = ovh_vendor.provision(ovh_spec)
    assert await outputs.password.is_secret()
    await outputs.password.future()
    (inputs,) = [i for t, _, i in Mocks.resources if t.endswith(":PostgresSqlUser")]
    assert inputs["name"] == "avnadmin"


@pulumi.runtime.test
def test_the_provider_has_no_credentials_and_the_timeouts_come_from_the_spec(ovh_spec, monkeypatch):
    providers = spy(monkeypatch, ovh, "Provider")
    databases = spy(monkeypatch, ovh.cloudproject, "Database")
    users = spy(monkeypatch, ovh.cloudprojectdatabase, "PostgresSqlUser")
    ovh_vendor.provision(ovh_spec)
    assert providers[0][1] == {}  # credentials, endpoint and project come from the environment
    for _, kwargs in (*databases, *users):
        timeouts = kwargs["opts"].custom_timeouts
        assert (timeouts.create, timeouts.delete) == ("50m", "100m")
        assert kwargs["opts"].provider is not None
    assert "service_name" not in databases[0][1]
    assert databases[0][0] == ("ovh-d2-2-1234abcd",)


@pulumi.runtime.test
def test_a_plan_with_replicas_has_a_node_for_each(ovh_spec, monkeypatch):
    databases = spy(monkeypatch, ovh.cloudproject, "Database")
    ovh_vendor.provision(dataclasses.replace(ovh_spec, record={**RECORD, "max_read_replicas": 2}))
    assert databases[0][1]["nodes"] == [{"region": "GRA"}] * 3


def test_a_database_without_a_client_address_is_refused(ovh_spec):
    with pytest.raises(ValueError, match="no client address"):
        ovh_vendor.provision(dataclasses.replace(ovh_spec, client_addresses=()))


def test_a_client_range_is_kept_as_it_is(ovh_spec):
    spec = dataclasses.replace(ovh_spec, client_addresses=("10.1.0.0/24", "198.51.100.4"))
    assert [r["ip"] for r in ovh_vendor.allowed_ips(spec)] == ["10.1.0.0/24", "198.51.100.4/32"]


@pytest.mark.parametrize(
    ("memory_mib", "vcpus", "size", "extra_max", "expected"),
    [
        (4096, 2, 80, 160, 80),  # the schema-derived size is below the flavor's floor
        (16384, 4, 320, 1280, 320),
        (65536, 16, 1280, 5120, 1280),
        (4096, 2, 80, 0, 80),  # no extra storage: the floor is the ceiling
        (1048576, 8, 85, 40, 120),  # the floor is not a multiple of 10: the ceiling is rounded down to one
    ],
)
def test_the_disk_is_between_the_floor_and_the_ceiling_of_the_flavor_in_steps_of_ten(
    ovh_spec, memory_mib, vcpus, size, extra_max, expected
):
    record = {
        **RECORD,
        "memory_amount": memory_mib,
        "vcpus": vcpus,
        "storage_size": size,
        "storage_extra_max": extra_max,
    }
    spec = dataclasses.replace(ovh_spec, record=record)
    assert ovh_vendor.disk_gib(spec) == expected
    assert expected >= size


def test_the_disk_is_a_multiple_of_ten_when_the_plan_is_above_the_floor(ovh_spec):
    record = {**RECORD, "memory_amount": 262144, "vcpus": 8, "storage_size": 80, "storage_extra_max": 5000}
    size = ovh_vendor.disk_gib(dataclasses.replace(ovh_spec, record=record))
    assert size % 10 == 0 and size >= 80


class FakeApi:
    """The token endpoint and the project's database list and details."""

    def __init__(self, clusters: dict[str, dict]):
        self.clusters = clusters
        self.requests: list[tuple[str, str | None]] = []

    def __call__(self, request, timeout=None):
        auth = request.get_header("Authorization")
        self.requests.append((request.full_url, auth))
        url = request.full_url
        if url.endswith("/auth/oauth2/token"):
            assert b"grant_type=client_credentials" in request.data
            body = {"access_token": "tok"}
        elif url.endswith("/database/postgresql"):
            body = list(self.clusters)
        else:
            body = self.clusters[url.rsplit("/", 1)[1]]
        return io.BytesIO(json.dumps(body).encode())


def test_inventory_lists_the_databases_that_carry_the_run_tag(monkeypatch):
    api = FakeApi(
        {
            "c1": {
                "description": "sc_inspector_run=r1-abcd sc_inspector_expires=20261120t1600z",
                "createdAt": "2026-11-20T10:00:00Z",
            },
            "c2": {"description": "somebody else's database", "createdAt": "2026-11-01T10:00:00Z"},
            "c3": {"description": None, "createdAt": "2026-11-02T10:00:00Z"},
        }
    )
    monkeypatch.setattr(ovh_vendor.urllib.request, "urlopen", api)
    monkeypatch.setenv("OVH_ENDPOINT", "ovh-eu")
    monkeypatch.setenv("OVH_CLIENT_ID", "id")
    monkeypatch.setenv("OVH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("OVH_CLOUD_PROJECT_SERVICE", "proj1")
    assert ovh_vendor.inventory() == [
        {"id": "c1", "run": "r1-abcd", "expires_at": "2026-11-20T16:00:00Z", "created_at": "2026-11-20T10:00:00Z"}
    ]
    assert api.requests[0][0] == "https://www.ovh.com/auth/oauth2/token"
    assert all(auth == "Bearer tok" for url, auth in api.requests[1:])
    assert api.requests[1][0] == "https://eu.api.ovh.com/1.0/cloud/project/proj1/database/postgresql"


def test_the_module_is_found_by_the_vendor_id():
    assert vendors.load("ovh").provision is ovh_vendor.provision
