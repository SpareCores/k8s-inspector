import io
import json
from dataclasses import replace

import pulumi
import pytest

PLAN = {
    "service_type": "pg",
    "plan_compute": "rdb.standard.8CPU-32GB",
    "plan_backups": ["regular", "mini"],
    "plan_storage": {"step_gib": 10, "options": [{"base_gib": 100, "max_gib": 200}, {"base_gib": 400, "max_gib": 600}]},
}
NETWORK = {"network_id": "0a1b2c3d-net", "cidr": "10.8.0.0/21"}


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        self.resources = []

    def new_resource(self, args):
        self.resources.append(args)
        outputs = dict(args.inputs)
        if args.typ == "upcloud:index/managedDatabasePostgresql:ManagedDatabasePostgresql":
            outputs |= {
                "serviceHost": "public.example",
                "servicePort": 11550,
                "components": [
                    {"component": "pg", "host": "db.example", "port": 11551, "route": "dynamic"},
                    {"component": "pg", "host": "public-db.example", "port": 11551, "route": "public"},
                    {"component": "pgbouncer", "host": "db.example", "port": 11552, "route": "dynamic"},
                ],
            }
        return [args.name + "-id", outputs]

    def call(self, args):
        return {}


@pytest.fixture
def upcloud_vendor():
    from k8s_inspector_dbaas.vendors import upcloud

    return upcloud


def make(spec, **changes):
    record = {"vcpus": 8, "memory_amount": 32768, "api_reference_object": PLAN}
    fields = {"vendor": "upcloud", "region": "de-fra1", "database_id": "rdb.standard.8CPU-32GB", "network": NETWORK,
              "record": record} | changes  # fmt: skip
    return replace(spec, **fields)


def test_storage_snaps_to_step_and_bands(upcloud_vendor):
    bands = [(100, 200), (400, 600)]
    assert upcloud_vendor.snap_storage_gib(105, 10, bands) == 110
    assert upcloud_vendor.snap_storage_gib(250, 10, bands) == 400  # between bands: the next one
    assert upcloud_vendor.snap_storage_gib(900, 10, bands) == 600
    assert (
        upcloud_vendor.snap_storage_gib(5, 10, bands) == 100
        and upcloud_vendor.snap_storage_gib(1, 10, [(10, 50)]) == 10
    )


def test_the_cheapest_backup_tier_is_chosen(upcloud_vendor):
    assert upcloud_vendor.backup_tier(["extended", "regular"]) == "regular"
    assert upcloud_vendor.backup_tier(["mini", "regular"]) == "mini"
    with pytest.raises(ValueError):
        upcloud_vendor.backup_tier([])


def test_a_flexible_plan_is_sized_from_keeper_data(spec, upcloud_vendor):
    args = upcloud_vendor.plan_args(make(spec))
    assert args == {"plan_compute": "rdb.standard.8CPU-32GB", "plan_node_count": 1, "plan_storage_gib": 100,
                    "plan_backups": "mini"}  # fmt: skip


def test_a_flexible_plan_without_bands_is_refused(spec, upcloud_vendor):
    bare = make(spec, record={"vcpus": 8, "memory_amount": 32768, "api_reference_object": {"plan_backups": ["mini"]}})
    with pytest.raises(ValueError, match="plan_storage"):
        upcloud_vendor.plan_args(bare)


def test_a_legacy_plan_is_only_named(spec, upcloud_vendor):
    assert upcloud_vendor.plan_args(make(spec, database_id="1x2xCPU-4GB-50GB")) == {"plan": "1x2xCPU-4GB-50GB"}


@pulumi.runtime.test
def test_the_database_joins_the_private_network(spec, upcloud_vendor):
    mocks = Mocks()
    pulumi.runtime.set_mocks(mocks, project="p", stack="s", preview=False)
    out = upcloud_vendor.provision(make(spec))

    def check(values):
        host, port = values
        assert (host, port) == ("db.example", 11551)
        (provider,) = [r for r in mocks.resources if r.typ == "pulumi:providers:upcloud"]
        assert "token" not in provider.inputs
        (db,) = [r for r in mocks.resources if r.typ.endswith("ManagedDatabasePostgresql")]
        assert db.inputs["zone"] == "de-fra1" and db.inputs["labels"] == spec.labels
        assert db.inputs["networks"][0]["uuid"] == "0a1b2c3d-net" and db.inputs["networks"][0]["type"] == "private"
        props = db.inputs["properties"]["value"]  # a secret: it holds the password
        assert props["ipFilters"] == ["10.8.0.0/21"] and props["publicAccess"] is False
        assert props["adminUsername"] == "scadmin" and props["version"] == "18"
        assert db.inputs["name"].startswith("scdb-") and len(db.inputs["name"]) <= 30
        assert "providers:upcloud::upcloud::" in db.provider  # the explicit provider
        assert out.admin_db == "defaultdb"

    return pulumi.Output.all(out.host, out.port).apply(check)


def test_inventory_lists_the_tagged_postgres_services(upcloud_vendor, monkeypatch):
    pages = [
        [
            {"uuid": "u1", "type": "pg", "create_time": "2026-11-20T10:00:00Z",
             "labels": [{"key": "sc_inspector_run", "value": "r1"},
                        {"key": "sc_inspector_expires", "value": "20261120t1600z"}]},
            {"uuid": "u2", "type": "pg", "create_time": "x", "labels": []},
            {"uuid": "u3", "type": "mysql", "create_time": "x", "labels": [{"key": "sc_inspector_run", "value": "r2"}]},
        ]
    ]  # fmt: skip
    monkeypatch.setenv("UPCLOUD_TOKEN", "t")
    seen = []

    def fake_open(request, timeout):
        seen.append(request.full_url)
        assert request.get_header("Authorization") == "Bearer t"
        return io.StringIO(json.dumps(pages.pop(0)))

    monkeypatch.setattr(upcloud_vendor.urllib.request, "urlopen", fake_open)
    assert upcloud_vendor.inventory() == [
        {"id": "u1", "run": "r1", "expires_at": "2026-11-20T16:00:00Z", "created_at": "2026-11-20T10:00:00Z"}
    ]
    assert len(seen) == 1


ATTACHED = [{"name": "bench", "type": "private", "uuid": "u"}]


def test_connection_prefers_a_private_pg_route(upcloud_vendor):
    components = [
        {"component": "pg", "route": "public", "host": "pub.example", "port": 11},
        {"component": "pg", "route": "dynamic", "host": "dyn.example", "port": 11},
        {"component": "pg", "route": "private", "host": "priv.example", "port": "11569"},
    ]
    assert upcloud_vendor._connection(components, ATTACHED) == ("priv.example", 11569)


def test_connection_uses_the_dynamic_pg_host_on_an_attached_network(upcloud_vendor):
    components = [
        {"component": "pgbouncer", "route": "dynamic", "host": "bouncer.example", "port": 12},
        {"component": "pg", "route": "public", "host": "pub.example", "port": 11},
        {"component": "pg", "route": "dynamic", "host": "dyn.example", "port": 11},
    ]
    assert upcloud_vendor._connection(components, ATTACHED) == ("dyn.example", 11)


def test_connection_without_an_attached_network_is_an_error(upcloud_vendor):
    components = [{"component": "pg", "route": "dynamic", "host": "dyn.example", "port": 11}]
    with pytest.raises(RuntimeError, match="not attached"):
        upcloud_vendor._connection(components, [])


def test_connection_never_falls_back_to_the_public_host(upcloud_vendor):
    with pytest.raises(RuntimeError, match="no private or dynamic"):
        upcloud_vendor._connection([{"component": "pg", "route": "public", "host": "p", "port": 11}], ATTACHED)


def test_labels_come_as_a_list_or_a_dict(upcloud_vendor):
    assert upcloud_vendor.label_dict([{"key": "a", "value": "1"}]) == {"a": "1"}
    assert upcloud_vendor.label_dict({"a": "1"}) == {"a": "1"}
    assert upcloud_vendor.label_dict(None) == {}
