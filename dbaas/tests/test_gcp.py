import asyncio
import json
from dataclasses import replace

import pulumi
import pytest

from k8s_inspector_dbaas import vendors
from k8s_inspector_dbaas.vendors import gcp as vendor

SECRET_MARKER = "4dabf18193072939515e22adb298388d"  # Pulumi's marker of a secret value in mock inputs
NETWORK = {"network": "projects/p/global/networks/sc-main", "allocated_range": "sc-main-psa"}


class Recorder(pulumi.runtime.Mocks):
    def __init__(self):
        self.resources = []
        self.options = {}  # resource type -> its ResourceOptions

    def new_resource(self, args):
        self.resources.append(args)
        outputs = dict(args.inputs)
        if args.typ == "gcp:sql/databaseInstance:DatabaseInstance":
            outputs["privateIpAddress"] = "10.252.0.3"
        return [f"{args.name}-id", outputs]

    def call(self, args):
        return {}


@pytest.fixture(autouse=True)
def event_loop():
    """Pulumi's mocks use the current event loop, which Python 3.15 no longer creates by itself."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def gcp_spec(spec):
    return replace(
        spec,
        vendor="gcp",
        database_id="db-perf-optimized-N-8",
        region="europe-west4",
        zone="europe-west4-b",
        network=NETWORK,
        record={"vcpus": 8, "memory_amount": 65536, "compatible_storage_ids": ["cloudsql-ssd"]},
    )


def provision(spec):
    """Run `provision` under Pulumi's mocks; the registered resources and the outputs."""
    mocks = Recorder()
    pulumi.runtime.set_mocks(mocks, "k8s-inspector-dbaas", "test", preview=False)
    outputs = {}

    def record(args):
        mocks.options[args.type_] = args.opts

    @pulumi.runtime.test
    def check():
        pulumi.runtime.register_stack_transformation(record)
        outputs["out"] = vendor.provision(spec)
        return outputs["out"].host.apply(lambda _: None)

    check()
    return mocks, outputs["out"]


def kinds(mocks, typ):
    return [r for r in mocks.resources if r.typ == typ]


def test_the_module_is_found_by_its_vendor_id():
    assert vendors.load("gcp").provision is vendor.provision


def test_enterprise_plus_instance_is_private_zonal_and_tagged(gcp_spec):
    mocks, out = provision(gcp_spec)
    (instance,) = kinds(mocks, "gcp:sql/databaseInstance:DatabaseInstance")
    (provider,) = kinds(mocks, "pulumi:providers:gcp")
    assert provider.inputs["region"] == "europe-west4"
    assert "credentials" not in provider.inputs and "project" not in provider.inputs
    i = instance.inputs
    assert (i["name"], i["databaseVersion"], i["region"], i["deletionProtection"]) == (
        "sc-r20261120t1000-aaaa", "POSTGRES_18", "europe-west4", False,
    )  # fmt: skip
    s = i["settings"]
    assert (s["tier"], s["edition"], s["availabilityType"], s["diskType"], s["diskAutoresize"]) == (
        "db-perf-optimized-N-8", "ENTERPRISE_PLUS", "ZONAL", "PD_SSD", False,
    )  # fmt: skip
    assert s["locationPreference"]["zone"] == "europe-west4-b"
    assert s["dataCacheConfig"]["dataCacheEnabled"] is False
    assert s["deletionProtectionEnabled"] is False
    assert s["backupConfiguration"]["enabled"] is False
    assert s["userLabels"] == {**gcp_spec.labels, "sc_inspector_run_name": "fake-m5-large-1234abcd"}
    ip = s["ipConfiguration"]
    assert (ip["ipv4Enabled"], ip["privateNetwork"], ip["allocatedIpRange"]) == (
        False,
        NETWORK["network"],
        "sc-main-psa",
    )
    timeouts = mocks.options["gcp:sql/databaseInstance:DatabaseInstance"].custom_timeouts
    assert (timeouts.create, timeouts.delete) == ("50m", "100m")


def test_the_password_is_a_secret_and_the_workload_user_is_set(gcp_spec):
    mocks, out = provision(gcp_spec)
    (instance,) = kinds(mocks, "gcp:sql/databaseInstance:DatabaseInstance")
    root_password = instance.inputs["rootPassword"]
    assert isinstance(root_password, dict) and SECRET_MARKER in root_password  # a Pulumi secret, not a plain string
    assert (out.admin_user, out.admin_db, out.user, out.grant_role, out.sslmode, out.port) == (
        "postgres", "postgres", "scadmin", "cloudsqlsuperuser", "require", 5432,
    )  # fmt: skip


def test_disk_is_sized_for_the_io_budget_on_pd_ssd(gcp_spec):
    mocks, _ = provision(gcp_spec)
    (instance,) = kinds(mocks, "gcp:sql/databaseInstance:DatabaseInstance")
    # 8 vCPUs: the 50 MB/s floor, capped by the VM at 6 MB/s per vCPU = 48 MB/s -> 48 / 0.48 MB/s per GiB = 100 GiB
    assert instance.inputs["settings"]["diskSize"] == 100


def test_hyperdisk_tiers_use_hyperdisk_balanced_and_enterprise_n1_has_no_data_cache(gcp_spec):
    c4a = replace(
        gcp_spec,
        database_id="db-c4a-highmem-4",
        record={"vcpus": 4, "memory_amount": 32768, "compatible_storage_ids": ["cloudsql-hyperdisk"]},
    )
    mocks, _ = provision(c4a)
    s = kinds(mocks, "gcp:sql/databaseInstance:DatabaseInstance")[0].inputs["settings"]
    assert (s["diskType"], s["edition"], s["diskSize"]) == ("HYPERDISK_BALANCED", "ENTERPRISE_PLUS", 64)
    n1 = replace(
        gcp_spec,
        database_id="db-n1-standard-2",
        record={"vcpus": 2, "memory_amount": 7680, "compatible_storage_ids": ["cloudsql-ssd-standard", "cloudsql-hdd"]},
    )
    mocks, _ = provision(n1)
    s = kinds(mocks, "gcp:sql/databaseInstance:DatabaseInstance")[0].inputs["settings"]
    assert (s["diskType"], s["edition"]) == ("PD_SSD", "ENTERPRISE")
    assert "dataCacheConfig" not in s


def test_without_a_zone_cloud_sql_picks_one(gcp_spec):
    mocks, _ = provision(replace(gcp_spec, zone=None))
    assert "locationPreference" not in kinds(mocks, "gcp:sql/databaseInstance:DatabaseInstance")[0].inputs["settings"]


def test_a_missing_network_reference_fails_loudly(gcp_spec):
    with pytest.raises(KeyError):
        provision(replace(gcp_spec, network={}))


def test_the_instance_is_named_by_the_run_id_and_a_name_that_does_not_fit_is_refused():
    assert vendor.instance_name("R20261120t1000-AAAA") == "sc-r20261120t1000-aaaa"
    with pytest.raises(ValueError, match="too long"):
        vendor.instance_name("r" * 99)


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        page = self.pages[len(self.calls) - 1]

        class Response:
            def raise_for_status(self):
                pass

            def json(self):
                return page

        return Response()


def test_inventory_lists_only_instances_with_the_run_label(monkeypatch):
    labelled = {
        "name": "sc-run-1",
        "createTime": "2026-11-20T10:00:03.123Z",
        "settings": {"userLabels": {"sc_inspector_run": "r1", "sc_inspector_expires": "20261120t1600z"}},
    }
    other = {"name": "prod-db", "createTime": "2020-01-01T00:00:00Z", "settings": {"userLabels": {"team": "x"}}}
    bare = {"name": "bare", "createTime": "2020-01-01T00:00:00Z", "settings": {}}
    session = FakeSession([{"items": [other, labelled], "nextPageToken": "t2"}, {"items": [bare]}])
    monkeypatch.setenv("GOOGLE_CREDENTIALS", json.dumps({"type": "external_account"}))
    monkeypatch.setenv("GOOGLE_PROJECT", "proj")
    monkeypatch.setattr(vendor.google.auth, "load_credentials_from_dict", lambda info, scopes: (object(), "proj"))
    monkeypatch.setattr(vendor, "AuthorizedSession", lambda credentials: session)
    assert vendor.inventory() == [
        {"id": "sc-run-1", "run": "r1", "expires_at": "2026-11-20T16:00:00Z", "created_at": "2026-11-20T10:00:03.123Z"}
    ]
    assert session.calls == [
        ("https://sqladmin.googleapis.com/v1/projects/proj/instances", None),
        ("https://sqladmin.googleapis.com/v1/projects/proj/instances", {"pageToken": "t2"}),
    ]


def test_an_instance_without_an_expiry_label_is_reported_with_an_unknown_expiry():
    item = {
        "name": "sc-r1",
        "createTime": "2026-11-20T10:00:03Z",
        "settings": {"userLabels": {"sc_inspector_run": "r1"}},
    }
    assert vendor._tagged(item) == {
        "id": "sc-r1", "run": "r1", "expires_at": None, "created_at": "2026-11-20T10:00:03Z",
    }  # fmt: skip


class CleanupSession:
    """The project's instances; a DELETE makes one PENDING_DELETE, and it is gone `lists_to_vanish` lists later."""

    def __init__(self, instances, delete_status=200, lists_to_vanish=2):
        self.instances, self.delete_status, self.lists_to_vanish = instances, delete_status, lists_to_vanish
        self.requests, self.pending = [], {}

    def _response(self, code, body):
        class Response:
            status_code = code

            def raise_for_status(self):
                assert code < 400, code

            def json(self):
                return body

        return Response()

    def delete(self, url, timeout=None):
        self.requests.append(("DELETE", url.rsplit("/", 1)[1]))
        name = url.rsplit("/", 1)[1]
        if self.delete_status == 200:
            self.pending[name] = self.lists_to_vanish
        return self._response(self.delete_status, {})

    def get(self, url, params=None, timeout=None):
        self.requests.append(("LIST", None))
        for name in list(self.pending):
            self.pending[name] -= 1
            if self.pending[name] < 0:
                self.instances = [i for i in self.instances if i["name"] != name]
        items = [dict(i, state="PENDING_DELETE") if i["name"] in self.pending else i for i in self.instances]
        return self._response(200, {"items": items})


def use_session(monkeypatch, session):
    monkeypatch.setenv("GOOGLE_CREDENTIALS", json.dumps({"type": "external_account"}))
    monkeypatch.setenv("GOOGLE_PROJECT", "proj")
    monkeypatch.setattr(vendor.google.auth, "load_credentials_from_dict", lambda info, scopes: (object(), "proj"))
    monkeypatch.setattr(vendor, "AuthorizedSession", lambda credentials: session)
    monkeypatch.setattr(vendor.time, "sleep", lambda seconds: None)


def instance(name, run, state="RUNNABLE"):
    return {"name": name, "state": state, "settings": {"userLabels": {"sc_inspector_run_name": run}}}


def test_cleanup_deletes_the_instance_labelled_with_the_run_name_and_waits_until_it_is_gone(monkeypatch):
    session = CleanupSession([instance("sc-r1", "gcp-a-1"), instance("sc-r2", "gcp-b-2"), {"name": "other"}])
    use_session(monkeypatch, session)
    vendor.cleanup("gcp-a-1")
    assert [r for r in session.requests if r[0] == "DELETE"] == [("DELETE", "sc-r1")]
    assert [i["name"] for i in session.instances] == ["sc-r2", "other"]


def test_cleanup_has_nothing_to_do_when_no_instance_carries_the_run_name(monkeypatch):
    session = CleanupSession([instance("sc-r2", "gcp-b-2")])
    use_session(monkeypatch, session)
    vendor.cleanup("gcp-a-1")
    assert session.requests == [("LIST", None)]


def test_cleanup_tolerates_an_instance_that_vanished_before_the_delete(monkeypatch):
    session = CleanupSession([instance("sc-r1", "gcp-a-1")], delete_status=404)
    use_session(monkeypatch, session)
    original = session.delete

    def delete_then_vanish(url, timeout=None):
        response = original(url, timeout)
        session.instances = []
        return response

    session.delete = delete_then_vanish
    vendor.cleanup("gcp-a-1")


def test_cleanup_only_waits_for_an_instance_that_is_already_being_deleted(monkeypatch):
    session = CleanupSession([instance("sc-r1", "gcp-a-1", "PENDING_DELETE")])
    session.pending["sc-r1"] = 1
    use_session(monkeypatch, session)
    vendor.cleanup("gcp-a-1")
    assert ("DELETE", "sc-r1") not in session.requests


def test_cleanup_fails_when_the_instance_does_not_go_away(monkeypatch):
    session = CleanupSession([instance("sc-r1", "gcp-a-1")], lists_to_vanish=10_000)
    use_session(monkeypatch, session)
    clock = iter(range(0, 100_000, 100))
    monkeypatch.setattr(vendor.time, "monotonic", lambda: next(clock))
    with pytest.raises(TimeoutError, match="sc-r1"):
        vendor.cleanup("gcp-a-1")
