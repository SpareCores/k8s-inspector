import asyncio
import dataclasses
import io
import json
import os
import urllib.request

import pulumi
import pytest

from k8s_inspector_dbaas import vendors
from k8s_inspector_dbaas.vendors import vultr as vendor

DATABASE = "vultr:index/database:Database"


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        self.resources = []
        self.timeouts = {}  # resource type -> custom timeouts

    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        self.resources.append(args)
        outputs = dict(args.inputs)
        if args.typ == "vultr:index/database:Database":
            outputs |= {"host": "private-db.vultrdb.com", "port": "16751", "user": "vultradmin", "dbname": "defaultdb"}
        return [f"{args.name}-id", outputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}, None


def run(program) -> Mocks:
    """Run `program()` (a Pulumi program body) under the mocks and return them once everything is registered."""
    mocks = Mocks()
    asyncio.set_event_loop(asyncio.new_event_loop())  # Python 3.14+ no longer makes one for the main thread
    pulumi.runtime.set_mocks(mocks, project="k8s-inspector-dbaas", stack="test", preview=False)

    def record(args):
        mocks.timeouts[args.type_] = args.opts.custom_timeouts

    @pulumi.runtime.test
    def wrapper():
        pulumi.runtime.register_stack_transformation(record)
        return pulumi.Output.from_input(program()).apply(lambda _: None)

    wrapper()
    return mocks


@pytest.fixture
def vultr_spec(spec):
    return dataclasses.replace(
        spec,
        vendor="vultr",
        database_id="vultr-dbaas-startup-cc-hp-amd-2-80-2",
        region="ewr",
        run="vultr-startup-cc-hp-amd-2-80-2-1234abcd",
        network={"vpc_id": "vpc-1", "trusted_ips": ["10.0.0.0/21"]},
        record={"vcpus": 2, "memory_amount": 2048, "ha": ["none"]},
    )


def resource(mocks, typ):
    (found,) = [r for r in mocks.resources if r.typ == typ]
    return found


def test_the_database_joins_the_vpc_and_trusts_its_range(vultr_spec):
    mocks = run(lambda: vendor.provision(vultr_spec).host)
    inputs = resource(mocks, DATABASE).inputs
    assert inputs["databaseEngine"] == "pg" and inputs["databaseEngineVersion"] == "18"
    assert inputs["plan"] == "vultr-dbaas-startup-cc-hp-amd-2-80-2" and inputs["region"] == "ewr"
    assert inputs["vpcId"] == "vpc-1" and inputs["trustedIps"] == ["10.0.0.0/21"]
    assert inputs["label"] == vultr_spec.run
    assert inputs["tag"] == "sc_inspector_expires:20261120t1600z:sc_inspector_run:r20261120t1000-aaaa"
    assert inputs["password"]["value"] == "s3cret-pass" and len(inputs["password"]) == 2  # a secret, not a plain string


def test_the_outputs_are_the_private_host_and_the_default_user(vultr_spec):
    seen = {}

    def program():
        out = vendor.provision(vultr_spec)
        return pulumi.Output.all(out.host, out.port, out.admin_user, out.admin_db).apply(
            lambda values: seen.update(values=values)
        )

    run(program)
    assert seen["values"] == ["private-db.vultrdb.com", 16751, "vultradmin", "defaultdb"]


def test_the_api_key_stays_out_of_the_provider_and_the_environment_keeps_it(vultr_spec, monkeypatch):
    monkeypatch.setenv("VULTR_API_KEY", "key-from-secret")
    mocks = run(lambda: vendor.provision(vultr_spec).host)
    assert "apiKey" not in resource(mocks, "pulumi:providers:vultr").inputs
    assert os.environ["VULTR_API_KEY"] == "key-from-secret"


def test_the_database_uses_the_explicit_provider_and_the_timeouts_of_the_spec(vultr_spec):
    mocks = run(lambda: vendor.provision(vultr_spec).host)
    database = resource(mocks, DATABASE)
    assert "pulumi:providers:vultr::vultr::" in database.provider
    assert database.name == vultr_spec.run
    timeouts = mocks.timeouts[DATABASE]
    assert (timeouts.create, timeouts.delete) == ("50m", "100m")


def test_a_high_availability_plan_is_refused(vultr_spec):
    spec = dataclasses.replace(vultr_spec, record={**vultr_spec.record, "ha": ["single-zone"]})
    with pytest.raises(ValueError, match="standalone"):
        vendor.provision(spec)


def test_the_tag_round_trips():
    labels = {"sc_inspector_run": "r1-aaaa", "sc_inspector_expires": "20261120t1600z"}
    assert vendor._labels(vendor._tag(labels)) == labels
    assert vendor._labels("") == {}
    assert vendor._labels("created-by:sc-runner") == {"created-by": "sc-runner"}


def test_the_vendor_module_is_found_by_its_id():
    assert vendors.load("vultr").inventory is vendor.inventory


def pages(monkeypatch, *pages):
    requested = []

    def urlopen(request, timeout):
        requested.append((request.full_url, request.get_header("Authorization")))
        return io.BytesIO(json.dumps(pages[len(requested) - 1]).encode())

    monkeypatch.setenv("VULTR_API_KEY", "k")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return requested


def test_the_inventory_lists_tagged_databases_across_pages(monkeypatch):
    tag = "sc_inspector_expires:20261121t0905z:sc_inspector_run:r1-aaaa"
    requested = pages(
        monkeypatch,
        {
            "databases": [{"id": "d1", "tag": tag, "date_created": "2026-11-20T10:00:00+00:00"}],
            "meta": {"links": {"next": "abc"}},
        },
        {
            "databases": [
                {"id": "d2", "tag": "created-by:sc-runner", "date_created": "2026-11-01T00:00:00+00:00"},
                {"id": "d3", "tag": "", "date_created": "2026-11-02T00:00:00+00:00"},
            ],
            "meta": {"links": {"next": ""}},
        },
    )
    assert vendor.inventory() == [
        {"id": "d1", "run": "r1-aaaa", "expires_at": "2026-11-21T09:05:00Z", "created_at": "2026-11-20T10:00:00+00:00"}
    ]
    assert [url for url, _ in requested] == [
        "https://api.vultr.com/v2/databases?per_page=500",
        "https://api.vultr.com/v2/databases?per_page=500&cursor=abc",
    ]
    assert {auth for _, auth in requested} == {"Bearer k"}
