import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pulumi
import pytest

from k8s_inspector_dbaas.spec import DbSpec
from k8s_inspector_dbaas.vendors import aws


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        self.resources = []

    def new_resource(self, args):
        self.resources.append(args)
        outputs = dict(args.inputs)
        if args.typ == "aws:rds/instance:Instance":
            outputs |= {"address": f"{args.inputs['identifier']}.abc.rds.amazonaws.com", "port": 5432}
        return args.name + "-id", outputs

    def call(self, args):
        return {}


@pytest.fixture
def mocks():
    return Mocks()


def make_spec(spec: DbSpec, **changes) -> DbSpec:
    network = {"subnet_group": "sg-bench", "security_group_id": "sg-0123"}
    aws_spec = replace(
        spec, vendor="aws", database_id="db.m6g.large", region="us-east-1", zone="us-east-1b",
        run="aws-db-m6g-large-1234abcd", network=network,
    )  # fmt: skip
    return replace(aws_spec, **changes)


def provisioned(spec: DbSpec, mocks: Mocks):
    """Run `provision` against the mocks: (outputs, host, port, the RDS instance's registration, the provider's)."""

    async def run():
        pulumi.runtime.set_mocks(mocks, project="p", stack="s", preview=False)
        out = aws.provision(spec)
        host, port = await asyncio.gather(
            pulumi.Output.from_input(out.host).future(), pulumi.Output.from_input(out.port).future()
        )
        return out, host, port

    out, host, port = asyncio.run(run())
    by_type = {r.typ: r for r in mocks.resources}
    return out, host, port, by_type["aws:rds/instance:Instance"], by_type["pulumi:providers:aws"]


def test_the_instance_is_a_private_single_az_postgres_without_leftovers(spec, mocks):
    spec = make_spec(spec)
    out, host, port, instance, provider = provisioned(spec, mocks)
    i = instance.inputs
    assert (i["engine"], i["engineVersion"], i["instanceClass"]) == ("postgres", "18", "db.m6g.large")
    assert (i["publiclyAccessible"], i["multiAz"], i["deletionProtection"]) == (False, False, False)
    assert (i["backupRetentionPeriod"], i["skipFinalSnapshot"], i["deleteAutomatedBackups"]) == (0, True, True)
    assert (i["dbSubnetGroupName"], i["vpcSecurityGroupIds"]) == ("sg-bench", ["sg-0123"])
    assert (i["availabilityZone"], i["storageType"], i["storageEncrypted"]) == ("us-east-1b", "gp3", True)
    assert i["identifier"] == "sc-aws-db-m6g-large-1234abcd"
    assert host == "sc-aws-db-m6g-large-1234abcd.abc.rds.amazonaws.com" and port == 5432
    assert (out.admin_user, out.admin_db, out.sslmode) == ("scadmin", "postgres", "require")


def test_the_provider_is_explicit_regional_and_has_no_credentials(spec, mocks):
    *_, instance, provider = provisioned(make_spec(spec), mocks)
    assert provider.inputs["region"] == "us-east-1"
    credentials = {"accessKey", "secretKey", "token", "profile", "assumeRole", "assumeRoleWithWebIdentity"}
    assert not credentials & provider.inputs.keys()
    assert instance.provider.rsplit("::", 1)[1] == "aws-id"  # the resource uses the explicit provider


def test_timeouts_tags_and_the_secret_password(spec, mocks, monkeypatch):
    options = []
    real = aws.aws.rds.Instance

    def spy(*args, opts, **kwargs):
        options.append(opts)
        return real(*args, opts=opts, **kwargs)

    monkeypatch.setattr(aws.aws.rds, "Instance", spy)
    *_, instance, _ = provisioned(make_spec(spec), mocks)
    assert (options[0].custom_timeouts.create, options[0].custom_timeouts.delete) == ("50m", "100m")
    assert instance.inputs["tags"]["sc_inspector_run"] == "r20261120t1000-aaaa"
    assert instance.inputs["tags"]["sc_inspector_expires"] == "20261120t1600z"
    assert instance.inputs["password"] != "s3cret-pass"  # the secret marker, not the plain value


def test_a_zone_id_is_not_used_as_the_availability_zone(spec, mocks):
    *_, instance, _ = provisioned(make_spec(spec, zone="use1-az2"), mocks)
    assert "availabilityZone" not in instance.inputs
    mocks.resources.clear()
    *_, instance, _ = provisioned(make_spec(spec, zone=None), mocks)
    assert "availabilityZone" not in instance.inputs


def test_a_small_database_gets_a_small_volume_with_the_fixed_baseline(spec):
    # 8 vCPU: 200 IOPS and 50 MiB/s targets in 1000 / 50 floors, under the 3000 / 125 baseline
    assert aws.rds_storage(make_spec(spec)) == (64, None, None)


def test_a_database_above_the_small_baseline_gets_400_gib(spec):
    big = make_spec(spec, record={"vcpus": 160, "memory_amount": 1024 * 1024})  # 4000 IOPS, 240 MiB/s
    gib, iops, mbps = aws.rds_storage(big)
    assert (gib, iops, mbps) == (400, None, None)


def test_a_database_above_the_large_baseline_provisions_iops_and_throughput(spec):
    huge = make_spec(spec, record={"vcpus": 512, "memory_amount": 1024 * 1024})  # 12800 IOPS, 768 MiB/s
    assert aws.rds_storage(huge) == (400, 12800, 768)


@pytest.mark.parametrize(
    ("run", "expected"),
    [
        ("aws-db-r6g-large-1", "sc-aws-db-r6g-large-1"),
        ("AWS_x..y--z-", "sc-aws-x-y-z"),
    ],
)
def test_identifiers_are_valid_and_deterministic(run, expected):
    assert aws.identifier(run) == expected


def test_a_long_run_name_keeps_a_hash_so_two_runs_do_not_collide():
    a, b = aws.identifier("x" * 80 + "a"), aws.identifier("x" * 80 + "b")
    assert len(a) <= 63 and len(b) <= 63 and a != b and a == aws.identifier("x" * 80 + "a")


def test_inventory_lists_the_tagged_instances_of_every_region(monkeypatch):
    created = datetime(2026, 11, 20, 10, 5, 3, tzinfo=UTC)
    tagged = {
        "DBInstanceArn": "arn:aws:rds:eu-west-1:1:db:sc-x",
        "InstanceCreateTime": created,
        "TagList": [
            {"Key": "sc_inspector_run", "Value": "r1"},
            {"Key": "sc_inspector_expires", "Value": "20261120t1600z"},
        ],
    }
    other = {"DBInstanceArn": "arn:aws:rds:eu-west-1:1:db:other", "TagList": [{"Key": "team", "Value": "x"}]}

    class Paginator:
        def __init__(self, region):
            self.region = region

        def paginate(self):
            return [{"DBInstances": [tagged, other] if self.region == "eu-west-1" else [other]}]

    class Client:
        def __init__(self, service, region):
            self.service, self.region = service, region

        def describe_regions(self):
            return {"Regions": [{"RegionName": "eu-west-1"}, {"RegionName": "us-east-1"}]}

        def get_paginator(self, name):
            assert name == "describe_db_instances"
            return Paginator(self.region)

    class Session:
        def __init__(self, region_name):
            pass

        def client(self, service, region_name="us-east-1"):
            return Client(service, region_name)

    monkeypatch.setattr(aws.boto3.session, "Session", Session)
    assert aws.inventory() == [
        {"id": "arn:aws:rds:eu-west-1:1:db:sc-x", "run": "r1", "expires_at": "2026-11-20T16:00:00Z",
         "created_at": "2026-11-20T10:05:03Z"}
    ]  # fmt: skip
