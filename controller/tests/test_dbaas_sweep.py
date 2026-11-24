"""Stack naming and state layout, the Jobs' shape, the sweeper of leaked stacks and the metric."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fakes import Clock, FakeKube, FakeStorage
from test_dbaas_plan import NOW, make_config

from k8s_inspector import crd, dbaas_jobs, dbaas_sweep
from k8s_inspector.metrics import Metrics
from k8s_inspector.model import DbaasVendor, DbPhase, DbStatus, RunSpec, VendorSettings

STACKS = "dbaas-state/.pulumi/stacks/k8s-inspector-dbaas"


# --------------------------------------------------------------------------- names and layout


def test_the_stack_follows_from_the_run_name():
    assert dbaas_jobs.stack_name("aws-t3-medium-6abacd9b") == "k8s-dbaas-aws-t3-medium-6abacd9b"
    assert dbaas_jobs.up_job_name("aws-x-1") == "dbaas-up-aws-x-1"
    assert dbaas_jobs.down_job_name("aws-x-1") == "dbaas-down-aws-x-1"
    assert dbaas_jobs.db_secret_name("aws-x-1") == "aws-x-1-db"
    assert dbaas_jobs.vendor_of_run("upcloud-2xcpu-4gb-1234abcd", ["aws", "upcloud"]) == "upcloud"
    assert (
        dbaas_jobs.vendor_of_run("my-cloud-x-1234abcd", ["my", "my-cloud"]) == "my-cloud"
    )  # a dash in the id, longest
    assert dbaas_jobs.vendor_of_run("other-x-1234abcd", ["aws"]) is None


def test_the_stack_prefix_is_the_provisioners():
    source = (Path(__file__).parents[2] / "dbaas/k8s_inspector_dbaas/stack.py").read_text()
    assert f'STACK_PREFIX = "{dbaas_jobs.STACK_PREFIX}"' in source


def test_long_run_names_give_short_distinct_job_names_and_the_run_stays_in_the_annotation():
    run = "x" * 63
    assert len(dbaas_jobs.down_job_name(run)) <= 63 and dbaas_jobs.down_job_name(run) != dbaas_jobs.down_job_name(
        run[:-1]
    )
    job = dbaas_jobs.down_job(run, "aws", make_config())
    assert job["metadata"]["annotations"][dbaas_jobs.ANNOTATION_RUN] == run
    assert job["spec"]["template"]["metadata"]["labels"][dbaas_jobs.LABEL_JOB] == job["metadata"]["name"]


def test_a_stack_file_gives_back_its_run_and_nothing_else_does():
    run = "aws-t3-medium-6abacd9b"
    assert dbaas_jobs.run_of_state_key(dbaas_jobs.stack_state_key(run)) == run
    for key in (
        f"{STACKS}/k8s-dbaas-{run}.json.bak",
        f"{STACKS}/k8s-dbaas-{run}.json.attrs",
        f"dbaas-state/.pulumi/backups/k8s-inspector-dbaas/k8s-dbaas-{run}/k8s-dbaas-{run}.1.json",
        f"dbaas-state/.pulumi/history/k8s-inspector-dbaas/k8s-dbaas-{run}/x.json",
        "dbaas-state/.pulumi/meta.yaml",
        f"{STACKS}/other-{run}.json",
        f"{STACKS}/k8s-dbaas-.json",
    ):
        assert dbaas_jobs.run_of_state_key(key) is None, key


def test_what_pulumi_leaves_behind_is_found_by_prefix():
    prefixes = dbaas_jobs.state_leftover_prefixes("aws-x-1")
    assert prefixes == [
        "dbaas-state/.pulumi/backups/k8s-inspector-dbaas/k8s-dbaas-aws-x-1/",
        "dbaas-state/.pulumi/history/k8s-inspector-dbaas/k8s-dbaas-aws-x-1/",
        f"{STACKS}/k8s-dbaas-aws-x-1.json",
    ]
    storage = FakeStorage()
    mine = [
        f"{prefixes[0]}k8s-dbaas-aws-x-1.1.json",
        f"{prefixes[1]}k8s-dbaas-aws-x-1-1.checkpoint.json",
        f"{STACKS}/k8s-dbaas-aws-x-1.json.bak",
    ]
    others = [f"{STACKS}/k8s-dbaas-aws-x-12.json", "dbaas-state/.pulumi/meta.yaml"]
    storage.objects = dict.fromkeys([*mine, *others], b"")
    dbaas_jobs.clean_state(storage, "aws-x-1")
    assert sorted(storage.objects) == sorted(others)  # `-12` is another run: the prefix ends at `.json`, not at `-1`...


def test_the_state_backend_is_the_controllers_bucket_below_its_key_prefix():
    env = make_config().env
    assert dbaas_jobs.backend_url(env) == "s3://b/dbaas-state?region=r&awssdk=v2"
    prefixed = EnvConfigWith(key_prefix="k8s-inspector/spikes/", s3_endpoint_url="https://s3.example.com")
    assert dbaas_jobs.backend_url(prefixed) == (
        "s3://b/k8s-inspector/spikes/dbaas-state?region=r&awssdk=v2&endpoint=https%3A%2F%2Fs3.example.com"
        "&use_path_style=true"
    )
    with_role = EnvConfigWith(dbaas_role_arn="arn:aws:iam::1:role/state")
    assert dbaas_jobs.backend_url(with_role) == "s3://b/dbaas-state?region=r&awssdk=v2&profile=k8si-state"
    assert dbaas_jobs.state_aws_config(env) is None  # no role, no profile


def EnvConfigWith(**fields):
    from dataclasses import replace

    return replace(make_config().env, **fields)


def test_the_web_identity_token_is_mounted_in_every_job_kind_but_the_state_env_only_where_there_is_state():
    cfg = make_sweep_config()
    cfg = type(cfg)(EnvConfigWith(dbaas_role_arn="arn:aws:iam::1:role/state"), cfg.settings)
    inventory = dbaas_jobs.inventory_job("aws", cfg)
    down = dbaas_jobs.down_job("aws-x-1", "aws", cfg)
    for job in (inventory, down):
        pod = job["spec"]["template"]["spec"]
        assert [v["name"] for v in pod["volumes"] if v["name"] == "aws-token"] == ["aws-token"]
        assert {"name": "aws-token", "mountPath": dbaas_jobs.AWS_TOKEN_DIR, "readOnly": True} in (
            pod["containers"][0]["volumeMounts"]
        )
    env = lambda job: {e["name"] for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}  # noqa: E731
    assert "PULUMI_BACKEND_URL" in env(down) and "PULUMI_BACKEND_URL" not in env(inventory)


def test_a_stack_is_as_old_as_its_first_update_whatever_its_state_file_was_last_changed(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 1)  # a destroy that keeps failing rewrites the state file
    history = dbaas_jobs.state_history_prefix("aws-leak-1")
    for name, hours in (("a", 9), ("b", 2)):
        storage.objects[f"{history}{name}.json"] = b"{}"
        storage.modified[f"{history}{name}.json"] = NOW - timedelta(hours=hours)
    (seen,) = dbaas_sweep.stacks(kube, make_sweep_config(), storage)
    assert seen.created == NOW - timedelta(hours=9)
    dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]  # past the 6 h from its creation


def test_the_client_addresses_are_the_external_ips_of_the_node_as_host_networks():
    node = {
        "status": {
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.4"},
                {"type": "ExternalIP", "address": "203.0.113.7"},
                {"type": "ExternalIP", "address": "2001:db8::7"},
                {"type": "Hostname", "address": "node-1"},
            ]
        }
    }
    assert dbaas_jobs.client_addresses(node) == ["203.0.113.7/32", "2001:db8::7/128"]
    assert dbaas_jobs.client_addresses({"status": {}}) == [] and dbaas_jobs.client_addresses({}) == []


def test_every_database_is_labelled_with_the_run_and_an_expiry():
    expires = datetime(2026, 11, 21, 9, 5, tzinfo=UTC)
    labels = dbaas_jobs.labels("r20261120T1000-AAAA", expires)
    assert labels == {"sc_inspector_run": "r20261120t1000-aaaa", "sc_inspector_expires": "20261121t0905z"}
    from k8s_inspector.tasks import get_task

    cfg = make_config()
    # creation (60) + bootstrap (75) + benchmark (120) + a destroy attempt (60): 5 h 15 min from now
    assert dbaas_jobs.expiry(NOW, cfg, get_task("pgbench_postgres_dbaas_ro_durable")) == NOW + timedelta(minutes=315)


def test_the_connection_secret_carries_the_password_first_and_the_facts_later():
    from k8s_inspector.model import BenchmarkRun, OwnerRef, RunStatus

    run = BenchmarkRun("aws-x-1", "inspector", "u", "1", NOW, RunSpec("aws", "x", "r"), RunStatus())
    owner = OwnerRef("v1", "BenchmarkRun", "aws-x-1", "u")
    assert dbaas_jobs.connection_secret(run, owner, "pw", None)["stringData"] == {"K8SI_RUN_PASSWORD": "pw"}
    gcp = {
        "host": "h",
        "admin_user": "postgres",
        "admin_db": "postgres",
        "user": "scadmin",
        "grant_role": "cloudsqlsuperuser",
    }
    data = dbaas_jobs.connection_secret(run, owner, "pw", gcp)["stringData"]
    assert (data["SC_DB_USER"], data["SC_DB_BOOTSTRAP_USER"], data["SC_DB_GRANT_ROLE"]) == (
        "scadmin", "postgres", "cloudsqlsuperuser",
    )  # fmt: skip
    assert data["SC_DB_SSLMODE"] == data["PGSSLMODE"] == "require" and data["SC_DB_PORT"] == "5432"


# --------------------------------------------------------------------------- the sweeper


def make_sweep_config(*, dbaas_vendors=("oci", "aws")):
    """oci (as in the planner tests) and aws, both known to the ConfigMap; `dbaas_vendors` have a `dbaas` section."""
    cfg = make_config()
    settings = replace(
        cfg.settings,
        vendors={**cfg.settings.vendors, "aws": VendorSettings(enabled=True)},
        dbaas=replace(cfg.settings.dbaas, vendors={v: DbaasVendor() for v in dbaas_vendors}),
    )
    return type(cfg)(cfg.env, settings)


@pytest.fixture
def kube() -> FakeKube:
    return FakeKube(Clock(NOW))


def stack(storage, run, age_hours):
    key = dbaas_jobs.stack_state_key(run)
    storage.objects[key] = b"{}"
    storage.modified[key] = NOW - timedelta(hours=age_hours)


def jobs(kube):
    return sorted(j["metadata"]["name"] for j in kube.list("job"))


def test_a_stack_without_a_run_that_is_old_enough_is_destroyed(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 7)
    seen = dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    assert [(s.run, s.vendor, s.orphan) for s in seen] == [("aws-leak-1", "aws", True)]
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]
    (job,) = kube.list("job")
    assert job["metadata"]["labels"]["inspector.sparecores.com/dbaas-sweep"] == "true"
    assert "ownerReferences" not in job["metadata"] and "inspector.sparecores.com/run" not in job["metadata"]["labels"]
    dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]  # not a second one


def test_a_young_stack_or_one_with_a_run_is_left_alone(kube):
    storage = FakeStorage()
    stack(storage, "aws-young-1", 2)
    run = crd.create_run(kube, RunSpec("oci", "x", "r1", database={"vendor": "oci", "id": "d", "engineVersion": "18"}))
    stack(storage, run.name, 30)  # old, but its run exists: the runner destroys it
    seen = dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    assert sorted((s.run, s.orphan) for s in seen) == sorted([("aws-young-1", True), (run.name, False)])
    assert jobs(kube) == []


def test_the_sweep_threshold_is_configurable(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 2)
    cfg = make_sweep_config()
    cfg = type(cfg)(cfg.env, replace(cfg.settings, dbaas=replace(cfg.settings.dbaas, sweep_after_hours=1)))
    dbaas_sweep.sweep(kube, cfg, storage, NOW)
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]


def test_a_finished_sweep_job_is_removed_with_what_it_left_and_a_failed_one_is_retried(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 7)
    dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    kube.set_status("job", "dbaas-down-aws-leak-1", {"conditions": [{"type": "Failed", "status": "True"}]})
    dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)  # deleted ...
    assert ("job", "dbaas-down-aws-leak-1") in kube.deleted
    assert jobs(kube) == []
    dbaas_sweep.sweep(
        kube, make_sweep_config(), storage, NOW
    )  # the next cycle creates it again: the stack is still there
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]
    storage.objects.pop(dbaas_jobs.stack_state_key("aws-leak-1"))  # the destroy removed the stack
    storage.objects[f"{STACKS}/k8s-dbaas-aws-leak-1.json.bak"] = b""
    kube.set_status("job", "dbaas-down-aws-leak-1", {"conditions": [{"type": "Complete", "status": "True"}]})
    assert dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW) == []
    assert jobs(kube) == [] and storage.objects == {}


def test_the_sweeper_sees_the_stacks_when_the_dbaas_vendors_section_is_empty_or_a_vendor_was_removed(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 7)
    cfg = make_sweep_config(dbaas_vendors=())  # `dbaas.vendors` gone, the vendor itself is still in the ConfigMap
    (seen,) = dbaas_sweep.sweep(kube, cfg, storage, NOW)
    assert (seen.run, seen.vendor, seen.orphan) == ("aws-leak-1", "aws", True)
    (job,) = kube.list("job")  # destroyed with the vendor's ServiceAccount and Secret by convention
    pod = job["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "k8s-inspector-dbaas"
    assert pod["containers"][0]["envFrom"] == [{"secretRef": {"name": "k8s-inspector-dbaas-aws", "optional": True}}]


def test_a_stack_of_a_vendor_the_configuration_does_not_know_is_reported_not_destroyed(kube, caplog):
    storage = FakeStorage()
    stack(storage, "gone-cloud-leak-1", 30)
    (seen,) = dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    assert (seen.run, seen.vendor, seen.orphan) == ("gone-cloud-leak-1", None, True)
    assert jobs(kube) == [] and "no configured vendor owns it" in caplog.text
    metrics = Metrics(kube, lambda: make_sweep_config().settings, clock=lambda: NOW)
    metrics.record_dbaas_stacks([seen])
    assert 'k8s_inspector_dbaas_instances{vendor="unknown"} 1' in metrics.render()
    assert 'k8s_inspector_dbaas_oldest_age_seconds{vendor="unknown"} 108000' in metrics.render()


def test_a_finished_down_job_of_a_vanished_run_is_cleaned_up_and_one_of_a_live_run_is_not(kube):
    storage = FakeStorage()
    live = crd.create_run(kube, RunSpec("oci", "x", "r1", database={"vendor": "oci", "id": "d", "engineVersion": "18"}))
    kube.create("job", dbaas_jobs.down_job(live.name, "oci", make_sweep_config()))
    kube.create("job", dbaas_jobs.down_job("oci-vanished-1", "oci", make_sweep_config()))  # labelled with its run
    done = {"conditions": [{"type": "Complete", "status": "True"}]}
    kube.set_status("job", dbaas_jobs.down_job_name(live.name), done)
    kube.set_status("job", dbaas_jobs.down_job_name("oci-vanished-1"), done)
    storage.objects[f"{STACKS}/k8s-dbaas-oci-vanished-1.json.bak"] = b""
    dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW)
    assert jobs(kube) == [dbaas_jobs.down_job_name(live.name)] and storage.objects == {}


# --------------------------------------------------------------------------- the inventory of tagged databases


def finish(kube, name, message, outcome="Complete"):
    kube.set_status("job", name, {"conditions": [{"type": outcome, "status": "True"}]})
    labels = {dbaas_jobs.LABEL_JOB: name}
    status = {"containerStatuses": [{"state": {"terminated": {"message": json.dumps(message)}}}]}
    kube.create("pod", {"metadata": {"name": f"{name}-pod", "labels": labels}, "spec": {}, "status": status})


def test_an_inventory_job_runs_per_configured_vendor_with_its_credentials_and_no_state(kube):
    started = {}
    assert dbaas_sweep.inventory(kube, make_sweep_config(), NOW, started) == {}
    assert jobs(kube) == ["dbaas-inventory-aws", "dbaas-inventory-oci"]
    job = kube.get("job", "dbaas-inventory-aws")
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["args"] == ["inventory"] and container["env"] == [
        {"name": "DBAAS_VENDOR", "value": "aws"},
        {"name": "DBAAS_REGIONS", "value": ""},
    ]
    assert container["envFrom"] == [{"secretRef": {"name": "k8s-inspector-dbaas-aws", "optional": True}}]
    assert job["spec"]["template"]["metadata"]["annotations"] == {"karpenter.sh/do-not-disrupt": "true"}
    assert job["spec"]["activeDeadlineSeconds"] == 900
    dbaas_sweep.inventory(kube, make_sweep_config(), NOW, started)
    assert len(jobs(kube)) == 2  # not again while they run


def test_the_inventory_result_is_collected_and_the_next_job_starts_after_the_interval(kube):
    started = {}
    cfg = make_sweep_config(dbaas_vendors=("aws",))
    dbaas_sweep.inventory(kube, cfg, NOW, started)
    message = {"count": 2, "oldest_created_at": "2026-11-20T01:00:00Z", "databases": []}
    finish(kube, "dbaas-inventory-aws", message)
    found = dbaas_sweep.inventory(kube, cfg, NOW + timedelta(minutes=1), started)
    at = NOW + timedelta(minutes=1)
    assert found == {"aws": dbaas_sweep.Tagged(2, datetime(2026, 11, 20, 1, 0, tzinfo=UTC), at)}
    assert jobs(kube) == []
    dbaas_sweep.inventory(kube, cfg, NOW + timedelta(minutes=20), started)
    assert jobs(kube) == []  # too early
    dbaas_sweep.inventory(kube, cfg, NOW + timedelta(minutes=31), started)
    assert jobs(kube) == ["dbaas-inventory-aws"]


def test_a_failed_inventory_is_logged_and_tried_again_later(kube, caplog):
    started = {}
    cfg = make_sweep_config(dbaas_vendors=("aws",))
    dbaas_sweep.inventory(kube, cfg, NOW, started)
    finish(kube, "dbaas-inventory-aws", {"error": "no credentials"}, outcome="Failed")
    assert dbaas_sweep.inventory(kube, cfg, NOW + timedelta(minutes=1), started) == {"aws": None}
    assert "inventory of vendor aws failed: no credentials" in caplog.text and jobs(kube) == []


@pytest.mark.parametrize("message", [{"count": 1, "oldest_created_at": "yesterday"}, {"count": "many"}])
def test_an_unreadable_inventory_counts_as_failed_and_its_job_is_still_deleted(kube, message):
    started = {}
    cfg = make_sweep_config(dbaas_vendors=("aws",))
    dbaas_sweep.inventory(kube, cfg, NOW, started)
    finish(kube, "dbaas-inventory-aws", message)
    assert dbaas_sweep.inventory(kube, cfg, NOW + timedelta(minutes=1), started) == {"aws": None}
    assert jobs(kube) == []


def test_a_naive_timestamp_of_the_inventory_is_utc(kube):
    started = {}
    cfg = make_sweep_config(dbaas_vendors=("aws",))
    dbaas_sweep.inventory(kube, cfg, NOW, started)
    finish(kube, "dbaas-inventory-aws", {"count": 1, "oldest_created_at": "2026-11-20T01:00:00"})
    found = dbaas_sweep.inventory(kube, cfg, NOW, started)
    assert found["aws"].oldest_created == datetime(2026, 11, 20, 1, 0, tzinfo=UTC)


def test_a_failed_inventory_drops_the_last_result_and_the_metric_tells_the_last_success(kube):
    settings = make_sweep_config().settings
    metrics = Metrics(kube, lambda: settings, clock=lambda: NOW)
    good = dbaas_sweep.Tagged(2, NOW - timedelta(hours=1), NOW - timedelta(hours=3))
    metrics.record_dbaas_inventory({"aws": good}, {"aws": good.at})
    text = metrics.render()
    assert (
        f'k8s_inspector_dbaas_inventory_last_success_timestamp_seconds{{vendor="aws"}} {good.at.timestamp():g}' in text
    )
    assert (
        f'k8s_inspector_dbaas_inventory_last_success_timestamp_seconds{{vendor="oci"}} {NOW.timestamp():g}' in text
    )  # never worked: the process start
    metrics.record_dbaas_inventory({}, {"aws": good.at})  # the latest one failed: no stale count
    text = metrics.render()
    assert "dbaas_tagged_databases{" not in text
    assert f'timestamp_seconds{{vendor="aws"}} {good.at.timestamp():g}' in text


def test_the_metrics_export_the_tagged_databases_and_the_age_of_the_oldest(kube):
    settings = make_sweep_config().settings
    metrics = Metrics(kube, lambda: settings, clock=lambda: NOW + timedelta(hours=2))
    metrics.record_dbaas_inventory(
        {
            "aws": dbaas_sweep.Tagged(2, NOW - timedelta(hours=7), NOW),
            "oci": dbaas_sweep.Tagged(0, None, NOW),
        },
        {},
    )
    text = metrics.render()
    assert 'k8s_inspector_dbaas_tagged_databases{vendor="aws"} 2' in text
    assert (
        'k8s_inspector_dbaas_tagged_oldest_age_seconds{vendor="aws"} 32400' in text
    )  # 7 h + 2 h: the age keeps growing
    assert 'k8s_inspector_dbaas_tagged_databases{vendor="oci"} 0' in text
    assert 'k8s_inspector_dbaas_tagged_oldest_age_seconds{vendor="oci"} 0' in text


# --------------------------------------------------------------------------- the metric


def test_the_metric_counts_databases_that_exist_and_the_age_of_the_oldest(kube, monkeypatch):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 9)
    for name, phase in (("a", DbPhase.READY), ("b", DbPhase.DESTROYED)):
        run = crd.create_run(
            kube, RunSpec("oci", name, "r", database={"vendor": "oci", "id": "d", "engineVersion": "18"})
        )
        crd.update_status(kube, run.name, lambda s, phase=phase: setattr(s, "database", DbStatus(phase, NOW, NOW)))
    settings = make_config().settings
    metrics = Metrics(kube, lambda: settings, clock=lambda: NOW + timedelta(hours=1))
    metrics.record_dbaas_stacks(dbaas_sweep.sweep(kube, make_sweep_config(), storage, NOW))
    text = metrics.render()
    assert 'k8s_inspector_dbaas_instances{vendor="aws"} 1' in text  # the leaked one
    assert 'k8s_inspector_dbaas_oldest_age_seconds{vendor="aws"} 36000' in text  # 9 h + 1 h
    assert 'k8s_inspector_dbaas_instances{vendor="oci"} 1' in text  # `a`; `b` is destroyed
    assert 'k8s_inspector_dbaas_oldest_age_seconds{vendor="oci"}' in text


def test_the_inventory_job_gets_the_sorted_configured_regions_of_the_vendor():
    cfg = make_sweep_config()
    networks = {"us-east-1": {}, "eu-central-1": {}}
    dbaas = replace(cfg.settings.dbaas, vendors={"aws": DbaasVendor(networks=networks)})
    cfg = type(cfg)(cfg.env, replace(cfg.settings, dbaas=dbaas))
    env = dbaas_jobs.inventory_job("aws", cfg)["spec"]["template"]["spec"]["containers"][0]["env"]
    assert {"name": "DBAAS_REGIONS", "value": "eu-central-1,us-east-1"} in env
