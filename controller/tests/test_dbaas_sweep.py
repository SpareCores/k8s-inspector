"""Stack naming and state layout, the Jobs' shape, the sweeper of leaked stacks and the metric."""

from datetime import UTC, datetime, timedelta

import pytest
from fakes import Clock, FakeKube, FakeStorage
from test_dbaas_plan import NOW, make_config

from k8s_inspector import crd, dbaas_jobs, dbaas_sweep
from k8s_inspector.metrics import Metrics
from k8s_inspector.model import DbPhase, DbStatus, RunSpec, Settings

STACKS = "dbaas-state/.pulumi/stacks/k8s-inspector-dbaas"


# --------------------------------------------------------------------------- names and layout


def test_the_stack_follows_from_the_run_name():
    assert dbaas_jobs.stack_name("aws-t3-medium-6abacd9b") == "k8s-dbaas-aws-t3-medium-6abacd9b"
    assert dbaas_jobs.up_job_name("aws-x-1") == "dbaas-up-aws-x-1"
    assert dbaas_jobs.down_job_name("aws-x-1") == "dbaas-down-aws-x-1"
    assert dbaas_jobs.db_secret_name("aws-x-1") == "aws-x-1-db"
    assert dbaas_jobs.vendor_of_run("upcloud-2xcpu-4gb-1234abcd") == "upcloud"


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
    assert dbaas_jobs.backend_url(env) == "s3://b/dbaas-state?region=r"
    prefixed = EnvConfigWith(key_prefix="k8s-inspector/spikes/", s3_endpoint_url="https://s3.example.com")
    assert dbaas_jobs.backend_url(prefixed) == (
        "s3://b/k8s-inspector/spikes/dbaas-state?region=r&endpoint=https%3A%2F%2Fs3.example.com&s3ForcePathStyle=true"
    )


def EnvConfigWith(**fields):
    from dataclasses import replace

    return replace(make_config().env, **fields)


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
    assert dbaas_jobs.connection_secret(run, owner, "pw", None)["stringData"] == {"SC_DB_PASSWORD": "pw"}
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
    seen = dbaas_sweep.sweep(kube, make_config(), storage, NOW)
    assert [(s.run, s.vendor, s.orphan) for s in seen] == [("aws-leak-1", "aws", True)]
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]
    (job,) = kube.list("job")
    assert job["metadata"]["labels"]["inspector.sparecores.com/dbaas-sweep"] == "true"
    assert "ownerReferences" not in job["metadata"] and "inspector.sparecores.com/run" not in job["metadata"]["labels"]
    dbaas_sweep.sweep(kube, make_config(), storage, NOW)
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]  # not a second one


def test_a_young_stack_or_one_with_a_run_is_left_alone(kube):
    storage = FakeStorage()
    stack(storage, "aws-young-1", 2)
    run = crd.create_run(kube, RunSpec("oci", "x", "r1", database={"vendor": "oci", "id": "d", "engineVersion": "18"}))
    stack(storage, run.name, 30)  # old, but its run exists: the runner destroys it
    seen = dbaas_sweep.sweep(kube, make_config(), storage, NOW)
    assert sorted((s.run, s.orphan) for s in seen) == sorted([("aws-young-1", True), (run.name, False)])
    assert jobs(kube) == []


def test_the_sweep_threshold_is_configurable(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 2)
    cfg = make_config()
    from dataclasses import replace

    cfg = type(cfg)(cfg.env, replace(cfg.settings, dbaas=replace(cfg.settings.dbaas, sweep_after_hours=1)))
    dbaas_sweep.sweep(kube, cfg, storage, NOW)
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]


def test_a_finished_sweep_job_is_removed_with_what_it_left_and_a_failed_one_is_retried(kube):
    storage = FakeStorage()
    stack(storage, "aws-leak-1", 7)
    dbaas_sweep.sweep(kube, make_config(), storage, NOW)
    kube.set_status("job", "dbaas-down-aws-leak-1", {"conditions": [{"type": "Failed", "status": "True"}]})
    dbaas_sweep.sweep(kube, make_config(), storage, NOW)  # deleted ...
    assert ("job", "dbaas-down-aws-leak-1") in kube.deleted
    assert jobs(kube) == []
    dbaas_sweep.sweep(kube, make_config(), storage, NOW)  # the next cycle creates it again: the stack is still there
    assert jobs(kube) == ["dbaas-down-aws-leak-1"]
    storage.objects.pop(dbaas_jobs.stack_state_key("aws-leak-1"))  # the destroy removed the stack
    storage.objects[f"{STACKS}/k8s-dbaas-aws-leak-1.json.bak"] = b""
    kube.set_status("job", "dbaas-down-aws-leak-1", {"conditions": [{"type": "Complete", "status": "True"}]})
    assert dbaas_sweep.sweep(kube, make_config(), storage, NOW) == []
    assert jobs(kube) == [] and storage.objects == {}


def test_without_dbaas_configured_nothing_is_listed(kube):
    class Exploding(FakeStorage):
        def list_objects(self, prefix):
            raise AssertionError("the bucket must not be listed")

    cfg = make_config()
    from dataclasses import replace

    cfg = type(cfg)(cfg.env, replace(cfg.settings, dbaas=Settings().dbaas))
    assert dbaas_sweep.sweep(kube, cfg, Exploding(), NOW) == []


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
    metrics.record_dbaas_stacks(dbaas_sweep.sweep(kube, make_config(), storage, NOW))
    text = metrics.render()
    assert 'k8s_inspector_dbaas_instances{vendor="aws"} 1' in text  # the leaked one
    assert 'k8s_inspector_dbaas_oldest_age_seconds{vendor="aws"} 36000' in text  # 9 h + 1 h
    assert 'k8s_inspector_dbaas_instances{vendor="oci"} 1' in text  # `a`; `b` is destroyed
    assert 'k8s_inspector_dbaas_oldest_age_seconds{vendor="oci"}' in text
