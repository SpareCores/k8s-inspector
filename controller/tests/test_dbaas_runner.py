"""The database of a DBaaS run in the run state machine: create after the client node, bootstrap, benchmark, destroy in
parallel with the node release, retries, restarts, the finalizer. Jobs and Pods are played by `fakes.Cluster`."""

import base64
import json
import logging
import re
from dataclasses import replace
from datetime import timedelta

import pytest
from fakes import iso
from test_runner import START, Harness, _launch_failed, _unlaunched_claim, in_step

from k8s_inspector import crd, dbaas_client, dbaas_jobs, dbaas_keeper, launch, pods, runner_dbaas
from k8s_inspector import keeper as sc_keeper
from k8s_inspector.model import (
    CapacityMode,
    Config,
    DbaasSettings,
    DbaasVendor,
    DbPhase,
    EnvConfig,
    ExitCode,
    RunPhase,
    RunSpec,
    Settings,
    VendorSettings,
)
from k8s_inspector.storage import run_json_key

TASK = "pgbench_postgres_dbaas_ro_durable"
STEP = "pgbench-postgres-dbaas-ro-durable"
DATABASE = {"vendor": "oci", "id": "PostgreSQL.VM.E5.2", "engineVersion": "18", "region": "eu-test-1"}
DATABASE |= {"vcpus": 4, "memoryGib": 32.0}
RECORD = {"api_reference": "PostgreSQL.VM.E5.2", "vcpus": 4, "memory_amount": 32768}
CLIENT = "VM.Standard.E5.Flex.2-8"
STATE_KEY = f"dbaas-state/.pulumi/stacks/k8s-inspector-dbaas/k8s-dbaas-{crd.run_name('oci', DATABASE['id'])}.json"


def make_dbaas_config(**dbaas) -> Config:
    env = EnvConfig(
        bucket="b",
        bucket_region="eu-west-1",
        role_arn="arn:aws:iam::1:role/x",
        sc_run_image="ghcr.io/sparecores/sc-run@sha256:abc",
        dbaas_image="ghcr.io/sparecores/k8s-inspector-dbaas@sha256:def",
    )
    vendors = {"oci": DbaasVendor(networks={"eu-test-1": {"vpc": "vcn-1", "subnet": "sn-1"}})}
    return Config(
        env=env,
        settings=Settings(vendors={"oci": VendorSettings(enabled=True)}, dbaas=DbaasSettings(vendors=vendors, **dbaas)),
    )


class DbHarness(Harness):
    def start_db(self, **spec) -> None:
        spec = RunSpec(
            vendor="oci",
            sku=DATABASE["id"],
            run_id="r20261120t1000-aaaa",
            tasks=[TASK],
            capacity=CapacityMode.ON_DEMAND,
            database=dict(DATABASE),
            **spec,
        )
        self.name = crd.create_run(self.kube, spec).name

    @property
    def db(self):
        return self.run.status.database

    def run_pods(self) -> list[str]:
        """The run's own Pods: the Pods of the provisioner Jobs are not among them."""
        return [n for n in self.pod_names() if not n.endswith("-pod")]

    def jobs(self) -> list[str]:
        return sorted(j["metadata"]["name"] for j in self.kube.list("job"))

    def secret(self, name: str) -> dict:
        data = self.kube.get("secret", name)["data"]
        return {k: base64.b64decode(v).decode() for k, v in data.items()}

    def until_phase(self, phase, **kw):
        return self.drive(stop=lambda run: run.status.database is not None and run.status.database.phase == phase, **kw)

    def run_exists(self) -> bool:
        return self.name in self.kube.objects["benchmarkrun"]


@pytest.fixture(autouse=True)
def keeper(monkeypatch):
    monkeypatch.setattr(dbaas_keeper, "database", lambda env, vendor, database_id: dict(RECORD))


@pytest.fixture
def h() -> DbHarness:
    return DbHarness(make_dbaas_config())


# --------------------------------------------------------------------------- the whole way


def test_a_dbaas_run_is_not_dropped_when_keeper_does_not_list_its_client(h, monkeypatch):
    asks = []
    monkeypatch.setattr(sc_keeper, "active_servers_block_reasons", lambda env, names: asks.append(names) or {})
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.PROVISIONING)
    h.clock.advance(minutes=11)
    h.runner.reconcile(h.name)
    assert h.run_exists() and h.run.status.phase == RunPhase.PROVISIONING and asks == []


def test_the_client_node_comes_first_and_the_database_is_created_when_the_anchor_runs(h):
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.PROVISIONING)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    assert anchor["spec"]["nodeSelector"]["topology.kubernetes.io/region"] == "eu-test-1"  # the database's region
    assert h.jobs() == [] and h.db is None  # no node, nothing that bills by the hour
    assert anchor["spec"]["nodeSelector"]["inspector.sparecores.com/run"] == h.name  # a node of its own
    assert "scarpenter.sparecores.com/api-reference" not in anchor["spec"]["nodeSelector"]
    terms = anchor["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
    assert terms["nodeSelectorTerms"][0]["matchExpressions"][1:] == dbaas_client.requirements()  # any small x86_64

    h.until_phase(DbPhase.CREATING)
    assert h.jobs() == [f"dbaas-up-{h.name}"]
    (job,) = h.kube.list("job")
    env = {e["name"]: e for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    spec = json.loads(env["DBAAS_SPEC"]["value"])
    assert spec["databaseId"] == DATABASE["id"] and spec["region"] == "eu-test-1" and spec["zone"] == "eu-test-1a"
    assert spec["network"] == {"vpc": "vcn-1", "subnet": "sn-1"} and spec["record"] == RECORD
    assert spec["labels"]["sc_inspector_run"] == "r20261120t1000-aaaa"
    assert spec["labels"]["sc_inspector_expires"].endswith("z") and spec["expiresAt"].endswith("Z")
    assert env["DBAAS_PASSWORD"]["valueFrom"]["secretKeyRef"] == {"name": f"{h.name}-db", "key": "K8SI_RUN_PASSWORD"}
    assert "Sc1-" in h.secret(f"{h.name}-db")["K8SI_RUN_PASSWORD"] and "DBAAS_PASSWORD" not in job["spec"]["template"][
        "spec"
    ]["containers"][0]["env"][0].get("value", "")
    assert job["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
    assert job["spec"]["backoffLimit"] == 0 and job["spec"]["activeDeadlineSeconds"] == 3600
    assert job["metadata"]["ownerReferences"][0]["name"] == h.name
    assert h.db.expires > START + timedelta(hours=3)


def test_a_dbaas_run_creates_bootstraps_benchmarks_and_destroys(h):
    h.start_db()
    h.until_phase(DbPhase.BOOTSTRAPPING)
    assert h.db.timing.keys() == {"api_start", "api_end"} and h.db.timing["api_end"] == h.db.since
    facts = h.secret(f"{h.name}-db")  # the Job's connection facts, written by the controller
    assert (facts["SC_DB_HOST"], facts["SC_DB_PORT"], facts["SC_DB_USER"], facts["SC_DB_NAME"]) == (
        "10.0.0.5", "5432", "scadmin", "bench",
    )  # fmt: skip
    assert (facts["SC_DB_BOOTSTRAP_USER"], facts["SC_DB_BOOTSTRAP_DATABASE"], facts["PGSSLMODE"]) == (
        "scadmin", "postgres", "require",
    )  # fmt: skip
    boot = h.kube.get("pod", dbaas_jobs.boot_pod_name(h.name, 1))
    assert boot["spec"]["nodeName"] == f"node-{h.name}-a1" and boot["spec"]["hostNetwork"] is True
    assert boot["spec"]["containers"][0]["command"] == ["python3", "/dbaas/bootstrap.py"]
    assert boot["spec"]["containers"][0]["envFrom"] == [{"secretRef": {"name": f"{h.name}-db"}}]
    assert boot["spec"]["containers"][0]["image"].startswith(
        "ghcr.io/sparecores/benchmark-pgbench-postgres:main@sha256:"
    )
    assert "def run(" in h.kube.get("configmap", f"{h.name}-boot")["data"]["bootstrap.py"]

    h.drive(stop=in_step(STEP))
    assert h.db.phase == DbPhase.READY and dbaas_jobs.boot_pod_name(h.name, 1) not in h.pod_names()
    step = h.kube.get("pod", f"{h.name}-a1-01-{STEP}")  # the only step: no hardware probes of the client node
    container = step["spec"]["containers"][0]
    assert container["envFrom"] == [{"secretRef": {"name": f"{h.name}-db"}}]
    env = {e["name"]: e["value"] for e in container["env"] if "value" in e}
    assert env["SC_DB_VCPUS"] == "4" and env["SC_DB_MEM_GIB"] == "32.0" and env["SC_CLIENT_VCPUS"] == "2"
    assert (
        env["SC_PROFILE_VUS"] == "1,2,4,8" and env["SC_WORKLOAD"] == "pgbench_ro" and env["SC_DURABILITY"] == "durable"
    )
    assert "/k8si/dbaas/postgres.json:postgres.json" in container["command"]
    assert h.step_pods() == [f"1-01-{STEP}"] and "lscpu" not in h.codes()

    h.storage.objects[f"dbaas-state/.pulumi/backups/k8s-inspector-dbaas/k8s-dbaas-{h.name}/x.json"] = b"old"
    h.storage.objects["dbaas-state/.pulumi/stacks/k8s-inspector-dbaas/k8s-dbaas-other-1.json.bak"] = b"other"
    run = h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    assert h.codes()[TASK] == (0, None)
    # the same pass released the node and started the destroy; creating the database stopped
    assert h.run_pods() == [] and h.db.phase == DbPhase.DESTROYING
    assert h.db.timing.keys() == {"api_start", "api_end", "api_destroy_start"}
    assert h.jobs() == [f"dbaas-down-{h.name}"]
    (down,) = h.kube.list("job")
    assert down["spec"]["template"]["spec"]["containers"][0]["args"] == ["down"]
    env = {e["name"]: e.get("value") for e in down["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert (env["DBAAS_RUN"], env["DBAAS_VENDOR"]) == (h.name, "oci")
    assert run.status.attempts[0].outcome.value == "Completed"

    h.until_phase(DbPhase.DESTROYED)
    assert "api_destroy_end" in h.db.timing
    assert not h.kube.get("benchmarkrun", h.name)["metadata"].get("finalizers")
    assert ("secret", f"{h.name}-db") in h.kube.deleted and h.jobs() == []
    assert not [k for k in h.storage.objects if "backups" in k]  # what Pulumi leaves behind is removed
    assert "dbaas-state/.pulumi/stacks/k8s-inspector-dbaas/k8s-dbaas-other-1.json.bak" in h.storage.objects


def test_the_job_pods_are_not_the_runs_pods(h):
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    h.cluster.react()
    (pod,) = [p for p in h.kube.list("pod") if p["metadata"]["name"].endswith("-pod")]
    assert crd.LABEL_RUN not in pod["metadata"]["labels"]  # the sweep of the run's Pods never touches a Job's
    assert pods.LABEL_ROLE in pod["metadata"]["labels"]


# --------------------------------------------------------------------------- failures: the database is destroyed anyway


def test_a_failed_create_ends_the_task_minus_one_and_destroys_the_database(h):
    h.cluster.jobs["up"] = ["fail"]
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    code, message = h.codes()[TASK]
    assert code == ExitCode.NO_NODE and message == "DBAAS_CREATE_FAILED: quota exceeded"
    assert h.step_pods() == []
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.DESTROYING and "DBAAS_CREATE_FAILED" in h.db.error
    assert h.pod_names("anchor") == []  # the node is released while the database is destroyed
    h.until_phase(DbPhase.DESTROYED)
    assert not h.kube.get("benchmarkrun", h.name)["metadata"].get("finalizers")


def test_the_run_records_which_client_the_cluster_launched_and_run_json_names_it(h):
    h.cluster.sku = "VM.Standard.E4.Flex.2-8"  # whatever the cluster found cheapest
    h.start_db()
    h.until_phase(DbPhase.BOOTSTRAPPING)
    assert h.run.name == crd.run_name("oci", DATABASE["id"]) and h.run.status.client_sku == "VM.Standard.E4.Flex.2-8"
    assert h.run.node_sku == "VM.Standard.E4.Flex.2-8"
    run_json = json.loads(h.storage.read_bytes(run_json_key(crd.run_key(h.run))))
    assert run_json["sku"] == "VM.Standard.E4.Flex.2-8" and run_json["database"]["id"] == DATABASE["id"]


def test_an_unlaunched_claim_of_the_run_is_found_by_its_run_requirement(h):
    h.cluster.provision_enabled = False
    h.start_db()
    h.runner.reconcile(h.name)
    _unlaunched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim-1")  # requirements: the run, no SKU
    _unlaunched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "other")
    h.kube.merge_patch(
        "nodeclaim",
        "other",
        {
            "spec": {
                "requirements": [{"key": "inspector.sparecores.com/run", "operator": "In", "values": ["another-run"]}]
            }
        },
    )
    crd.request_cancel(h.kube, h.name)
    h.runner.reconcile(h.name)
    assert [c["metadata"]["name"] for c in h.kube.list("nodeclaim")] == ["other"]


def test_an_anchor_made_before_the_run_label_finds_its_claims_by_vendor_and_sku(h):
    h.cluster.provision_enabled = False
    h.start_db()
    h.runner.reconcile(h.name)
    anchor = f"{h.name}-a1"  # as the previous controller made it: the client SKU in the nodeSelector, no run label
    selector = {"inspector.sparecores.com/run": None, "scarpenter.sparecores.com/api-reference": CLIENT}
    h.kube.merge_patch("pod", anchor, {"spec": {"nodeSelector": selector}})
    _unlaunched_claim(h, h.kube.get("pod", anchor), "claim-1")
    _unlaunched_claim(h, h.kube.get("pod", anchor), "of-a-run-with-its-own-node")  # the same SKU, but another run's
    run_requirement = {"key": "inspector.sparecores.com/run", "operator": "In", "values": ["another-run"]}
    claim = h.kube.get("nodeclaim", "of-a-run-with-its-own-node")
    h.kube.merge_patch(
        "nodeclaim", "of-a-run-with-its-own-node",
        {"spec": {"requirements": [*claim["spec"]["requirements"][:-1], run_requirement]}},
    )  # fmt: skip
    _unlaunched_claim(h, h.kube.get("pod", anchor), "stamped")  # the NodePool's `Exists` gave it a generated label
    stamped = h.kube.get("nodeclaim", "stamped")
    exists = {"key": "inspector.sparecores.com/run", "operator": "Exists"}
    h.kube.merge_patch("nodeclaim", "stamped", {
        "metadata": {"labels": {"inspector.sparecores.com/run": "4711"}},
        "spec": {"requirements": [*stamped["spec"]["requirements"], exists]},
    })  # fmt: skip
    crd.request_cancel(h.kube, h.name)
    h.runner.reconcile(h.name)
    assert [c["metadata"]["name"] for c in h.kube.list("nodeclaim")] == ["of-a-run-with-its-own-node"]


@pytest.mark.parametrize("reason", ["NO_OFFERING", "ICE", "QUOTA", "DAILY_LIMIT", "BOOT_FAILURE", "LAUNCH_REJECTED"])
def test_a_dbaas_launch_failure_writes_no_per_sku_entry(h, reason):
    h.start_db()
    h.runner._fail_run(h.run, {}, launch.error_message(reason, "x"))
    assert h.run.status.phase == RunPhase.FAILED and h.codes()[TASK][1].startswith("DBAAS_CLIENT_FAILED: SCARPENTER_")
    assert launch.read(h.kube, "oci") == {}  # one entry per candidate would hold back server runs, or pause the vendor


def test_a_dbaas_client_gives_up_after_three_failed_claims(h):
    h.cluster.provision_enabled = False
    h.start_db()
    h.runner.reconcile(h.name)
    for n, claim in enumerate(("bench-aaaaa", "bench-bbbbb", "bench-ccccc"), 1):
        _launch_failed(h, claim, "Fatal (InvalidParameter): x")
        h.runner.reconcile(h.name)
        assert h.run.status.phase == (RunPhase.FAILED if n == 3 else RunPhase.PROVISIONING)  # a server run ends at 2
        h.kube.delete("nodeclaim", claim)
        h.clock.advance(minutes=5)


def test_a_dbaas_run_in_flight_without_a_client_sku_gets_it_from_its_node(h):
    h.start_db()
    h.until_phase(DbPhase.BOOTSTRAPPING)
    crd.update_status(h.kube, h.name, lambda s: setattr(s, "client_sku", None))
    h.runner._nodes.pop(h.name)  # as after a restart of the controller
    h.runner.reconcile(h.name)
    assert h.run.status.client_sku == "client-x"


def test_a_new_attempt_forgets_the_client_of_the_last_one(h):
    h.cluster.provision_enabled = False
    h.start_db()
    h.runner.reconcile(h.name)
    crd.update_status(h.kube, h.name, lambda s: (setattr(s, "phase", RunPhase.PENDING), setattr(s, "client_sku", "c1")))
    h.runner.reconcile(h.name)
    assert h.run.status.attempt == 2 and h.run.status.client_sku is None


def test_a_dbaas_run_without_an_offering_ends_with_its_result_and_defers_no_sku(h):
    h.cluster.provision_enabled = False
    h.start_db()
    h.runner.reconcile(h.name)
    condition = {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "no node matches"}
    h.kube.merge_patch("pod", f"{h.name}-a1", {"status": {"conditions": [condition]}})
    h.clock.advance(minutes=11)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED  # recorded, so that the cap bounds the retries
    code, message = h.codes()[TASK]
    assert code == ExitCode.NO_NODE and message.startswith("DBAAS_CLIENT_FAILED: SCARPENTER_NO_OFFERING")
    assert h.run.status.region == "eu-test-1"  # the next plan tries another region first
    assert launch.read(h.kube, "oci") == {}  # server runs of these SKUs are not held back by a DBaaS run


def test_a_job_that_cannot_work_in_the_region_ends_with_its_own_reason_and_the_clients_region(h):
    h.cluster.jobs["up"] = ["fail"]
    h.cluster.up_error = (
        "Program failed with an unhandled exception:\n"
        "RegionUnavailable: DBAAS_REGION_UNAVAILABLE: azure pre-flight: restricted in eastus\n"
        "more of the traceback"
    )
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    code, message = h.codes()[TASK]
    assert code == ExitCode.NO_NODE
    assert message == "DBAAS_REGION_UNAVAILABLE: azure pre-flight: restricted in eastus"
    assert h.run.status.region == "eu-test-1"  # published with the meta: the next plan tries another region first


def test_a_vendor_wide_region_failure_blocks_the_region_for_the_vendor(h):
    h.cluster.jobs["up"] = ["fail"]
    h.cluster.up_error = "DBAAS_REGION_UNAVAILABLE: [vendor-wide] azure pre-flight: restricted in eu-test-1"
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert h.codes()[TASK][1].startswith("DBAAS_REGION_UNAVAILABLE: [vendor-wide] azure pre-flight")  # as before
    assert launch.blocked_regions(h.kube, "oci", h.clock.now) == {"eu-test-1"}  # the database's region


def test_a_region_failure_of_one_database_blocks_nothing_for_the_vendor(h):
    h.cluster.jobs["up"] = ["fail"]
    h.cluster.up_error = "DBAAS_REGION_UNAVAILABLE: azure pre-flight: the SKU is not offered"
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert launch.blocked_regions(h.kube, "oci", h.clock.now) == set()


def test_a_region_unavailable_marker_without_a_cause_still_ends_the_run(h):
    h.cluster.jobs["up"] = ["fail"]
    h.cluster.up_error = "DBAAS_REGION_UNAVAILABLE:"
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert h.codes()[TASK][1] == "DBAAS_REGION_UNAVAILABLE: no cause given"


def test_a_failed_up_job_and_its_pod_stay_for_their_logs_through_the_destroy(h):
    h.cluster.jobs["up"] = ["fail"]
    h.start_db()
    h.until_phase(DbPhase.DESTROYED)
    up = f"dbaas-up-{h.name}"
    assert up in h.jobs() and f"{up}-pod" in h.pod_names() and ("job", up) not in h.kube.deleted
    assert h.kube.get("job", up)["spec"]["ttlSecondsAfterFinished"] == dbaas_jobs.FINISHED_JOB_TTL_SECONDS


def test_a_long_job_error_is_kept_in_the_status_and_the_result(h, monkeypatch):
    cause = "creating (0s) error: " + "y" * 1400 + " the end"
    monkeypatch.setattr(dbaas_jobs, "job_message", lambda kube, name: {"error": cause})
    h.cluster.jobs["up"] = ["fail"]
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert h.db.error == f"DBAAS_CREATE_FAILED: {cause}"
    assert h.codes()[TASK][1].endswith("the end")


def test_a_create_that_takes_too_long_ends_the_task_but_the_up_job_is_never_killed(h):
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.CREATING  # the Pod runs
    h.clock.advance(minutes=66)
    h.runner.reconcile(h.name)
    assert h.codes()[TASK][1].startswith("DBAAS_CREATE_TIMEOUT") and h.run.status.phase == RunPhase.FAILED
    assert h.db.phase == DbPhase.DESTROYING
    assert h.jobs() == [f"dbaas-up-{h.name}"]  # still running: it ends within its own deadline, then the destroy
    assert (("job", f"dbaas-up-{h.name}")) not in h.kube.deleted
    h.cluster.finish_job(f"dbaas-up-{h.name}", "fail")  # the Job's deadline passed and Pulumi recorded what it did
    h.runner.reconcile(h.name)
    h.until_phase(DbPhase.DESTROYED)


def test_a_failed_up_job_whose_pod_still_runs_holds_back_the_destroy(h):
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    h.cluster.react()
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=66)
    h.runner.reconcile(h.name)
    up = f"dbaas-up-{h.name}"
    h.kube.set_status("job", up, {"conditions": [{"type": "Failed", "status": "True"}]})  # the deadline passed
    for _ in range(3):
        h.runner.reconcile(h.name)  # the Pod is Running through its grace period: Pulumi finishes the create
    assert h.jobs() == [up] and h.db.phase == DbPhase.DESTROYING
    h.cluster.finish_job(up, "fail")
    h.runner.reconcile(h.name)
    assert f"dbaas-down-{h.name}" in h.jobs() and f"{up}-pod" in h.pod_names()  # the Pod stays for its logs


def hold_up_pod_of_failed_job(h) -> str:
    """A failed up Job whose Pod is still there (the creation hung past its deadline)."""
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    h.cluster.react()
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=66)
    h.runner.reconcile(h.name)
    up = f"dbaas-up-{h.name}"
    h.kube.set_status("job", up, {"conditions": [{"type": "Failed", "status": "True"}]})
    return f"{up}-pod"


def test_a_failed_up_jobs_pod_in_phase_unknown_holds_back_the_destroy(h):
    pod = hold_up_pod_of_failed_job(h)
    h.kube.set_status("pod", pod, {"phase": "Unknown"})  # its node is unreachable: Pulumi may still run
    h.clock.advance(minutes=5)
    for _ in range(3):
        h.runner.reconcile(h.name)
    assert h.jobs() == [f"dbaas-up-{h.name}"] and h.db.phase == DbPhase.DESTROYING


def test_a_pod_terminating_far_past_its_grace_period_no_longer_holds_back_the_destroy(h):
    pod = hold_up_pod_of_failed_job(h)
    h.kube.patch_obj("pod", pod, {"metadata": {"deletionTimestamp": iso(h.clock.now)}})  # the node is gone
    h.kube.patch_obj("pod", pod, {"spec": {"terminationGracePeriodSeconds": 900}})
    h.clock.advance(minutes=24)  # within grace (15 min) plus the margin (10 min)
    h.runner.reconcile(h.name)
    assert h.jobs() == [f"dbaas-up-{h.name}"] and "UP_POD_STUCK" not in h.db.error  # not yet
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert f"dbaas-down-{h.name}" in h.jobs()
    assert h.db.error.startswith("DBAAS_UP_POD_STUCK: ") and pod in h.db.error


@pytest.mark.parametrize("how", ["cancel", "delete"])
def test_a_teardown_waits_for_a_running_up_job_and_its_pod_before_the_destroy(h, how):
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    h.cluster.react()
    h.runner.reconcile(h.name)
    if how == "cancel":
        crd.request_cancel(h.kube, h.name)
    else:
        h.kube.delete("benchmarkrun", h.name)
    for _ in range(3):
        h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.DESTROYING and h.run.status.phase == RunPhase.CANCELLED
    assert h.jobs() == [f"dbaas-up-{h.name}"] and ("job", f"dbaas-up-{h.name}") not in h.kube.deleted
    h.cluster.finish_job(f"dbaas-up-{h.name}", "ok")  # the creation finished within its deadline
    keep = h.kube.delete

    def slow(kind, name, grace_seconds=None):  # the Pod of a deleted Job lingers for its termination grace period
        if kind == "job":
            h.kube.deleted.append((kind, name))
            h.kube.objects["job"].pop(name, None)
            return
        keep(kind, name, grace_seconds)

    h.kube.delete = slow
    h.runner.reconcile(h.name)
    assert h.jobs() == []  # the Pulumi process may still be running: no destroy yet
    h.kube.delete = keep
    h.kube.delete("pod", f"dbaas-up-{h.name}-pod")
    h.runner.reconcile(h.name)
    assert h.jobs() == [f"dbaas-down-{h.name}"]
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert (how == "delete") != h.run_exists()  # a deleted run is gone once the database is destroyed
    if how == "cancel":
        assert h.db.phase == DbPhase.DESTROYED


def test_a_failed_bootstrap_ends_the_task_and_destroys_the_database(h):
    h.cluster.jobs["boot"] = ["fail"]
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert h.codes()[TASK] == (ExitCode.NO_NODE, "DBAAS_BOOTSTRAP_FAILED: no route")
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.DESTROYING
    h.until_phase(DbPhase.DESTROYED)


def test_a_missing_network_fails_before_anything_is_created(h):
    h.cfg = make_dbaas_config()
    h.cfg.settings.dbaas.vendors["oci"].networks.clear()
    h.start_db()
    run = h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert h.codes()[TASK][1].startswith("DBAAS_NO_NETWORK") and run.status.database is None
    assert h.jobs() == [] and h.kube.list("secret") == []


def test_a_missing_image_is_a_harness_error_not_a_database(h):
    h.cfg = Config(env=EnvConfig(bucket="b", bucket_region="r", sc_run_image="x"), settings=h.cfg.settings)
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.FAILED)
    assert h.codes()[TASK] == (ExitCode.HARNESS_ERROR, "K8SI_DBAAS_IMAGE is empty") and h.jobs() == []


def down_password_ref(job: dict) -> dict:
    env = {e["name"]: e for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    return env["DBAAS_PASSWORD"]["valueFrom"]["secretKeyRef"]


def test_the_down_job_does_not_need_the_password_but_the_up_job_does(h):
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    assert "optional" not in down_password_ref(h.kube.get("job", f"dbaas-up-{h.name}"))
    assert down_password_ref(dbaas_jobs.down_job(h.name, "oci", h.cfg)) == {
        "name": f"{h.name}-db",
        "key": "K8SI_RUN_PASSWORD",
        "optional": True,
    }


def test_starting_the_bootstrap_deletes_an_up_job_left_by_a_restart(h):
    h.start_db()
    h.until_phase(DbPhase.BOOTSTRAPPING)
    h.kube.create("job", dbaas_jobs.up_job(h.run, h.cfg, {}, h.runner._run_owner(h.run)))  # the delete did not happen
    h.runner._start_bootstrap(h.run, h.cfg, {p["metadata"]["name"]: p for p in h.kube.list("pod")})
    assert f"dbaas-up-{h.name}" not in h.jobs()


def test_a_stack_left_from_an_earlier_run_is_destroyed_first_and_the_run_goes_on(h):
    h.storage.objects[STATE_KEY] = b"{}"
    h.start_db()
    h.until_phase(DbPhase.CLEARING)
    assert h.jobs() == [f"dbaas-down-{h.name}"]  # no `up` yet: only the destroy of the leftover
    assert h.pod_names() == [] and h.run.status.phase == RunPhase.PENDING  # and no paid client node waits for it
    assert h.kube.list("secret") == []  # no Secret of the run yet: the down Job must start without its password
    assert down_password_ref(h.kube.get("job", f"dbaas-down-{h.name}")).get("optional") is True
    h.storage.objects.pop(STATE_KEY)  # what the destroy removes
    run = h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    assert h.codes()[TASK] == (0, None) and run.status.attempts[0].outcome.value == "Completed"  # no -1 result
    h.until_phase(DbPhase.DESTROYED)


def test_a_destroy_of_the_leftover_stack_that_fails_is_retried(h):
    h.cluster.jobs["down"] = ["fail"]
    h.storage.objects[STATE_KEY] = b"{}"
    h.start_db()
    h.until_phase(DbPhase.CLEARING)
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.CLEARING and h.db.destroy_failures == 1
    assert h.jobs() == [f"dbaas-down-{h.name}"]  # the failed Job stays for its logs until the retry replaces it
    h.clock.advance(minutes=2)
    h.storage.objects.pop(STATE_KEY)
    h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    assert h.codes()[TASK] == (0, None)


def test_a_leftover_stack_that_cannot_be_destroyed_ends_the_run_with_a_result_and_keeps_the_stack(h):
    failures = []
    h.runner._on_leftover_stack_failure = failures.append
    h.cluster.jobs["down"] = ["fail"] * 5
    h.storage.objects[STATE_KEY] = b"{}"
    h.start_db()
    h.until_phase(DbPhase.CLEARING)
    for _ in range(3):
        h.cluster.react()
        h.runner.reconcile(h.name)
        h.clock.advance(minutes=5)
        h.runner.reconcile(h.name)
    run = h.run
    assert run.status.phase == RunPhase.FAILED and run.status.database is None  # nothing holds the vendor's slot
    code, message = h.codes()[TASK]
    assert code == ExitCode.NO_NODE and message.startswith("DBAAS_LEFTOVER_STACK: k8s-dbaas-")
    assert "operator action needed" in message and failures == ["oci"]
    assert STATE_KEY in h.storage.objects and h.run_pods() == []  # the sweeper and the operator take it from here


@pytest.mark.parametrize("how", ["cancel", "delete"])
def test_a_run_cancelled_while_clearing_records_nothing(h, how):
    h.cluster.jobs["down"] = ["hang"]
    h.storage.objects[STATE_KEY] = b"{}"
    h.start_db()
    h.until_phase(DbPhase.CLEARING)
    if how == "cancel":
        crd.request_cancel(h.kube, h.name)
    else:
        h.kube.delete("benchmarkrun", h.name)
    h.runner.reconcile(h.name)
    assert h.codes().get(TASK, (None, None)) == (None, None)  # no result for a database that was never created
    assert h.db.phase == DbPhase.DESTROYING  # the leftover stack is still destroyed


# ------------------------------------------------------------------ the node is lost, the run is cancelled or deleted


def test_a_lost_client_node_ends_the_run_and_destroys_the_database(h):
    h.start_db()
    h.drive(stop=in_step(STEP))
    h.kube.delete("pod", f"{h.name}-a1")  # a node lost on-demand: no spot retry
    run = h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    assert h.codes()[TASK] == (ExitCode.NEVER_FINISHED, "NODE_LOST")
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.DESTROYING and h.jobs() == [f"dbaas-down-{h.name}"]
    h.until_phase(DbPhase.DESTROYED)
    assert run.status.database is not None


def test_a_cancel_destroys_the_database(h):
    h.start_db()
    h.drive(stop=in_step(STEP))
    crd.request_cancel(h.kube, h.name)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.CANCELLED and h.db.phase == DbPhase.DESTROYING
    assert h.jobs() == [f"dbaas-down-{h.name}"] and h.run_pods() == []


def test_deleting_the_run_keeps_it_until_the_database_is_gone(h):
    h.start_db()
    h.drive(stop=in_step(STEP))
    h.kube.delete("benchmarkrun", h.name)  # the finalizer keeps it
    assert h.run_exists() and h.kube.get("benchmarkrun", h.name)["metadata"]["deletionTimestamp"]
    h.runner.reconcile(h.name)  # a deleted run is a cancelled run
    assert h.run.status.phase == RunPhase.CANCELLED and h.db.phase == DbPhase.DESTROYING
    h.cluster.react()
    h.runner.reconcile(h.name)  # the destroy succeeded
    assert not h.run_exists()


def test_deleting_a_run_that_never_created_a_database_releases_it_at_once(h):
    h.start_db()
    h.runner.reconcile(h.name)  # anchor created, no node yet
    h.kube.delete("benchmarkrun", h.name)
    h.runner.reconcile(h.name)
    assert not h.run_exists() and h.jobs() == []
    assert h.storage.status == {} and h.pod_names() == []  # a deleted run leaves no record, like a server run


# --------------------------------------------------------------------------- retries and restarts


def test_the_destroy_is_retried_with_a_growing_wait_until_it_succeeds(h):
    h.cluster.jobs["down"] = ["fail", "fail"]
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    h.runner.reconcile(h.name)
    h.cluster.react()
    h.runner.reconcile(h.name)  # the first destroy failed
    assert h.db.destroy_failures == 1 and h.db.phase == DbPhase.DESTROYING and "quota exceeded" in h.db.error
    failed = f"dbaas-down-{h.name}"
    assert h.jobs() == [failed] and h.db.retry_after == START + timedelta(minutes=1)
    h.runner.reconcile(h.name)
    assert h.jobs() == [failed] and ("job", failed) not in h.kube.deleted  # kept, not counted twice
    assert h.db.destroy_failures == 1
    h.clock.advance(minutes=1, seconds=1)
    h.runner.reconcile(h.name)
    assert ("job", failed) in h.kube.deleted  # the retry replaces it
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.db.destroy_failures == 2 and h.db.retry_after == h.clock.now + timedelta(minutes=2)
    h.clock.advance(minutes=3)
    h.runner.reconcile(h.name)
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.DESTROYED


def test_a_failed_destroy_job_from_before_the_failure_annotation_is_counted_once_and_retried(h):
    h.cluster.jobs["down"] = ["fail"]
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    h.runner.reconcile(h.name)
    h.cluster.react()
    down = f"dbaas-down-{h.name}"
    del h.kube.objects["job"][down]["metadata"]["annotations"][dbaas_jobs.ANNOTATION_DESTROY_FAILURES]
    h.runner.reconcile(h.name)
    assert h.db.destroy_failures == 1 and ("job", down) in h.kube.deleted  # counted once, the Job is replaced
    h.clock.advance(minutes=1, seconds=1)
    h.runner.reconcile(h.name)
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.db.phase == DbPhase.DESTROYED and h.db.destroy_failures == 1


def test_a_restarted_controller_continues_creating_and_runs_the_destroy(h):
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    second = h.new_runner()  # the controller restarts while the database is being created
    second.reconcile(h.name)
    assert h.jobs() == [f"dbaas-up-{h.name}"] and h.db.phase == DbPhase.CREATING
    h.cluster.finish_job(f"dbaas-up-{h.name}", "ok")
    h.drive(runner=second, stop=in_step(STEP))
    crd.request_cancel(h.kube, h.name)
    third = h.new_runner()  # and again, before the destroy
    h.drive(runner=third, stop=lambda run: run.status.database.phase == DbPhase.DESTROYED)


def test_a_restart_between_the_phase_and_the_job_creates_the_missing_job(h):
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    h.kube.delete("job", f"dbaas-up-{h.name}")  # the controller died after writing the phase
    h.new_runner().reconcile(h.name)
    assert h.jobs() == [f"dbaas-up-{h.name}"]


def test_the_password_survives_a_restart_and_is_never_in_the_job_spec(h):
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    password = h.secret(f"{h.name}-db")["K8SI_RUN_PASSWORD"]
    h.kube.delete("job", f"dbaas-up-{h.name}")
    h.new_runner().reconcile(h.name)
    assert h.secret(f"{h.name}-db")["K8SI_RUN_PASSWORD"] == password
    assert password not in json.dumps(h.kube.get("job", f"dbaas-up-{h.name}"))


def test_the_generated_password_fits_the_vendor_policies():
    for _ in range(100):
        password = runner_dbaas._new_password()
        assert len(password) <= 30 and re.fullmatch(r"[A-Za-z0-9_-]+", password)
        assert password.startswith("Sc1-")  # upper, lower, digit and special, whatever the random part is


def test_the_anchor_is_kept_alive_while_the_database_is_created(h):
    h.cluster.jobs["up"] = ["hang"]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    before = len(h.kube.heartbeats)
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert len(h.kube.heartbeats) > before


def test_the_anchor_deadline_covers_the_creation_and_the_bootstrap(h):
    h.start_db()
    h.runner.reconcile(h.name)
    deadline = h.kube.get("pod", f"{h.name}-a1")["spec"]["activeDeadlineSeconds"]
    # one step (120 min task + 15 min pull + 1 + 10 + 5, two 15 min flushes) + 1 h + 60 min creation + 75 min bootstrap
    assert deadline == (120 + 15 + 1 + 10 + 5 + 30 + 60 + 60 + 75) * 60


def test_the_up_job_uses_the_vendor_service_account_and_credentials(h):
    h.cfg.settings.dbaas.vendors["oci"] = DbaasVendor(networks={"eu-test-1": {}}, service_account="oci-dbaas")
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    (job,) = h.kube.list("job")
    pod = job["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "oci-dbaas"
    assert pod["containers"][0]["envFrom"] == [{"secretRef": {"name": "k8s-inspector-dbaas-oci", "optional": True}}]
    env = {e["name"]: e for e in pod["containers"][0]["env"]}
    assert env["PULUMI_CONFIG_PASSPHRASE"]["valueFrom"]["secretKeyRef"] == {
        "name": "k8s-inspector-dbaas-state",
        "key": "passphrase",
    }
    assert pod["nodeSelector"] == {"karpenter.sh/nodepool": "apps"}


def test_the_state_bucket_credentials_are_a_profile_not_the_aws_environment(h):
    h.cfg = Config(env=replace(h.cfg.env, dbaas_role_arn="arn:aws:iam::1:role/state"), settings=h.cfg.settings)
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    (job,) = h.kube.list("job")
    container = job["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["PULUMI_BACKEND_URL"] == ("s3://b/dbaas-state?region=eu-west-1&awssdk=v2&profile=k8si-state")
    assert not [name for name in env if name.startswith("AWS_")]  # nothing for a vendor's provider to pick up
    config = env["K8SI_STATE_AWS_CONFIG"]
    assert config.startswith("[profile k8si-state]\nrole_arn = arn:aws:iam::1:role/state\n")
    assert "web_identity_token_file = /var/run/secrets/aws/token" in config
    assert {"name": "aws-token", "mountPath": "/var/run/secrets/aws", "readOnly": True} in container["volumeMounts"]


def test_the_jobs_are_not_disrupted_and_get_time_to_record_what_is_in_flight(h):
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    (up,) = h.kube.list("job")
    h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    down = h.kube.get("job", dbaas_jobs.down_job_name(h.name))
    for job in (up, down):
        template = job["spec"]["template"]
        assert template["metadata"]["annotations"] == {"karpenter.sh/do-not-disrupt": "true"}
        assert template["spec"]["terminationGracePeriodSeconds"] == 900
    assert up["spec"]["activeDeadlineSeconds"] == 3600 and h.db.phase == DbPhase.DESTROYING


def test_the_vendor_module_gets_pulumi_timeouts_shorter_than_the_up_jobs_deadline(h):
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    (job,) = h.kube.list("job")
    env = {e["name"]: e for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    spec = json.loads(env["DBAAS_SPEC"]["value"])
    assert spec["createTimeoutMinutes"] == 50 and spec["deleteTimeoutMinutes"] == 110  # 60 and 120 minus 10


def test_the_down_job_is_labelled_not_owned_so_foreground_deletion_cannot_wait_for_it(h):
    h.start_db()
    h.drive(stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    down = h.kube.get("job", dbaas_jobs.down_job_name(h.name))
    assert "ownerReferences" not in down["metadata"]
    assert down["metadata"]["labels"][crd.LABEL_RUN] == h.name
    assert down["metadata"]["annotations"][dbaas_jobs.ANNOTATION_RUN] == h.name
    h.until_phase(DbPhase.DESTROYED)
    assert h.jobs() == []  # cleaned up after the success


def test_only_the_dbaas_finalizer_is_removed(h):
    h.start_db()
    h.kube.objects["benchmarkrun"][h.name]["metadata"]["finalizers"].append("example.com/other")
    h.drive(stop=in_step(STEP))
    crd.request_cancel(h.kube, h.name)
    h.until_phase(DbPhase.DESTROYED)
    assert h.kube.get("benchmarkrun", h.name)["metadata"]["finalizers"] == ["example.com/other"]


def test_job_names_stay_within_the_limit_and_are_deterministic():
    run = "o" * 63
    for name in (dbaas_jobs.up_job_name(run), dbaas_jobs.down_job_name(run), dbaas_jobs.inventory_job_name("v" * 63)):
        assert len(name) <= 63
    assert dbaas_jobs.up_job_name(run) == dbaas_jobs.up_job_name(run)
    assert dbaas_jobs.up_job_name(run) != dbaas_jobs.up_job_name("o" * 62 + "p")  # the hash tells long names apart
    assert dbaas_jobs.up_job_name("aws-x-1") == "dbaas-up-aws-x-1"  # short names are kept as they are


def test_the_client_goes_to_its_own_region_when_the_database_is_in_another(h):
    h.cfg.settings.dbaas.vendors["oci"].networks.clear()
    h.cfg.settings.dbaas.vendors["oci"].networks["eu-test-1"] = {"vpc": "client-side"}  # keyed by the client's region
    spec = RunSpec(
        vendor="oci", sku=DATABASE["id"], run_id="r20261120t1000-aaaa", tasks=[TASK], capacity=CapacityMode.ON_DEMAND,
        database={**DATABASE, "region": "db-test-1", "clientRegion": "eu-test-1"},
    )  # fmt: skip
    h.name = crd.create_run(h.kube, spec).name
    h.drive(stop=lambda run: run.status.phase == RunPhase.PROVISIONING)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    assert anchor["spec"]["nodeSelector"]["topology.kubernetes.io/region"] == "eu-test-1"  # the client's
    h.until_phase(DbPhase.CREATING)
    (job,) = h.kube.list("job")
    env = {e["name"]: e.get("value") for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    spec = json.loads(env["DBAAS_SPEC"])
    assert (spec["region"], spec["clientRegion"]) == ("db-test-1", "eu-test-1")
    assert spec["network"] == {"vpc": "client-side"}


def test_the_up_job_gets_the_public_addresses_of_the_client_node(h):
    h.cluster.node_addresses = [
        {"type": "InternalIP", "address": "10.1.2.3"},
        {"type": "ExternalIP", "address": "203.0.113.7"},
        {"type": "ExternalIP", "address": "2001:db8::7"},
    ]
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    (job,) = h.kube.list("job")
    env = {e["name"]: e.get("value") for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert json.loads(env["DBAAS_SPEC"])["clientAddresses"] == ["203.0.113.7/32", "2001:db8::7/128"]


def _spec_of_up_job(h) -> dict:
    (job,) = h.kube.list("job")
    env = {e["name"]: e.get("value") for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    return json.loads(env["DBAAS_SPEC"])


def _client_node_without_address(h):
    h.cluster.node_addresses = []
    h.start_db()
    h.drive(stop=lambda run: run.status.database is not None)  # Creating, the node is up
    h.runner.reconcile(h.name)


def test_the_up_job_waits_for_the_public_address_of_the_client_node(h, caplog):
    caplog.set_level(logging.INFO)
    _client_node_without_address(h)
    assert h.kube.list("job") == [] and h.run.status.database.phase == DbPhase.CREATING
    assert "waiting for the public address" in caplog.text
    node = h.kube.list("node")[0]["metadata"]["name"]
    h.clock.advance(minutes=2)  # SCarpenter's sweep writes the annotation
    h.kube.merge_patch(
        "node", node, {"metadata": {"annotations": {"scarpenter.sparecores.com/public-ipv4": "203.0.113.9"}}}
    )
    h.runner.reconcile(h.name)
    assert _spec_of_up_job(h)["clientAddresses"] == ["203.0.113.9/32"]
    timing = h.run.status.database.timing
    assert timing["api_start"] == h.clock.now and h.run.status.database.since < timing["api_start"]  # the Job's start


def test_the_wait_logs_only_at_the_start_and_makes_no_keeper_or_secret_call(h, caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    _client_node_without_address(h)
    logged = caplog.text.count("waiting for the public address")  # the first poll(s), right at the start
    assert logged >= 1
    asked = []
    monkeypatch.setattr(dbaas_keeper, "database", lambda *a: asked.append(a) or dict(RECORD))
    for _ in range(3):
        h.clock.advance(seconds=15)
        h.runner.reconcile(h.name)
    assert asked == [] and f"{h.name}-db" not in h.kube.objects.get("secret", {})
    assert caplog.text.count("waiting for the public address") == logged  # none after the first poll


def test_an_address_only_on_the_nodeclaim_is_enough(h):
    _client_node_without_address(h)
    (claim,) = h.kube.list("nodeclaim")
    h.kube.merge_patch(
        "nodeclaim", claim["metadata"]["name"],
        {"metadata": {"annotations": {"scarpenter.sparecores.com/public-ipv4": "203.0.113.8"}}},
    )  # fmt: skip
    h.runner.reconcile(h.name)
    assert _spec_of_up_job(h)["clientAddresses"] == ["203.0.113.8/32"]


def test_a_new_attempt_waits_for_the_address_of_its_new_node(h):
    _client_node_without_address(h)
    h.clock.advance(minutes=4)
    h.runner.reconcile(h.name)
    assert h.kube.list("job") == []
    (node,) = h.kube.list("node")
    h.kube.delete("node", node["metadata"]["name"])  # the node is lost during the wait
    h.drive(stop=lambda run: run.status.attempt == 2 and run.status.phase == RunPhase.RUNNING)
    h.clock.advance(minutes=2)  # 6 minutes since the first node, 2 since the second
    h.runner.reconcile(h.name)
    assert h.kube.list("job") == []
    h.clock.advance(minutes=4)
    h.runner.reconcile(h.name)
    assert _spec_of_up_job(h)["clientAddresses"] == []


def test_the_up_job_starts_without_an_address_once_the_wait_is_over(h):
    _client_node_without_address(h)
    h.clock.advance(minutes=4)
    h.runner.reconcile(h.name)
    assert h.kube.list("job") == []
    h.clock.advance(minutes=2)  # a vendor that needs no address is not blocked
    h.runner.reconcile(h.name)
    assert _spec_of_up_job(h)["clientAddresses"] == []


def test_a_vendor_generated_password_replaces_the_one_the_benchmark_uses_and_the_up_job_is_deleted(h):
    h.start_db()
    h.until_phase(DbPhase.CREATING)
    run_password = h.secret(f"{h.name}-db")["K8SI_RUN_PASSWORD"]
    h.cluster.db_outputs = {**h.cluster.db_outputs, "password": "Vendor-Generated-pw"}
    h.until_phase(DbPhase.BOOTSTRAPPING)
    secret = h.secret(f"{h.name}-db")
    assert secret["SC_DB_PASSWORD"] == "Vendor-Generated-pw" and secret["K8SI_RUN_PASSWORD"] == run_password
    assert h.kube.list("job") == [] and h.kube.list("pod", labels={dbaas_jobs.LABEL_JOB: f"dbaas-up-{h.name}"}) == []


def test_a_restart_after_the_secret_was_written_keeps_the_runs_own_password_for_later_jobs(h):
    h.cluster.db_outputs = {**h.cluster.db_outputs, "password": "Vendor-Generated-pw"}
    h.start_db()
    h.until_phase(DbPhase.BOOTSTRAPPING)
    run_password = h.secret(f"{h.name}-db")["K8SI_RUN_PASSWORD"]
    second = h.new_runner()  # restarted after the Secret was written
    h.drive(runner=second, stop=in_step(STEP))
    assert h.secret(f"{h.name}-db")["SC_DB_PASSWORD"] == "Vendor-Generated-pw"
    assert second._password(h.run) == run_password
    h.drive(runner=second, stop=lambda run: run.status.phase == RunPhase.PUBLISHING)
    down = h.kube.get("job", dbaas_jobs.down_job_name(h.name))
    (env,) = [e for e in down["spec"]["template"]["spec"]["containers"][0]["env"] if e["name"] == "DBAAS_PASSWORD"]
    assert env["valueFrom"]["secretKeyRef"] == {
        "name": f"{h.name}-db",
        "key": "K8SI_RUN_PASSWORD",
        "optional": True,
    }
