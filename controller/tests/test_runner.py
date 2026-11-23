"""Runner tests: a fake Kube, a fake Storage and a scripted cluster (tests/fakes.py), real catalogue tasks.

Each test drives `Runner.reconcile` the way the watch would: reconcile, let the cluster react (provision the
anchor's node, finish step Pods as scripted), repeat.
"""

import base64
import json
import threading
from datetime import UTC, datetime, timedelta

import httpx2 as httpx
import pytest
from fakes import Clock, Cluster, FakeKube, FakeStorage, iso
from lightkube import ApiError

from k8s_inspector import crd, launch, pods, spot, steps, tasks
from k8s_inspector import kube as kube_mod
from k8s_inspector import runner as runner_mod
from k8s_inspector.kube import WatchEvent
from k8s_inspector.model import (
    AttemptOutcome,
    CapacityMode,
    CapacityType,
    Config,
    Deadlines,
    EnvConfig,
    ExitCode,
    NodeFacts,
    PodRole,
    RunPhase,
    RunSpec,
    Settings,
    TaskState,
    TaskStatus,
    UploadSettings,
    VendorSettings,
)
from k8s_inspector.runner import CPU_REFUSALS_ANNOTATION, RELAUNCH_ANNOTATION, SKU_NOT_SELECTABLE, Runner
from k8s_inspector.storage import Storage, object_key, render_urls_json

START = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
LSCPU = json.dumps(
    {"lscpu": [{"field": "Model name:", "data": "AMD EPYC 9R14"}, {"field": "Architecture:", "data": "x86_64"}]}
)


def make_config(**settings) -> Config:
    vendors = {"oci": VendorSettings(enabled=True)}
    env = EnvConfig(bucket="b", bucket_region="r", sc_run_image="ghcr.io/sparecores/sc-run@sha256:abc")
    return Config(env=env, settings=Settings(vendors=vendors, **settings))


class Harness:
    def __init__(self, cfg: Config | None = None) -> None:
        self.clock = Clock(START)
        self.kube = FakeKube(self.clock)
        self.storage = FakeStorage()
        self.cluster = Cluster(self.kube, self.clock)
        self.cfg = cfg or make_config()
        self.failures: list[tuple[str, str]] = []  # (vendor, reason) of `on_launch_failure`
        self.runner = self.new_runner()
        self.name = ""

    def new_runner(self) -> Runner:
        return Runner(
            self.kube,
            self.storage,
            lambda: self.cfg,
            "inspector",
            clock=self.clock,
            on_launch_failure=lambda vendor, reason: self.failures.append((vendor, reason)),
        )

    def entry(self, sku: str = "VM.Standard.E5.Flex.2-16") -> dict:
        """The launch ConfigMap entry of the SKU (a vendor oci run)."""
        return launch.read(self.kube, "oci")[sku]

    def start(self, tasks: list[str], **spec) -> None:
        sku = spec.pop("sku", "VM.Standard.E5.Flex.2-16")
        spec = RunSpec(vendor="oci", sku=sku, run_id="r20261120t1000-aaaa", tasks=tasks, **spec)
        self.name = crd.create_run(self.kube, spec).name
        self.storage.objects[object_key(self.run_key(), "lscpu", "stdout")] = LSCPU.encode()

    def run_key(self):
        return crd.run_key(self.kube.run(self.name))

    @property
    def run(self):
        return self.kube.run(self.name)

    def task(self, name: str):
        return next(t for t in self.run.status.tasks if t.name == name)

    def codes(self) -> dict[str, tuple[int | None, str | None]]:
        return {t.name: (t.exit_code, t.error_msg) for t in self.run.status.tasks}

    def pod_names(self, role: str | None = None) -> list[str]:
        pods = self.kube.list("pod")
        return [
            p["metadata"]["name"]
            for p in pods
            if role is None or p["metadata"]["labels"]["inspector.sparecores.com/role"] == role
        ]

    def drive(
        self, rounds: int = 80, stop=lambda run: run.status.phase in (RunPhase.PUBLISHING, RunPhase.FAILED), runner=None
    ):
        """Reconcile and react until `stop(run)` or the rounds are used up."""
        for _ in range(rounds):
            (runner or self.runner).reconcile(self.name)
            if stop(self.run):
                return self.run
            self.cluster.react()
        raise AssertionError(f"not finished: {self.run.status.phase} {self.run.status.current_step}")

    def step_pods(self) -> list[str]:
        return sorted(n.split("-a")[-1] for n in self.pod_names("step"))


@pytest.fixture
def h() -> Harness:
    return Harness()


def in_step(step_suffix: str):
    """Stop condition: a Pod of the step exists and has not finished."""
    return lambda run: (
        bool(run.status.current_step and run.status.current_step.endswith(step_suffix))
        and any(t.state == TaskState.RUNNING for t in run.status.tasks)
    )


# --------------------------------------------------------------------------- happy path


def test_price_annotations_are_recorded_and_a_missing_price_warns(h, caplog):
    h.cluster.claim_annotations = {"scarpenter.sparecores.com/price-incomplete": "true"}
    h.start(["membench"])
    with caplog.at_level("WARNING"):
        run = h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    assert run.status.price_per_hour is None and run.status.price_incomplete is True
    assert any("no scarpenter.sparecores.com/price annotation" in r.message for r in caplog.records)


def test_happy_path_runs_every_step_and_publishes_state(h):
    h.start(["membench", "geekbench"])
    run = h.drive()

    assert run.status.phase == RunPhase.PUBLISHING
    assert [n for n in h.pod_names("step")] == []  # anchor deleted: step Pods went with it
    assert h.pod_names() == []
    assert all(t.state == TaskState.DONE and t.exit_code == 0 for t in run.status.tasks)
    assert {t.name for t in run.status.tasks} == {"membench", "geekbench", "lscpu"}
    # probe step first, then by priority
    created = [n for kind, n in h.kube.created if kind == "pod"]
    steps = [n.split("-a1-")[1] for n in created if "-a1-" in n]
    assert steps == ["01-probes", "02-geekbench", "03-membench"]
    assert h.kube.list("secret") == []

    # status node facts
    assert (run.status.region, run.status.zone, run.status.price_per_hour) == ("eu-test-1", "eu-test-1a", "0.0337")
    assert run.status.attempts[0].outcome == AttemptOutcome.COMPLETED
    assert run.status.attempts[0].capacity_type.value == "spot"

    # status.json per task (commit point) with the Kubernetes facts
    doc = h.storage.status["geekbench"]
    assert doc["exit_code"] == 0 and doc["capacity_type"] == "spot" and doc["zone"] == "eu-test-1a"
    assert doc["node_os"] == "Flatcar 4000" and doc["runtime"] == "k8s-containerd" and doc["nodeclass_hash"] == "h1"
    assert doc["pod"].endswith("02-geekbench") and doc["image"].startswith("ghcr.io/sparecores/")
    assert doc["upload_ok"] is True and doc["stdout_bytes"] == 10

    # run.json: timing from NodeClaim conditions and the kernel boot time of the probe step
    timing = h.storage.run_json["timing"]
    assert set(timing) == {"api_start", "api_end", "node_registered", "node_ready", "machine_start"}
    attempt = h.storage.run_json["attempts"][0]
    assert attempt["attempt"] == 1 and attempt["region"] == "eu-test-1"
    assert {k: attempt[k] for k in timing} == timing  # `timing` is the latest attempt's
    assert attempt["api_start"] == "2026-11-20T10:00:00Z" and attempt["lscpu_stdout"] == LSCPU
    assert h.storage.run_json["database"] is None
    assert set(h.storage.run_json["plan"]) == {"membench", "geekbench", "lscpu"}
    # the anchor was kept alive between steps
    assert h.kube.heartbeats and h.kube.heartbeats[0][1] == "/k8si/runs/r20261120t1000-aaaa"
    # the URLs of a step are signed before its Pod exists, one set of upload slots per task
    assert h.storage.planned == ["lscpu", "geekbench", "membench"]


def test_a_step_pod_is_deleted_once_its_result_is_recorded_so_the_kubelet_can_free_its_images(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    assert h.step_pods() == ["1-02-geekbench"]  # only the step in flight
    assert h.pod_names("anchor") == [f"{h.name}-a1"]
    assert h.task("lscpu").state == TaskState.DONE and h.storage.status["lscpu"]["exit_code"] == 0
    h.drive(stop=in_step("03-membench"))
    assert h.step_pods() == ["1-03-membench"]
    assert h.codes()["geekbench"] == (0, None)  # the missing Pod of a recorded step is not an external delete
    assert h.pod_names("anchor") == [f"{h.name}-a1"]


def test_flush_pods_go_with_their_step_pod(h):
    h.storage.incomplete["geekbench"] = 1
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("03-membench"))
    assert any("flush" in n for k, n in h.kube.created if k == "pod")  # the flush really ran
    assert h.step_pods() == ["1-03-membench"] and h.codes()["geekbench"] == (0, None)


def test_reconciles_after_the_step_pod_is_gone_leave_the_recorded_outcome_alone(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    h.runner.reconcile(h.name)  # records geekbench and deletes its Pod
    assert h.step_pods() == [] and h.codes()["geekbench"] == (0, None)
    created = [n for k, n in h.kube.created if k == "pod"]
    for _ in range(3):  # level-triggered passes with no step Pod left
        h.runner.reconcile(h.name)
        assert h.codes()["geekbench"] == (0, None) and h.codes()["lscpu"] == (0, None)
        assert h.run.status.phase != RunPhase.CANCELLED
    assert [n for k, n in h.kube.created if k == "pod"] == created  # nothing relaunched
    assert h.run.status.phase in (RunPhase.RELEASING, RunPhase.PUBLISHING)


def test_a_new_controller_continues_after_the_step_pods_were_deleted(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("03-membench"))
    created = [n for k, n in h.kube.created if k == "pod"]
    second = h.new_runner()  # a restart: no caches, the finished steps have no Pods
    second.reconcile(h.name)
    assert [n for k, n in h.kube.created if k == "pod"] == created
    run = h.drive(runner=second)
    assert run.status.phase == RunPhase.PUBLISHING
    assert {t.name: t.exit_code for t in run.status.tasks} == {"lscpu": 0, "geekbench": 0, "membench": 0}


def test_a_pod_left_by_a_crash_between_recording_and_deleting_it_is_collected_later(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    delete = h.kube.delete

    def failing(kind, name, grace_seconds=None):
        raise RuntimeError("the controller died before the delete")

    h.kube.delete = failing
    with pytest.raises(RuntimeError):  # the sweep logs it; the release then deletes the anchor and raises
        h.runner.reconcile(h.name)
    h.kube.delete = delete
    assert h.codes()["geekbench"] == (0, None)  # recorded
    assert h.step_pods() == ["1-02-geekbench"]  # not deleted yet
    run = h.drive(runner=h.new_runner())
    assert run.status.phase == RunPhase.PUBLISHING and h.codes()["geekbench"] == (0, None)
    assert [n for k, n in h.kube.created if k == "pod"].count(f"{h.name}-a1-02-geekbench") == 1
    assert ("pod", f"{h.name}-a1-02-geekbench") in h.kube.deleted


def test_a_failing_delete_of_a_finished_pod_is_logged_and_does_not_block_the_next_step(h, caplog):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    delete = h.kube.delete

    def failing(kind, name, grace_seconds=None):
        if name.endswith("-02-geekbench"):
            raise api_error(500)
        delete(kind, name, grace_seconds)

    h.kube.delete = failing
    with caplog.at_level("WARNING"):
        h.runner.reconcile(h.name)
    assert f"{h.name}-a1-02-geekbench" in caplog.text
    assert h.codes()["geekbench"] == (0, None)
    assert f"{h.name}-a1-03-membench" in [n for k, n in h.kube.created if k == "pod"]  # the next step started
    h.kube.delete = delete
    run = h.drive()
    assert run.status.phase == RunPhase.PUBLISHING
    assert h.step_pods() == []  # a later pass retried the delete


def test_the_sweep_between_steps_only_deletes_step_and_flush_pods(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.kube.create(
        "pod",
        {
            "metadata": {
                "name": f"{h.name}-a1-dbaas",
                "labels": {crd.LABEL_RUN: h.name, pods.LABEL_ROLE: PodRole.DBAAS.value},
            },
            "spec": {"nodeName": "node-1"},
        },
    )
    h.drive(stop=in_step("03-membench"))
    assert h.step_pods() == ["1-03-membench"]
    assert h.pod_names("dbaas") == [f"{h.name}-a1-dbaas"]
    assert h.pod_names("anchor") == [f"{h.name}-a1"]


def test_a_step_pod_that_is_not_recorded_yet_is_never_deleted(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()  # finished, but its result is not recorded
    h.storage.crash_after_status_json = True
    with pytest.raises(RuntimeError):
        h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING
    assert h.step_pods() == ["1-02-geekbench"]  # the outcome can still be derived from it


def test_probe_step_always_has_lscpu_and_learns_facts(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("01-probes"))
    probe = h.kube.get("pod", f"{h.name}-a1-01-probes")  # deleted once recorded
    assert [c["name"] for c in probe["spec"]["containers"]] == ["lscpu"]
    h.drive(stop=in_step("02-geekbench"))
    assert h.runner._nodes[h.name].facts.lscpu["Model name:"] == "AMD EPYC 9R14"
    assert h.runner._nodes[h.name].facts.boot_time is not None


def test_step_pod_is_pinned_to_the_node_and_uses_the_secret(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    pod = h.kube.get("pod", f"{h.name}-a1-02-geekbench")
    assert pod["spec"]["nodeName"] == f"node-{h.name}-a1"
    assert pod["spec"]["volumes"][1]["secret"]["secretName"] == f"{h.name}-urls"
    urls = json.loads(__import__("base64").b64decode(h.kube.get("secret", f"{h.name}-urls")["data"]["urls.json"]))
    assert set(k.split("/")[0] for k in urls["objects"]) == {"geekbench"}  # only the step's keys
    assert urls["objects"]["geekbench/stdout"]["put"].startswith("https://s3.test/")


# --------------------------------------------------------------------------- induced failures


def test_oom_step_is_recorded_and_the_run_continues(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": 1, "oom_kills": 1}]
    h.start(["membench", "geekbench"])
    run = h.drive()
    assert h.codes()["geekbench"] == (137, "OOMKilled")
    assert h.task("membench").exit_code == 0
    assert h.storage.status["geekbench"]["reason"] == "OOMKilled"
    assert run.status.phase == RunPhase.PUBLISHING


def test_timeout_step_is_minus_six(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "timed_out": True}]
    h.start(["geekbench"])
    h.drive()
    assert h.codes()["geekbench"] == (-6, "Execution timed out after 3600s")


def _unlaunched_claim(h, anchor, name):
    selector = anchor["spec"]["nodeSelector"]
    h.kube.create(  # as Karpenter creates it: pool label only, vendor and SKU in spec.requirements
        "nodeclaim",
        {
            "metadata": {
                "name": name,
                "labels": {"karpenter.sh/nodepool": selector["karpenter.sh/nodepool"]},
                "creationTimestamp": iso(h.clock.now + timedelta(seconds=1)),
            },
            "spec": {
                "requirements": [
                    {"key": key, "operator": "In", "values": [selector[key]]}
                    for key in ("scarpenter.sparecores.com/vendor", "scarpenter.sparecores.com/api-reference")
                ]
            },
        },
    )


def test_unlaunched_nodeclaim_at_the_deadline_is_ice_not_boot_failure(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    _unlaunched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim-unlaunched")
    h.clock.advance(hours=3)
    h.runner.reconcile(h.name)
    assert h.entry()["reason"] == "ICE"  # Karpenter keeps replacing claims that never launched: capacity or quota


def test_image_pull_stuck_becomes_minus_four_and_the_run_continues(h):
    h.cluster.behaviour["geekbench"] = [{"pull_stuck": True}]
    h.start(["membench", "geekbench"])
    h.drive(stop=lambda run: h.kube.list("pod", {"inspector.sparecores.com/task": "geekbench"}) != [])
    h.cluster.react()
    h.runner.reconcile(h.name)  # still within the pull budget
    assert h.task("geekbench").state == TaskState.RUNNING
    h.clock.advance(minutes=16)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-4, "IMAGE_PULL_TIMEOUT")
    assert f"{h.name}-a1-02-geekbench" not in h.pod_names()
    assert "geekbench" in h.storage.aborted
    run = h.drive()
    assert h.task("membench").exit_code == 0 and run.status.phase == RunPhase.PUBLISHING


def test_gpu_step_without_runtime_is_recreated_once_without_the_gpu(h):
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.cluster.node_allocatable = {"memory": "15Gi", "cpu": "2", "nvidia.com/gpu": "1"}
    h.storage.objects.clear()
    h.cluster.behaviour["membench"] = [{"start_error": True}]
    h.start(["membench"])
    h.storage.objects[object_key(h.run_key(), "lscpu", "stdout")] = LSCPU.encode()
    h.storage.objects[object_key(h.run_key(), "gpu_facts", "stdout")] = b"16384, 8.0\n"
    h.drive()

    created = [n for k, n in h.kube.created if k == "pod"]
    assert f"{h.name}-a1-02-membench" in created and f"{h.name}-a1-02-membench-cpu" in created
    assert h.task("membench").exit_code == 0 and h.task("membench").gpu_fallback
    assert h.storage.status["membench"]["gpu_fallback"] is True
    # the GPU facts of the probe step reached the node facts (gpu_facts ran on the GPU node)
    assert "gpu_facts" in h.storage.status


def test_gpu_fallback_happens_once(h):
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.cluster.behaviour["membench"] = [{"start_error": True}, {"start_error": True}]
    h.start(["membench"])
    h.drive()
    created = [n for k, n in h.kube.created if k == "pod" and "membench" in n]
    assert len(created) == 2
    assert h.task("membench").exit_code == 256  # harness error, no second retry


def test_gpu_fallback_pod_has_no_runtime_class(h):
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.cluster.behaviour["membench"] = [{"start_error": True}]
    h.start(["membench"])
    h.drive(stop=in_step("02-membench-cpu"))
    assert ("pod", f"{h.name}-a1-02-membench") in h.kube.deleted  # the failed Pod does not stay around
    assert "runtimeClassName" not in h.kube.get("pod", f"{h.name}-a1-02-membench-cpu")["spec"]
    # the multipart uploads of the first Pod are reused, not planned again
    assert h.storage.planned.count("membench") == 1


def test_incomplete_manifest_runs_a_flush_step_then_finishes(h):
    h.storage.incomplete["geekbench"] = 1
    h.start(["geekbench"])
    h.drive()
    created = [n for k, n in h.kube.created if k == "pod"]
    assert f"{h.name}-a1-02-geekbench-flush1" in created and not any("flush2" in n for n in created)
    assert h.codes()["geekbench"] == (0, None)
    assert h.storage.status["geekbench"]["upload_ok"] is True
    assert "geekbench" not in h.storage.salvaged


def test_outputs_lost_after_two_flush_steps(h):
    h.storage.lost.add("geekbench")
    h.start(["geekbench", "membench"])
    run = h.drive()
    created = [n for k, n in h.kube.created if k == "pod"]
    assert f"{h.name}-a1-02-geekbench-flush1" in created and f"{h.name}-a1-02-geekbench-flush2" in created
    assert not any("flush3" in n for n in created)
    assert h.codes()["geekbench"] == (-4, "OUTPUTS_LOST")
    assert "geekbench" in h.storage.salvaged
    assert h.task("membench").exit_code == 0 and run.status.phase == RunPhase.PUBLISHING


def test_precheck_marks_gpu_tasks_on_cpu_nodes_minus_three(h):
    h.start(["nvbandwidth", "membench"])
    h.drive()
    assert h.codes()["nvbandwidth"] == (-3, "GPU task on a node without an NVIDIA GPU")
    assert h.storage.status["nvbandwidth"]["exit_code"] == -3
    assert h.task("membench").exit_code == 0


def test_vllm_ladder_failure_is_recorded_with_the_probe_errors(h):
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.cluster.behaviour["vllm-probe-benchmark-vllm-gpu"] = [{"exit_code": 3}]
    h.cluster.behaviour["vllm-probe-benchmark-vllm-cpu"] = [{"exit_code": 4}]
    h.cluster.behaviour["vllm-probe-benchmark-vllm-cpu-avx2"] = [{"exit_code": 5}]
    h.start(["vllm"])
    h.storage.objects[object_key(h.run_key(), "gpu_facts", "stdout")] = b"16384, 8.0\n"
    run = h.drive()
    vllm = h.task("vllm")
    assert vllm.exit_code == 1 and "probe benchmark-vllm-gpu: exit_code=3" in vllm.error_msg
    assert "benchmark-vllm-cpu-avx2: exit_code=5" in vllm.error_msg
    assert {"vllm-probe-benchmark-vllm-gpu", "vllm-probe-benchmark-vllm-cpu"} <= set(h.storage.status)
    assert run.status.phase == RunPhase.PUBLISHING


# --------------------------------------------------------------------------- provisioning


def test_capacity_error_on_the_nodeclaim_defers_the_launch_and_deletes_the_run(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), "InsufficientInstanceCapacity in zone")
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.pod_names() == []
    assert h.storage.status == {}  # nothing to publish
    assert h.entry()["reason"] == "ICE" and h.entry()["count"] == 1
    assert h.failures == [("oci", "ICE")]


def test_provisioning_deadline_fails_the_run(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=59)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []
    assert h.entry()["reason"] == "ICE"


def _unschedulable_anchor(
    h, message="0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.", force=False
):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], force=force)
    h.runner.reconcile(h.name)
    condition = {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": message}
    h.kube.merge_patch("pod", f"{h.name}-a1", {"status": {"conditions": [condition]}})


def test_no_offering_is_temporary_unless_the_run_is_forced(h):
    _unschedulable_anchor(h)
    h.clock.advance(minutes=11)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "NO_OFFERING"


def test_no_nodeclaim_and_unschedulable_anchor_fails_fast_with_no_offering(h):
    _unschedulable_anchor(h, force=True)
    h.clock.advance(minutes=9)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    code, message = h.codes()["geekbench"]
    assert code == -1 and message.startswith("SCARPENTER_NO_OFFERING: 0/3 nodes are available")


def _karpenter_event(h, message, reporter="karpenter", name="ev1", uid=None, **times):
    anchor = f"{h.name}-a1"
    h.kube.create(
        "event",
        {
            "metadata": {"name": name},
            "involvedObject": {
                "kind": "Pod",
                "name": anchor,
                "uid": uid or h.kube.get("pod", anchor)["metadata"]["uid"],
            },
            "reason": "FailedScheduling",
            "reportingController": reporter,
            "message": message,
            **times,
        },
    )


BENCH_CLAUSE = (
    "no instance type met all requirements, requirements=frankenscube/pool In [bench], "
    "karpenter.sh/capacity-type In [on-demand spot], karpenter.sh/nodepool In [bench], "
    "scarpenter.sparecores.com/api-reference In [VM.Standard4.Ax.Flex.32-256]"
)
OTHER_CLAUSE = "no instance type met all requirements, requirements=karpenter.sh/nodepool In [other]"
BENCH_MESSAGE = (
    "Failed to schedule pod, did not tolerate taint (taint=frankenscube/web=true:NoSchedule); "
    f"{BENCH_CLAUSE}; did not tolerate taint (taint=frankenscube/apps=true:NoSchedule)"
)


def test_karpenter_event_for_bench_pool_fails_after_fast_deadline(h):
    _unschedulable_anchor(h, force=True)
    _karpenter_event(h, BENCH_MESSAGE)
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert h.codes()["geekbench"] == (
        -1,
        "SCARPENTER_NO_OFFERING: no instance type met all requirements for VM.Standard4.Ax.Flex.32-256"
        " in nodepool bench",
    )


def test_bench_clause_in_the_first_position_is_found(h):
    _unschedulable_anchor(h, force=True)
    _karpenter_event(h, f"Failed to schedule pod, {BENCH_CLAUSE}; {OTHER_CLAUSE}")
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED


def test_event_of_an_earlier_pod_with_the_same_name_is_ignored(h):
    _unschedulable_anchor(h)
    _karpenter_event(h, BENCH_MESSAGE, uid="uid-of-the-previous-anchor")
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_event_from_before_the_attempt_started_is_ignored(h):
    _unschedulable_anchor(h)
    _karpenter_event(h, BENCH_MESSAGE, lastTimestamp=iso(h.clock.now - timedelta(hours=1)))
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_newest_event_wins_by_parsed_time_not_by_text(h):
    _unschedulable_anchor(h)
    now = h.clock.now
    _karpenter_event(h, BENCH_MESSAGE, name="old", lastTimestamp=iso(now + timedelta(seconds=10)))
    newer = (now + timedelta(seconds=40)).isoformat()  # "+00:00" sorts before "Z" as text
    _karpenter_event(h, f"Failed to schedule pod, {OTHER_CLAUSE}", name="new", eventTime=newer)
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_launch_failure_of_the_attempt_disables_the_fast_path(h):
    _unschedulable_anchor(h)
    _karpenter_event(h, BENCH_MESSAGE)
    _launch_failed(h, "bench-aaaaa", "first")
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_karpenter_event_for_other_pool_only_keeps_waiting(h):
    _unschedulable_anchor(h)
    _karpenter_event(h, f"Failed to schedule pod, did not tolerate taint (taint=x:NoSchedule); {OTHER_CLAUSE}")
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_event_not_from_karpenter_is_ignored(h):
    _unschedulable_anchor(h)
    _karpenter_event(h, BENCH_MESSAGE, reporter="default-scheduler")
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_events_api_error_falls_back_to_the_long_deadline(h):
    _unschedulable_anchor(h, force=True)
    _karpenter_event(h, BENCH_MESSAGE)
    h.kube.events_forbidden = True
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=6)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED


def test_no_offering_wait_is_configurable(h):
    h.cfg = make_config(deadlines=Deadlines(no_offering_minutes=3))
    _unschedulable_anchor(h, force=True)
    h.clock.advance(minutes=4)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED


def test_pending_anchor_without_scheduling_message_keeps_waiting(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=30)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_unschedulable_anchor_with_a_nodeclaim_keeps_waiting(h):
    _unschedulable_anchor(h)
    h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), "launching")
    h.kube.merge_patch(
        "nodeclaim", f"claim-{h.name}-a1", {"status": {"conditions": [{"type": "Launched", "status": "Unknown"}]}}
    )
    h.clock.advance(minutes=30)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def _launch_failed(h, name, message="Fatal (InvalidParameter): 400"):
    anchor = h.kube.get("pod", f"{h.name}-a1")
    h.cluster.fail_launch(anchor, message, name=name, status="Unknown", reason="LaunchFailed")


def test_two_distinct_launch_failures_end_the_run_even_when_claims_are_replaced(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa", "first")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    assert [f.node_claim for f in h.run.status.launch_failures] == ["bench-aaaaa"]
    h.kube.delete("nodeclaim", "bench-aaaaa")  # Karpenter removes the failed claim
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    _launch_failed(h, "bench-bbbbb", "Fatal (InvalidParameter): Error returned by Compute Service. 400")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    code, message = h.codes()["geekbench"]
    assert (
        code == -1
        and message == "SCARPENTER_LAUNCH_REJECTED: Fatal (InvalidParameter): Error returned by Compute Service. 400"
    )


def test_launch_failures_of_a_spot_attempt_fall_back_to_an_on_demand_attempt(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    assert pods.allows_spot(h.kube.get("pod", f"{h.name}-a1"))
    _launch_failed(h, "bench-aaaaa", "Fatal (X): first")
    _launch_failed(h, "bench-bbbbb", "Fatal (X): second")
    h.runner.reconcile(h.name)
    assert h.run.status.attempts[0].outcome == AttemptOutcome.LAUNCH_FAILED
    assert h.run.status.phase == RunPhase.PENDING
    h.runner.reconcile(h.name)
    assert h.run.status.attempt == 2 and h.run.status.launch_failures == []
    assert not pods.allows_spot(h.kube.get("pod", f"{h.name}-a2"))
    assert h.codes()["geekbench"] == (None, None)


def test_launch_failures_after_lost_attempts_still_fall_back_with_a_clean_record(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    for attempt in (1, 2):  # attempts lost to a stale node: not a reason to leave spot
        h.runner.reconcile(h.name)
        h.cluster.provision(h.kube.get("pod", f"{h.name}-a{attempt}"), node_created=START - timedelta(hours=1))
        h.runner.reconcile(h.name)
        assert h.run.status.attempts[-1].outcome == AttemptOutcome.PROVISIONING_FAILED
    h.runner.reconcile(h.name)
    assert pods.allows_spot(h.kube.get("pod", f"{h.name}-a3"))
    anchor = h.kube.get("pod", f"{h.name}-a3")
    h.cluster.fail_launch(anchor, "Fatal (X): first", name="bench-c1", status="Unknown", reason="LaunchFailed")
    h.cluster.fail_launch(anchor, "Fatal (X): second", name="bench-c2", status="Unknown", reason="LaunchFailed")
    h.runner.reconcile(h.name)
    assert h.run.status.attempts[2].outcome == AttemptOutcome.LAUNCH_FAILED and h.run.status.phase == RunPhase.PENDING
    h.runner.reconcile(h.name)
    assert not pods.allows_spot(h.kube.get("pod", f"{h.name}-a4"))
    assert h.codes()["geekbench"] == (None, None)


def test_launch_failures_of_the_on_demand_attempt_end_the_run(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    for attempt in (1, 2):
        h.runner.reconcile(h.name)
        anchor = h.kube.get("pod", f"{h.name}-a{attempt}")
        for n, text in ((1, "first"), (2, "second")):
            h.cluster.fail_launch(
                anchor, f"Fatal (X): {text}", name=f"bench-a{attempt}{n}", status="Unknown", reason="LaunchFailed"
            )
        h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert h.codes()["geekbench"] == (-1, "SCARPENTER_LAUNCH_REJECTED: Fatal (X): second")


ICE_TAIL = "insufficient capacity, every launch attempt failed (2 attempts), the last: {}"
NO_QUOTA = "insufficient capacity, no candidate offering had quota headroom (3 skipped)"
NO_CANDIDATE = "insufficient capacity, no offering satisfies the NodeClaim's requirements and has capacity"


def _ice_event(h, claim, tail=None, **times):
    """What Karpenter does when a launch hits InsufficientCapacity: it deletes the NodeClaim (no condition is ever
    seen) and publishes this Event on the pending anchor Pod (the message is SCarpenter's, see its create.go)."""
    anchor = f"{h.name}-a1"
    tail = tail or ICE_TAIL.format("InsufficientCapacity (NoValidHost): nova: HTTP 500: No valid host was found.")
    h.kube.create(
        "event",
        {
            "metadata": {"name": f"ev-{claim}"},
            "involvedObject": {"kind": "Pod", "name": anchor, "uid": h.kube.get("pod", anchor)["metadata"]["uid"]},
            "reason": "InsufficientCapacityError",
            "reportingController": "karpenter",
            "message": f"NodeClaim {claim} nominated for this pod failed to launch: {tail}",
            **times,
        },
    )


def _provisioning_without_nodes(h, **start):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], **start)
    h.runner.reconcile(h.name)


def test_ice_of_deleted_nodeclaims_is_counted_from_the_anchor_events(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND, force=True)
    _ice_event(h, "bench-5krfr", lastTimestamp=iso(h.clock.now))
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    assert [f.node_claim for f in h.run.status.launch_failures] == ["bench-5krfr"]
    h.runner.reconcile(h.name)  # the same event again is not a second failure
    assert [f.node_claim for f in h.run.status.launch_failures] == ["bench-5krfr"]
    h.clock.advance(minutes=3)
    _ice_event(h, "bench-c54f9", lastTimestamp=iso(h.clock.now))
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    code, message = h.codes()["geekbench"]
    assert code == -1
    assert message.startswith("SCARPENTER_ICE: insufficient capacity, every launch attempt failed")


@pytest.mark.parametrize(
    "last_error",
    [
        "Unsupported (InvalidFlavor): flavor not found",
        "InvalidOffering (NoSuchOffering): offering is gone",
        "Fatal (X): something else",
    ],
)
def test_ice_event_is_ice_whatever_the_class_of_the_last_candidate(h, last_error):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND, force=True)
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim, ICE_TAIL.format(last_error))
    h.runner.reconcile(h.name)
    code, message = h.codes()["geekbench"]
    assert code == -1 and message.startswith("SCARPENTER_ICE: ")


def test_ice_events_of_an_unforced_run_end_as_a_temporary_ice(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim, ICE_TAIL.format("Unsupported (InvalidFlavor): flavor not found"))
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []
    assert h.entry()["reason"] == "ICE"


def test_ice_events_of_a_spot_attempt_are_not_a_launch_rejection(h):
    _provisioning_without_nodes(h, force=True)
    assert pods.allows_spot(h.kube.get("pod", f"{h.name}-a1"))
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim, ICE_TAIL.format("InvalidOffering (NoSuchOffering): offering is gone"))
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED  # no on-demand fallback: that is for LAUNCH_REJECTED only
    assert h.codes()["geekbench"][1].startswith("SCARPENTER_ICE: ")


@pytest.mark.parametrize("tail", [NO_QUOTA, NO_CANDIDATE])
def test_ice_events_without_a_last_candidate_class_are_ice(h, tail):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND, force=True)
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim, tail)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-1, f"SCARPENTER_ICE: {tail}")


def test_ice_event_with_a_quota_last_candidate_stays_quota(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim, ICE_TAIL.format("QuotaExceeded (Limit): quota is zero"))
    h.runner.reconcile(h.name)
    assert h.entry()["reason"] == "QUOTA"


def test_the_same_claim_as_a_condition_and_as_an_event_is_one_failure(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    h.cluster.fail_launch(
        anchor, ICE_TAIL.format("Fatal (X): y"), name="bench-aaaaa", status="Unknown", reason="LaunchFailed"
    )
    _ice_event(h, "bench-aaaaa")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    assert [f.node_claim for f in h.run.status.launch_failures] == ["bench-aaaaa"]


def test_a_condition_failure_and_an_event_failure_reach_the_cap(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND, force=True)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    h.cluster.fail_launch(anchor, "Fatal (X): first", name="bench-aaaaa", status="Unknown", reason="LaunchFailed")
    _ice_event(h, "bench-bbbbb")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert len(h.run.status.launch_failures) == 2


def test_ice_events_of_a_bound_anchor_are_ignored(h):
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    h.kube.merge_patch("pod", f"{h.name}-a1", {"spec": {"nodeName": "node-1"}})
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim)
    h.runner.reconcile(h.name)
    assert h.run.status.launch_failures == []


def test_forbidden_events_fail_open(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    h.kube.events_forbidden = True
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING and h.run.status.launch_failures == []


def test_an_event_without_a_nodeclaim_of_this_anchor_is_not_a_launch_failure(h):
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    _ice_event(h, "bench-aaaaa")
    h.kube.merge_patch("event", "ev-bench-aaaaa", {"involvedObject": {"uid": "uid-of-the-previous-anchor"}})
    h.runner.reconcile(h.name)
    assert h.run.status.launch_failures == []


def _ice(h):
    anchor = h.kube.get("pod", f"{h.name}-a1")
    h.cluster.fail_launch(anchor, "InsufficientCapacity (X): no capacity")


def test_each_temporary_class_defers_with_its_reason_and_backoff(h):
    for message, reason in (
        ("QuotaExceeded (X): vCPU limit", "QUOTA"),
        ("DailyLimit (LimitExceeded): daily limit", "DAILY_LIMIT"),
        ("Retryable (RequestLimitExceeded): slow", "VENDOR"),
    ):
        h.cluster.provision_enabled = False
        h.start(["geekbench"])
        h.runner.reconcile(h.name)
        h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), message)
        h.runner.reconcile(h.name)
        assert h.kube.list("benchmarkrun") == [] and h.storage.status == {}
        assert h.entry()["reason"] == reason
        launch.clear(h.kube, "oci", ["VM.Standard.E5.Flex.2-16"])
        h.kube.objects.clear()
        h.cluster.provision_enabled = False
        h.runner = h.new_runner()


def test_repeated_ice_doubles_the_wait_and_a_node_clears_the_entry(h):
    for count, hours in ((1, 1), (2, 2), (3, 4)):
        h.cluster.provision_enabled = False
        h.start(["geekbench"])
        h.runner.reconcile(h.name)
        _ice(h)
        h.runner.reconcile(h.name)
        entry = h.entry()
        assert entry["count"] == count
        assert datetime.fromisoformat(entry["next"]) - h.clock.now == timedelta(hours=hours)
        h.kube.objects.pop("nodeclaim", None)
        h.runner = h.new_runner()
    h.cluster.provision_enabled = True
    h.start(["geekbench"])
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    assert launch.read(h.kube, "oci") == {}


def test_a_failed_launch_keeps_a_run_that_has_finished_tasks(h):
    h.start(["geekbench"])
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    run = h.run
    run.status.tasks.append(TaskStatus("lscpu", TaskState.DONE, 1, START, START, 0))
    h.runner._defer_launch(run, "SCARPENTER_ICE: x")
    assert [r["metadata"]["name"] for r in h.kube.list("benchmarkrun")] == [h.name]
    assert h.entry()["reason"] == "ICE"


def _launched_claim(h, anchor, name):
    """A NodeClaim that launched (the node exists) and never registered."""
    _unlaunched_claim(h, anchor, name)
    _set_conditions(h, name, Launched=True)


def _set_conditions(h, name, *, labels=None, **conditions):
    """Conditions of a claim, each `True` at the current time (`False`: Unknown)."""
    status = {
        "conditions": [
            {"type": kind, "status": "True" if on else "Unknown", "lastTransitionTime": iso(h.clock.now)}
            for kind, on in conditions.items()
        ]
    }
    h.kube.merge_patch("nodeclaim", name, {"status": status, "metadata": {"labels": labels or {}}})


def test_boot_failure_is_temporary_once_waits_a_day_and_is_final_the_second_time(h):
    for count in (1, 2):
        h.cluster.provision_enabled = False
        h.start(["geekbench"])
        h.runner.reconcile(h.name)
        _launched_claim(h, h.kube.get("pod", f"{h.name}-a1"), f"claim-{count}")
        h.clock.advance(minutes=9)
        h.runner.reconcile(h.name)
        if count == 1:
            assert h.kube.list("benchmarkrun") == [] and h.entry()["count"] == 1
            assert datetime.fromisoformat(h.entry()["next"]) - h.clock.now == timedelta(hours=24)
        else:
            assert h.run.status.phase == RunPhase.FAILED
            assert h.codes()["geekbench"][1].startswith("SCARPENTER_BOOT_FAILURE")
            assert h.entry()["count"] == 2  # kept for the vendor's breaker
        h.kube.objects.pop("nodeclaim", None)
        h.runner = h.new_runner()


def _boot_run(h, claim="claim-1", sku=None, **spec):
    """A run whose anchor is pending with a claim that launched now (not registered)."""
    h.cluster.provision_enabled = False
    h.start(["geekbench"], **({"sku": sku} if sku else {}), **spec)
    h.runner.reconcile(h.name)
    _launched_claim(h, h.kube.get("pod", f"{h.name}-a1"), claim)


def test_an_unregistered_claim_past_the_limit_ends_the_run_at_once_and_is_deleted(h):
    _boot_run(h)
    h.clock.advance(minutes=7)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    assert [c.node_claim for c in h.run.status.uninitialized_claims] == ["claim-1"]
    h.clock.advance(minutes=1, seconds=1)  # the limit counts from the claim's creation, one second after the run
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []
    assert ("nodeclaim", "claim-1") in h.kube.deleted
    assert h.entry()["reason"] == "BOOT_FAILURE" and h.failures == [("oci", "BOOT_FAILURE")]


def test_a_forced_run_records_the_boot_failure_message(h):
    _boot_run(h, force=True)
    h.clock.advance(minutes=8, seconds=1)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-1, "SCARPENTER_BOOT_FAILURE: node of claim-1 did not register within 8 min")


def test_the_requeue_is_scheduled_for_the_moment_the_limit_expires(h, monkeypatch):
    _boot_run(h)
    later = []
    monkeypatch.setattr(h.runner, "_enqueue_later", lambda name, delay: later.append((name, delay)))
    h.clock.advance(minutes=3)
    h.runner.reconcile(h.name)
    assert later == [(h.name, 5 * 60 + 1)]


def test_a_claim_being_deleted_while_unregistered_ends_the_run_early(h):
    _boot_run(h)
    h.kube.merge_patch("nodeclaim", "claim-1", {"metadata": {"deletionTimestamp": iso(h.clock.now)}})
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"


def test_a_claim_that_vanished_unregistered_ends_the_run(h):
    _boot_run(h)
    h.runner.reconcile(h.name)  # seen launched: remembered
    h.kube.objects.pop("nodeclaim")  # Karpenter deleted it
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"


def test_a_registered_claim_that_is_replaced_is_not_a_boot_failure(h):
    _boot_run(h)
    _set_conditions(h, "claim-1", Launched=True, Registered=True, Initialized=True)
    h.runner.reconcile(h.name)
    assert h.run.status.uninitialized_claims == []
    h.kube.objects.pop("nodeclaim")
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_boot_failure_of_a_spot_attempt_does_not_fall_back_to_on_demand(h):
    _boot_run(h, capacity=CapacityMode.SPOT_FIRST)
    h.clock.advance(minutes=9)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.pod_names() == []  # no second, on-demand attempt
    assert [n for k, n in h.kube.created if k == "pod"] == [f"{h.name}-a1"]


def test_a_bare_metal_sku_gets_the_longer_registration_limit(h):
    _boot_run(h, sku="BM.Standard3.64")
    h.clock.advance(minutes=13)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=1, seconds=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []


@pytest.mark.parametrize(
    ("sku", "bare_metal"),
    [
        ("BM.Standard3.64", True),
        ("m5.metal", True),
        ("c3-standard-192-metal", True),
        ("vbm-4c-32gb", True),
        ("vdm-8c-32gb", True),
        ("ecs.ebmc7.32xlarge", True),
        ("bm-s1", True),
        ("VM.Standard.E5.Flex.2-16", False),
        ("vc2-1c-0.5gb-v6", False),
        ("m5.large", False),
    ],
)
def test_bare_metal_is_told_by_the_sku_name(sku, bare_metal):
    assert bool(runner_mod.BARE_METAL_SKU.search(sku)) is bare_metal


def _registered_run(h, minutes, sku=None, labels=None, **spec):
    """A run whose claim registered `minutes` ago and whose node is not Ready."""
    _boot_run(h, sku=sku, **spec)
    _set_conditions(h, "claim-1", labels=labels, Launched=True, Registered=True)
    h.clock.advance(minutes=minutes)
    h.runner.reconcile(h.name)


def test_a_registered_node_that_is_not_ready_ends_boot_failure_at_the_ready_limit(h):
    _registered_run(h, 14)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"
    assert ("nodeclaim", "claim-1") in h.kube.deleted


def test_a_bare_metal_node_gets_the_bare_metal_ready_limit(h):
    _registered_run(h, 29, sku="BM.Standard3.64")
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []


def test_a_gpu_node_gets_the_gpu_ready_limit(h):
    labels = {"scarpenter.sparecores.com/accelerator-count": "1"}
    _registered_run(h, 44, labels=labels)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []


def test_a_registered_tainted_gpu_node_that_never_initializes_ends_boot_failure(h):
    """SCarpenter's `gpu-pending` taint stays on a node whose driver check failed: the claim is never Initialized
    and the anchor (no toleration for it, see test_pods) stays Pending."""
    labels = {"scarpenter.sparecores.com/accelerator-count": "1"}
    _registered_run(h, 46, labels=labels)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"
    assert ("nodeclaim", "claim-1") in h.kube.deleted


def test_a_registered_gpu_claim_deleted_before_it_initializes_ends_the_run_after_one_claim(h):
    """SCarpenter deletes a GPU NodeClaim whose NVIDIA check failed; the anchor would only get a new one."""
    labels = {"scarpenter.sparecores.com/accelerator-count": "1"}
    _registered_run(h, 5, labels=labels)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.kube.objects.pop("nodeclaim")
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE" and h.entry()["count"] == 1


def test_a_registered_claim_being_deleted_before_it_initializes_ends_the_run(h):
    _registered_run(h, 5)
    h.kube.merge_patch("nodeclaim", "claim-1", {"metadata": {"deletionTimestamp": iso(h.clock.now)}})
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"


def test_a_claim_deleted_for_a_spot_interruption_is_not_a_boot_failure(h):
    _registered_run(h, 5)
    h.kube.merge_patch(
        "nodeclaim",
        "claim-1",
        {"metadata": {"deletionTimestamp": iso(h.clock.now), "annotations": {spot.INTERRUPTION_ANNOTATION: "x"}}},
    )
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_a_run_with_a_gpu_task_is_a_gpu_run_before_its_claim_has_labels(h):
    h.cluster.provision_enabled = False
    h.start(["nvidia_smi"])
    h.runner.reconcile(h.name)
    _launched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim-1")
    _set_conditions(h, "claim-1", Launched=True, Registered=True)
    h.clock.advance(minutes=44)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_the_pre_launch_limit_still_ends_a_run_that_never_launched_as_ice(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=61)
    h.runner.reconcile(h.name)
    assert h.entry()["reason"] == "ICE"


def test_the_deadline_after_launch_failures_is_the_launch_error_not_boot_failure(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa", "Fatal (InvalidParameter): 400")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.kube.delete("nodeclaim", "bench-aaaaa")
    h.clock.advance(hours=2)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-1, "SCARPENTER_LAUNCH_REJECTED: Fatal (InvalidParameter): 400")


def test_a_temporary_launch_error_after_the_deadline_is_deferred_not_recorded(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa", "InsufficientCapacity (X): none")
    h.runner.reconcile(h.name)
    h.kube.delete("nodeclaim", "bench-aaaaa")
    h.clock.advance(hours=2)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ICE"


def test_the_same_failed_nodeclaim_counts_once(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa")
    h.runner.reconcile(h.name)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    assert len(h.run.status.launch_failures) == 1


def test_one_transient_launch_failure_then_success_proceeds(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa")
    h.runner.reconcile(h.name)
    h.kube.delete("nodeclaim", "bench-aaaaa")
    h.cluster.provision_enabled = True
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    assert h.run.status.phase == RunPhase.RUNNING


def test_node_older_than_the_anchor_is_replaced(h):
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    h.cluster.provision(anchor, node_created=START - timedelta(hours=1))
    h.runner.reconcile(h.name)
    assert h.run.status.attempts[0].outcome == AttemptOutcome.PROVISIONING_FAILED
    assert ("nodeclaim", f"claim-node-{h.name}-a1") in h.kube.deleted
    h.runner.reconcile(h.name)  # Pending -> a new attempt
    assert h.run.status.attempt == 2 and f"{h.name}-a2" in h.pod_names("anchor")


def test_attempt_records_the_capacity_type_of_its_node_only(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)  # the anchor allows spot, but there is no node
    assert h.run.status.attempts[0].capacity_type is None and h.run.status.capacity_type is None
    h.cluster.capacity = "on-demand"  # SCarpenter fell back
    h.cluster.provision_enabled = True
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    assert h.run.status.attempts[0].capacity_type == CapacityType.ON_DEMAND
    assert h.run.status.capacity_type == CapacityType.ON_DEMAND


def test_failed_provisioning_leaves_the_capacity_type_empty_in_run_json(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa", "Fatal (X): first")
    _launch_failed(h, "bench-bbbbb", "Fatal (X): second")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert [a["capacity_type"] for a in h.storage.run_json["attempts"]] == [None]


def test_capacity_follows_the_spot_strategy(h):
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    terms = anchor["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
    assert terms["nodeSelectorTerms"][0]["matchExpressions"][0]["values"] == ["on-demand"]


def test_empty_sc_run_image_fails_fast():
    cfg = Config(env=EnvConfig(bucket="b", bucket_region="r"), settings=make_config().settings)
    h = Harness(cfg)
    with pytest.raises(ValueError, match="K8SI_SC_RUN_IMAGE"):
        h.runner.check_config()
    h.start(["geekbench"])
    with pytest.raises(ValueError, match="K8SI_SC_RUN_IMAGE"):
        h.runner.reconcile(h.name)
    assert h.pod_names() == []
    with pytest.raises(ValueError):
        h.runner.run_forever(threading.Event())


# --------------------------------------------------------------------------- node loss


def lose_node(h: Harness, attempt: int = 1) -> None:
    """The node vanishes mid-step (spot reclaim without a notice): Node, NodeClaim, anchor and its Pods go."""
    h.kube.delete("node", f"node-{h.name}-a{attempt}")
    h.kube.delete("nodeclaim", f"claim-node-{h.name}-a{attempt}")
    h.kube.delete("pod", f"{h.name}-a{attempt}")


def test_spot_node_loss_mid_step_resumes_with_the_remaining_tasks(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("03-membench"))
    assert h.task("geekbench").exit_code == 0
    lose_node(h)
    h.runner.reconcile(h.name)

    run = h.run
    assert run.status.attempts[0].outcome == AttemptOutcome.SPOT_INTERRUPTED
    assert run.status.phase == RunPhase.PENDING
    assert h.task("membench").state == TaskState.PENDING and h.task("geekbench").state == TaskState.DONE
    assert "membench" in h.storage.aborted and h.storage.salvaged == []  # it runs again: nothing to salvage
    assert h.kube.get("configmap", "k8s-inspector-spot")["data"]  # per-SKU memory

    run = h.drive()
    assert run.status.attempt == 2 and run.status.phase == RunPhase.PUBLISHING
    assert h.task("geekbench").attempt == 1 and h.task("membench").attempt == 2
    assert [a.outcome for a in run.status.attempts] == [AttemptOutcome.SPOT_INTERRUPTED, AttemptOutcome.COMPLETED]
    # attempt 2 runs the probe step again, but not geekbench
    created = [n.split("-a2-")[1] for k, n in h.kube.created if k == "pod" and "-a2-" in n]
    assert created == ["01-probes", "02-membench"]
    assert [a["outcome"] for a in h.storage.run_json["attempts"]] == ["SpotInterrupted", "Completed"]
    assert [a["attempt"] for a in h.storage.run_json["attempts"]] == [1, 2]
    assert h.storage.run_json["timing"]["api_start"] == h.storage.run_json["attempts"][1]["api_start"]


def test_spot_budget_exhausted_goes_on_demand_then_node_lost_ends_the_rest(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    lose_node(h, 1)
    h.runner.reconcile(h.name)
    h.drive(stop=lambda run: run.status.attempt == 2 and in_step("02-geekbench")(run))
    lose_node(h, 2)
    h.runner.reconcile(h.name)
    assert [a.outcome for a in h.run.status.attempts] == [AttemptOutcome.SPOT_INTERRUPTED] * 2

    # maxSpotAttempts (2) interrupted attempts: the third anchor requires on-demand
    h.cluster.capacity = "on-demand"
    h.drive(stop=lambda run: run.status.attempt == 3 and in_step("02-geekbench")(run))
    anchor = h.kube.get("pod", f"{h.name}-a3")
    terms = anchor["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
    assert terms["nodeSelectorTerms"][0]["matchExpressions"][0]["values"] == ["on-demand"]

    # an on-demand node lost: not a spot interruption, the unfinished tasks end -4 NODE_LOST
    lose_node(h, 3)
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.attempts[2].outcome == AttemptOutcome.NODE_LOST
    assert run.status.phase == RunPhase.PUBLISHING
    assert h.codes()["geekbench"] == (-4, "NODE_LOST") and h.codes()["membench"] == (-4, "NODE_LOST")
    assert h.storage.status["membench"]["reason"] == "NODE_LOST"


def test_interruption_notice_relabels_the_evicted_step_as_spot_interrupted(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "cancelled": True}]
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    # SCarpenter marks the claim, the drain evicts the step Pod; the node is still there
    h.kube.patch_obj(
        "nodeclaim",
        f"claim-node-{h.name}-a1",
        {"metadata": {"annotations": {"scarpenter.sparecores.com/interruption": "SpotInterrupted"}}},
    )
    h.cluster.react()
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.attempts[0].outcome == AttemptOutcome.SPOT_INTERRUPTED
    assert h.task("geekbench").state == TaskState.PENDING  # runs again on the next node
    assert run.status.phase == RunPhase.PENDING
    assert h.pod_names() == []  # anchor released


def test_cancelled_task_on_a_spot_node_waits_for_the_reclaim_and_is_retried(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "cancelled": True}]
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()  # the container ends with sc-run's cancelled result, the Node is not marked or deleted yet
    h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING and h.run.status.phase == RunPhase.RUNNING
    lose_node(h)  # the reclaim shows
    h.runner.reconcile(h.name)
    assert h.run.status.attempts[0].outcome == AttemptOutcome.SPOT_INTERRUPTED
    assert h.task("geekbench").state == TaskState.PENDING and h.run.status.phase == RunPhase.PENDING


def test_cancelled_task_on_a_spot_node_that_stays_healthy_is_recorded_after_the_wait(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "cancelled": True}]
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    h.clock.advance(minutes=1 + 4)  # the fake task ended a minute after the Pod ran
    h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-4, "CANCELLED")


def test_cancelled_task_waits_longer_while_the_spot_node_is_not_ready(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "cancelled": True}]
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    node = f"node-{h.name}-a1"
    h.kube.merge_patch("node", node, {"status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}})
    h.clock.advance(minutes=1 + 10)
    h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING
    h.clock.advance(minutes=5)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-4, "CANCELLED")


def test_reclaim_wait_schedules_a_recheck(h, monkeypatch):
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "cancelled": True}]
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    later = []
    monkeypatch.setattr(h.runner, "_enqueue_later", lambda name, delay: later.append((name, delay)))
    h.runner.reconcile(h.name)
    assert later == [(h.name, runner_mod.RECLAIM_RECHECK_SECONDS)]


def test_cancelled_task_on_an_on_demand_node_is_recorded_at_once(h):
    h.cluster.capacity = "on-demand"
    h.cluster.behaviour["geekbench"] = [{"exit_code": -1, "cancelled": True}]
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-4, "CANCELLED")


INTERRUPTION = {"metadata": {"annotations": {"scarpenter.sparecores.com/interruption": "SpotInterrupted"}}}


def test_draining_node_between_steps_ends_the_attempt_at_once(h):
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.cluster.react()  # the node arrives
    h.kube.patch_obj("nodeclaim", f"claim-node-{h.name}-a1", INTERRUPTION)
    h.runner.reconcile(h.name)
    run = h.run
    assert h.pod_names("step") == [] and h.pod_names() == []  # no step on it, the anchor is released
    assert run.status.attempts[0].outcome == AttemptOutcome.SPOT_INTERRUPTED
    assert run.status.phase == RunPhase.PENDING


def test_node_draining_when_a_step_ends_records_the_step_and_ends_the_attempt(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()  # the step finishes
    h.kube.patch_obj("nodeclaim", f"claim-node-{h.name}-a1", INTERRUPTION)
    h.runner.reconcile(h.name)
    run = h.run
    assert h.codes()["geekbench"] == (0, None)
    assert run.status.attempts[0].outcome == AttemptOutcome.SPOT_INTERRUPTED and run.status.phase == RunPhase.PUBLISHING


def test_eviction_that_deletes_the_step_pod_outright_ends_the_attempt(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.kube.patch_obj("nodeclaim", f"claim-node-{h.name}-a1", INTERRUPTION)
    h.kube.delete("pod", f"{h.name}-a1-02-geekbench")  # no Failed Pod is left to read the eviction from
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.attempts[0].outcome == AttemptOutcome.SPOT_INTERRUPTED
    assert h.task("geekbench").state == TaskState.PENDING and run.status.phase == RunPhase.PENDING
    assert "geekbench" in h.storage.aborted and h.pod_names() == []
    assert [n for k, n in h.kube.created if k == "pod"].count(f"{h.name}-a1-02-geekbench") == 1  # not relaunched


# --------------------------------------------------------------------------- restart and cancel


def test_restart_mid_step_continues_from_the_objects(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    pods_before = [n for k, n in h.kube.created if k == "pod"]
    planned_before = list(h.storage.planned)

    second = h.new_runner()  # a new controller process: no caches
    second.reconcile(h.name)
    assert [n for k, n in h.kube.created if k == "pod"] == pods_before  # nothing created twice
    run = h.drive(runner=second)
    assert run.status.phase == RunPhase.PUBLISHING and all(t.exit_code == 0 for t in run.status.tasks)
    assert h.storage.planned[: len(planned_before)] == planned_before and h.storage.planned.count("geekbench") == 1


def test_restart_collects_a_step_that_ended_while_the_controller_was_down(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()  # the Pod finishes, nobody is watching
    assert h.task("geekbench").state == TaskState.RUNNING
    second = h.new_runner()
    second.reconcile(h.name)
    assert h.codes()["geekbench"] == (0, None)
    run = h.drive(runner=second)
    assert run.status.phase == RunPhase.PUBLISHING


def test_crash_between_status_json_and_status_patch_is_repeated_safely(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.cluster.react()
    h.storage.crash_after_status_json = True
    with pytest.raises(RuntimeError):
        h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING  # the commit point is written, the patch is not
    committed = json.dumps(h.storage.status["geekbench"], sort_keys=True)

    run = h.drive(runner=h.new_runner())
    assert h.codes()["geekbench"] == (0, None) and run.status.phase == RunPhase.PUBLISHING
    assert json.dumps(h.storage.status["geekbench"], sort_keys=True) == committed


def test_cancel_records_minus_four_cancelled_and_releases_the_node(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    crd.request_cancel(h.kube, h.name)
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.phase == RunPhase.CANCELLED
    assert h.codes()["geekbench"] == (-4, "CANCELLED") and h.codes()["membench"] == (-4, "CANCELLED")
    assert h.task("lscpu").exit_code == 0  # finished tasks stay
    assert h.pod_names() == [] and h.kube.list("secret") == []
    assert run.status.attempts[0].outcome == AttemptOutcome.CANCELLED
    assert "geekbench" in h.storage.aborted and h.storage.status["membench"]["reason"] == "CANCELLED"
    h.runner.reconcile(h.name)  # a terminal run stays as it is
    assert h.run.status.phase == RunPhase.CANCELLED


def test_cancel_annotation_makes_the_runner_cancel_the_run(h):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    crd.request_cancel(h.kube, h.name)
    assert h.run.cancel_requested and h.run.status.phase == RunPhase.RUNNING  # the CLI only asks
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.phase == RunPhase.CANCELLED and run.status.attempts[0].outcome == AttemptOutcome.CANCELLED
    assert h.codes()["geekbench"] == (-4, "CANCELLED") and h.codes()["membench"] == (-4, "CANCELLED")
    assert h.pod_names() == [] and "geekbench" in h.storage.aborted


def test_cancel_annotation_on_a_pending_run_and_on_a_finished_one(h):
    h.start(["geekbench"])
    crd.request_cancel(h.kube, h.name)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.CANCELLED and h.codes()["geekbench"] == (-4, "CANCELLED")
    assert h.pod_names() == []

    other = Harness()
    other.start(["geekbench"])
    other.drive()
    crd.request_cancel(other.kube, other.name)
    other.runner.reconcile(other.name)
    assert other.run.status.phase == RunPhase.PUBLISHING and other.codes()["geekbench"] == (0, None)


def test_deleted_run_leaves_no_pods_behind(h):
    h.start(["geekbench"])
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    h.kube.objects["benchmarkrun"].pop(h.name)  # plain delete, no finalizer: nothing to record on
    h.runner.reconcile(h.name)
    assert h.pod_names() == []


# --------------------------------------------------------------------------- long steps, hold, resync


def test_urls_are_resigned_hourly_and_keep_their_multipart_uploads(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    writes, signed, heartbeats = h.kube.secret_writes, h.storage.signed, len(h.kube.heartbeats)
    h.clock.advance(minutes=30)
    h.runner.reconcile(h.name)
    assert h.kube.secret_writes == writes  # not due yet
    h.clock.advance(minutes=31)
    h.runner.reconcile(h.name)
    assert h.kube.secret_writes == writes + 1 and h.storage.signed == signed + 1
    assert len(h.kube.heartbeats) == heartbeats + 1
    urls = json.loads(__import__("base64").b64decode(h.kube.get("secret", f"{h.name}-urls")["data"]["urls.json"]))
    assert urls["objects"]["geekbench/stdout"]["upload_id"].startswith("up-geekbench-stdout")
    assert h.storage.planned.count("geekbench") == 1  # no new upload slots


def test_hold_after_keeps_the_anchor_alive_with_heartbeats(h):
    h.start(["geekbench"], hold_after=timedelta(hours=2))
    h.drive(stop=lambda run: run.status.phase == RunPhase.RELEASING)
    assert h.pod_names() == [f"{h.name}-a1"]  # the held node has the anchor only
    beats = len(h.kube.heartbeats)
    h.clock.advance(hours=1)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.RELEASING and len(h.kube.heartbeats) == beats + 1
    h.clock.advance(hours=1, minutes=1)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PUBLISHING and h.pod_names() == []


def test_enqueue_deduplicates_and_resync_notices_deleted_runs(h):
    h.start(["geekbench"])
    h.runner.enqueue(h.name)
    h.runner.enqueue(h.name)
    assert h.runner._queue.qsize() == 1
    h.runner._queue.get_nowait()
    h.runner._queued.clear()
    h.runner._resync()
    assert h.runner._queue.get_nowait() == h.name
    h.runner._queued.clear()
    h.kube.objects["benchmarkrun"].pop(h.name)
    h.runner._resync()  # the LIST no longer has it, but it was known
    assert h.runner._queue.get_nowait() == h.name


def test_run_forever_drives_a_run_from_a_resync(h):
    h.start(["geekbench"])
    stop = threading.Event()
    thread = threading.Thread(target=h.runner.run_forever, args=(stop,))
    thread.start()
    try:
        for _ in range(100):
            if h.kube.list("pod"):
                break
            stop.wait(0.05)
        assert h.pod_names("anchor") == [f"{h.name}-a1"]
    finally:
        stop.set()
        thread.join(5)
    assert not thread.is_alive()


def api_error(code: int, headers: dict | None = None) -> ApiError:
    body = {"kind": "Status", "status": "Failure", "code": code, "message": "transient"}
    return ApiError(response=httpx.Response(code, json=body, headers=headers))


@pytest.fixture
def fast_backoff(monkeypatch):
    monkeypatch.setattr(runner_mod, "backoff_delay", lambda failures, error=None: 0.01)
    monkeypatch.setattr("k8s_inspector.kube.backoff_delay", lambda failures, error=None: 0.01)


def test_run_forever_survives_a_failing_initial_list(h, fast_backoff):
    """429 while the CRD is being established: retried, then the run is processed."""
    h.start(["geekbench"])
    real_list, failures = h.kube.list, [0]

    def flaky_list(kind, labels=None):
        if kind == "benchmarkrun" and failures[0] < 3:
            failures[0] += 1
            raise api_error(429, {"Retry-After": "1"})
        return real_list(kind, labels)

    h.kube.list = flaky_list
    stop = threading.Event()
    thread = threading.Thread(target=h.runner.run_forever, args=(stop,))
    thread.start()
    try:
        for _ in range(200):
            if h.kube.objects.get("pod"):
                break
            stop.wait(0.05)
        assert thread.is_alive()
        assert failures[0] == 3
        assert h.pod_names("anchor") == [f"{h.name}-a1"]
    finally:
        stop.set()
        thread.join(5)
    assert not thread.is_alive()


def test_watch_restarts_after_a_transient_error(h, fast_backoff):
    h.start(["geekbench"])
    calls = [0]

    def flaky_watch(kind, labels=None, resource_version=None):
        calls[0] += 1
        if calls[0] == 1:
            raise api_error(503)
        yield WatchEvent("ADDED", kind, h.kube.list(kind)[0])
        stop.wait()

    h.kube.watch = flaky_watch
    stop = threading.Event()
    thread = threading.Thread(target=h.runner._watch, args=("benchmarkrun", None, stop))
    thread.start()
    try:
        assert h.runner._queue.get(timeout=5) == h.name
        assert calls[0] == 2
    finally:
        stop.set()
        thread.join(5)
    assert not thread.is_alive()


def test_backoff_delay_grows_is_capped_and_honours_retry_after():
    assert 0.7 <= kube_mod.backoff_delay(1) <= 1.3
    assert 2.9 <= kube_mod.backoff_delay(3) <= 5.1
    assert kube_mod.backoff_delay(30) <= 75
    assert kube_mod.backoff_delay(1, api_error(429, {"Retry-After": "20"})) == 20
    assert kube_mod.backoff_delay(1, api_error(429, {"Retry-After": "soon"})) < 2


def test_helpers():
    assert runner_mod._quantity("15Gi") == 15 * 2**30 and runner_mod._quantity("3920m") == 3
    assert runner_mod.parse_gpu_facts(b"15360, 7.5\n81920, 8.0\n") == (15360, (7, 5))
    assert runner_mod.parse_gpu_facts(b"garbage") == (None, None)


# --------------------------------------------------------------------------- the step in flight is stored


def test_step_in_flight_is_stored_and_cleared(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    stored = h.run.status.step
    assert stored["name"] == "02-geekbench" and stored["containers"][0]["key"] == "geekbench"
    assert stored["containers"][0]["image"].startswith("ghcr.io/sparecores/")
    h.drive()
    assert h.run.status.step is None


def test_step_that_cannot_be_read_back_is_deleted_before_it_is_recorded(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.kube.objects["benchmarkrun"][h.name]["status"]["step"]["containers"][0]["key"] = "gone_task"
    h.runner.reconcile(h.name)
    assert h.pod_names("step") == []
    assert h.codes()["geekbench"] == (ExitCode.HARNESS_ERROR, "the step in flight cannot be read back from the status")
    assert h.run.status.step is None
    h.drive()  # the run goes on with what is left


def test_pod_that_does_not_match_the_stored_step_is_deleted_before_it_is_recorded(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.kube.objects["benchmarkrun"][h.name]["status"]["step"]["containers"][0]["key"] = "membench"
    h.runner.reconcile(h.name)
    assert f"{h.name}-a1-02-geekbench" not in h.pod_names()
    assert h.codes()["geekbench"][0] == ExitCode.HARNESS_ERROR


def test_restart_does_not_plan_the_running_step_again(h, monkeypatch):
    h.start(["membench", "geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    second = h.new_runner()
    monkeypatch.setattr(second, "_plan", lambda *args: pytest.fail("planned a step that is in flight"))
    second.reconcile(h.name)


def test_record_resets_published_and_commit(h):
    h.start(["geekbench"])
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    h.kube.objects["benchmarkrun"][h.name]["status"]["tasks"][0].update({"published": True, "commit": "abc"})
    run = h.run
    task = runner_mod._find(run.status, run.status.tasks[0].name)
    h.runner._record(run, None, [runner_mod._Result(steps.task_of(task.name), task.name, 0)])
    assert (h.task(task.name).published, h.task(task.name).commit) == (False, None)


# --------------------------------------------------------------------------- failures that must end the run


def fail_reconciles(h, times: int) -> None:
    for _ in range(times):
        h.runner._work(h.name)


def break_reconcile(h, monkeypatch) -> None:
    monkeypatch.setattr(h.runner, "reconcile", lambda name: (_ for _ in ()).throw(RuntimeError("boom")))


def test_outage_shorter_than_half_an_hour_does_not_end_the_run(h, monkeypatch):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    with monkeypatch.context() as outage:
        break_reconcile(h, outage)
        for _ in range(30):  # many failures, all inside the window
            fail_reconciles(h, 1)
            h.clock.advance(minutes=1)
        assert h.run.status.phase == RunPhase.RUNNING
    fail_reconciles(h, 1)  # S3 is back: the run goes on and the failing window starts afresh
    h.clock.advance(minutes=29)
    with monkeypatch.context() as outage:
        break_reconcile(h, outage)
        fail_reconciles(h, 1)
    assert h.run.status.phase == RunPhase.RUNNING
    run = h.drive()
    assert run.status.phase == RunPhase.PUBLISHING and h.codes()["geekbench"] == (0, None)


def test_reconcile_failing_for_half_an_hour_ends_the_run_without_deleting_the_anchor_first(h, monkeypatch):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    deleted_before = [d for d in h.kube.deleted if d[0] == "pod"]  # the recorded probe step's Pod
    break_reconcile(h, monkeypatch)
    fail_reconciles(h, 1)
    h.clock.advance(minutes=31)
    fail_reconciles(h, 1)
    run = h.run
    assert run.status.phase == RunPhase.FAILED
    assert h.codes()["geekbench"] == (ExitCode.HARNESS_ERROR, "the controller gave up on the run: RuntimeError")
    assert run.status.attempts[0].outcome == AttemptOutcome.HARNESS_ERROR  # not a node loss
    assert "geekbench" in h.storage.aborted  # the running task's open uploads
    assert h.kube.list("secret") == [] and h.storage.status["geekbench"]["exit_code"] == ExitCode.HARNESS_ERROR
    assert [
        d for d in h.kube.deleted if d[0] == "pod"
    ] == deleted_before  # recorded first, no Pod deleted by the give-up
    assert f"{h.name}-a1" in h.pod_names("anchor")
    monkeypatch.undo()
    h.runner.reconcile(h.name)  # the Failed phase releases the node
    assert h.pod_names() == []


def test_sku_that_is_not_a_label_value_fails_as_harness_error_without_an_anchor(h):
    h.start(["geekbench"], sku="VM.Standard.E5 Flex")  # a space cannot be a label value
    h.runner.reconcile(h.name)
    assert h.pod_names("anchor") == []
    assert h.codes()["geekbench"] == (ExitCode.HARNESS_ERROR, SKU_NOT_SELECTABLE)
    assert h.run.status.phase == RunPhase.FAILED


def test_pending_run_that_cannot_start_gets_its_tasks_so_the_lease_can_expire(h):
    h.start(["no_such_task"])
    fail_reconciles(h, 1)
    h.clock.advance(minutes=31)
    fail_reconciles(h, 1)
    run = h.run
    assert run.status.phase == RunPhase.FAILED
    assert [(t.name, t.state, t.exit_code, t.published) for t in run.status.tasks] == [
        ("no_such_task", TaskState.DONE, 256, True)
    ]


def test_urls_too_large_for_a_secret_end_the_step_with_a_harness_error(h, monkeypatch):
    real = h.storage.presign
    big = "x" * 130_000
    monkeypatch.setattr(
        h.storage, "presign", lambda plans: [p.__class__(**{**p.__dict__, "put_url": big}) for p in real(plans)]
    )
    h.start(["geekbench"])
    run = h.drive()
    assert run.status.phase == RunPhase.PUBLISHING
    code, message = h.codes()["geekbench"]
    assert code == ExitCode.HARNESS_ERROR and "cannot sign the upload URLs" in message and "urls.json" in message
    assert h.pod_names("step") == [] and h.kube.list("secret") == []


class _Presigner:
    """Just enough of a boto3 client for `Storage.plan_uploads` and `presign`: STS-sized URLs."""

    def create_multipart_upload(self, Bucket, Key):
        return {"UploadId": "u" * 64}

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        token = "FwoGZXIvYXdzE" * 110  # a session token of about 1.4 KB
        return (
            f"https://bucket.s3.eu-west-1.amazonaws.com/{Params['Key']}?X-Amz-Security-Token={token}&X-Amz-Signature="
            + "a" * 64
        )


def test_urls_secret_of_the_largest_step_stays_far_below_the_object_limit(h):
    storage = Storage.__new__(Storage)
    storage.bucket, storage.uploads, storage.s3, storage._prefix = "b", UploadSettings(), _Presigner(), ""
    probes = [t for t in tasks.all_tasks() if t.parallel]
    step = steps.probe_step(probes, index=1)
    assert len(step.containers) == 7 and step.gpu_runtime  # the GPU node's probe step
    h.start(["geekbench"])
    run = h.run
    key = crd.run_key(run)
    urls = render_urls_json(storage.presign(storage.plan_uploads(key, step.containers)))
    owner = runner_mod.OwnerRef("inspector.sparecores.com/v1alpha1", "BenchmarkRun", run.name, "u")
    secret = pods.render_urls_secret(run, urls, owner)
    assert len(base64.b64encode(secret["stringData"]["urls.json"].encode())) < 700 * 1024


def test_urls_secret_of_the_pgbench_step_stays_far_below_the_object_limit(h):
    """pgbench has the most multipart slots: resource tracker and the collected server log with `max_parts` parts."""
    storage = Storage.__new__(Storage)
    storage.bucket, storage.uploads, storage.s3, storage._prefix = "b", UploadSettings(), _Presigner(), ""
    facts = NodeFacts("n", "oci.x", "amd64", START, 1)
    step = steps.next_step([tasks.get_task("pgbench_postgres_ro_durable")], {}, facts, index=2)
    h.start(["pgbench_postgres_ro_durable"])
    run = h.run
    urls = render_urls_json(storage.presign(storage.plan_uploads(crd.run_key(run), step.containers)))
    owner = runner_mod.OwnerRef("inspector.sparecores.com/v1alpha1", "BenchmarkRun", run.name, "u")
    secret = pods.render_urls_secret(run, urls, owner)
    assert 150 < urls.count("X-Amz-Signature") < 400  # the parts of the large files are in there
    assert len(base64.b64encode(secret["stringData"]["urls.json"].encode())) < 700 * 1024


# --------------------------------------------------------------------------- partial image pull, node loss details


def test_only_the_containers_stuck_pulling_end_with_image_pull_timeout(h):
    h.cluster.behaviour["dmidecode"] = [{"pull_stuck": True}]
    h.start(["dmidecode", "geekbench"])
    h.drive(stop=lambda run: h.kube.list("pod", {"inspector.sparecores.com/task": "probe"}) != [])
    h.cluster.react()
    h.clock.advance(minutes=16)
    h.runner.reconcile(h.name)
    assert h.codes()["dmidecode"] == (-4, "IMAGE_PULL_TIMEOUT")
    assert h.codes()["lscpu"] == (0, None)  # it ran: its result stays
    assert f"{h.name}-a1-01-probes" not in h.pod_names()
    run = h.drive()
    assert h.task("geekbench").exit_code == 0 and run.status.phase == RunPhase.PUBLISHING


def probe_pod_exists(h) -> bool:
    return h.kube.list("pod", {"inspector.sparecores.com/task": "probe"}) != []


def test_stuck_pull_with_a_flush_neither_relaunches_the_step_nor_deletes_it_early(h):
    h.cluster.behaviour["dmidecode"] = [{"pull_stuck": True}]
    h.storage.incomplete["lscpu"] = 1  # lscpu ran, but its uploads need a flush
    h.start(["dmidecode"])
    h.drive(stop=lambda run: probe_pod_exists(h))
    h.cluster.react()
    h.clock.advance(minutes=16)
    h.runner.reconcile(h.name)
    probe = f"{h.name}-a1-01-probes"
    assert {probe, f"{probe}-flush1"} <= set(h.pod_names())  # the stuck Pod stays until the flush is over
    assert "dmidecode" not in h.storage.aborted  # the image may still arrive during the flush
    h.runner.reconcile(h.name)  # the flush is still going
    assert h.task("dmidecode").state == TaskState.RUNNING
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert h.codes()["dmidecode"] == (-4, "IMAGE_PULL_TIMEOUT") and h.codes()["lscpu"] == (0, None)
    assert probe not in h.pod_names()
    assert [n for k, n in h.kube.created if k == "pod"].count(probe) == 1
    assert "dmidecode" in h.storage.aborted  # aborted when the Pod is deleted and the result recorded


def test_container_waiting_on_a_bad_config_fails_after_the_pull_budget_with_its_message(h):
    message = 'secret "inspector-secrets" not found'
    h.cluster.behaviour["geekbench"] = [{"pull_stuck": "CreateContainerConfigError", "message": message}]
    h.start(["membench", "geekbench"])
    h.drive(stop=lambda run: h.kube.list("pod", {"inspector.sparecores.com/task": "geekbench"}) != [])
    h.cluster.react()
    h.clock.advance(minutes=14)
    h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-4, message)
    assert f"{h.name}-a1-02-geekbench" not in h.pod_names() and "geekbench" in h.storage.aborted
    run = h.drive()
    assert h.task("membench").exit_code == 0 and run.status.phase == RunPhase.PUBLISHING


@pytest.mark.parametrize("reason", ["InvalidImageName", "ErrImageNeverPull"])
def test_other_unresolvable_waiting_states_fail_like_a_stuck_pull(h, reason):
    h.cluster.behaviour["geekbench"] = [{"pull_stuck": reason}]
    h.start(["geekbench"])
    h.drive(stop=lambda run: h.kube.list("pod", {"inspector.sparecores.com/task": "geekbench"}) != [])
    h.cluster.react()
    h.clock.advance(minutes=16)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"] == (-4, reason)


def test_gpu_fallback_reruns_only_the_containers_that_did_not_start(h):
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.cluster.node_allocatable = {"memory": "15Gi", "cpu": "2", "nvidia.com/gpu": "1"}
    h.cluster.behaviour["dmidecode"] = [{"start_error": True}]
    h.start(["dmidecode"])
    h.storage.objects[object_key(h.run_key(), "gpu_facts", "stdout")] = b"16384, 8.0\n"
    h.drive()
    created = [n for k, n in h.kube.created if k == "pod"]
    assert f"{h.name}-a1-01-probes" in created and f"{h.name}-a1-01-probes-cpu" in created
    assert h.storage.planned.count("lscpu") == 1 and h.storage.planned.count("dmidecode") == 1
    assert h.task("dmidecode").exit_code == 0 and h.task("dmidecode").gpu_fallback
    assert h.task("lscpu").exit_code == 0 and not h.task("lscpu").gpu_fallback
    assert not h.task("gpu_facts").gpu_fallback and h.task("gpu_facts").exit_code == 0
    assert h.storage.status["lscpu"]["gpu_fallback"] is False


def test_anchor_that_ended_on_a_healthy_node_is_node_lost_not_spot(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.kube.set_status("pod", f"{h.name}-a1", {"phase": "Failed", "reason": "DeadlineExceeded"})
    h.runner.reconcile(h.name)  # stops the step Pod
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.attempts[0].outcome == AttemptOutcome.NODE_LOST
    assert h.codes()["geekbench"] == (-4, "NODE_LOST") and run.status.phase == RunPhase.PUBLISHING
    assert "configmap" not in h.kube.objects or not h.kube.list("configmap")  # no interruption recorded


def test_no_salvage_while_a_step_pod_may_still_upload(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    h.kube.objects["pod"].pop(f"{h.name}-a1")  # the anchor is gone, its Pods and the Node are not (yet)
    h.runner.reconcile(h.name)
    assert h.storage.salvaged == [] and h.run.status.phase == RunPhase.RUNNING
    assert f"{h.name}-a1-02-geekbench" not in h.pod_names() and h.kube.list("secret")  # told to stop, Secret stays
    h.runner.reconcile(h.name)  # nothing can upload any more
    assert h.storage.salvaged == ["geekbench"] and h.run.status.phase == RunPhase.PUBLISHING
    assert h.kube.list("secret") == []  # deleted last


def test_gpu_count_follows_the_node(h):
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.start(["membench"])
    h.drive(stop=in_step("01-probes"))
    h.kube.patch_obj("node", f"node-{h.name}-a1", {"status": {"allocatable": {"nvidia.com/gpu": "2"}}})
    h.runner.reconcile(h.name)
    assert h.runner._nodes[h.name].gpu_count == 2


def gpu_node(h) -> None:
    h.cluster.node_labels = {"scarpenter.sparecores.com/accelerator-manufacturer": "nvidia"}
    h.cluster.node_allocatable = {"memory": "15Gi", "cpu": "2", "nvidia.com/gpu": "1"}


def test_gpu_fallback_is_one_status_update_so_a_failed_patch_never_relaunches_the_gpu_step(h):
    gpu_node(h)
    h.cluster.behaviour["dmidecode"] = [{"start_error": True}]
    h.start(["dmidecode"])
    h.drive(stop=lambda run: probe_pod_exists(h))
    h.cluster.react()
    probe = f"{h.name}-a1-01-probes"

    real_patch = h.kube.merge_patch
    patches = []

    def failing_second_patch(kind, name, body, subresource_status=False):
        if subresource_status:
            patches.append(body)
            if len(patches) > 1:
                raise RuntimeError("API server unavailable")
        return real_patch(kind, name, body, subresource_status)

    h.kube.merge_patch = failing_second_patch
    try:
        h.runner.reconcile(h.name)  # one patch: records the others, switches to the CPU step
    finally:
        h.kube.merge_patch = real_patch
    assert len(patches) == 1
    run = h.run
    assert run.status.step["name"] == "01-probes-cpu" and run.status.current_step == "01-probes-cpu"
    assert h.task("dmidecode").state == TaskState.RUNNING and h.task("dmidecode").gpu_fallback
    assert h.task("lscpu").state == TaskState.DONE

    run = h.drive(runner=h.new_runner())  # the CPU Pod is launched by the next pass
    assert h.task("dmidecode").exit_code == 0 and run.status.phase == RunPhase.PUBLISHING
    assert [n for k, n in h.kube.created if k == "pod"].count(probe) == 1


def test_probe_facts_are_learned_after_the_cpu_fallback_of_the_probe_step(h):
    gpu_node(h)
    h.cluster.behaviour["lscpu"] = [{"start_error": True}]
    h.start(["membench"])
    h.storage.objects[object_key(h.run_key(), "gpu_facts", "stdout")] = b"16384, 8.0\n"
    h.drive(stop=in_step("02-membench"))
    nd = h.runner._nodes[h.name]
    assert nd.lscpu_stdout == LSCPU and nd.facts.lscpu["Model name:"] == "AMD EPYC 9R14"


# --------------------------------------------------------------------------- P0 findings


def geekbench_pod(h) -> str:
    return next(n for n in h.pod_names("step") if n.endswith("geekbench"))


def wedge_geekbench(h) -> str:
    """Run until the geekbench step Pod stays Running past its deadline + grace + margin."""
    h.cluster.behaviour["geekbench"] = [{"wedged": True}]
    h.start(["geekbench"])
    h.drive(stop=lambda run: any(n.endswith("geekbench") for n in h.pod_names("step")))
    h.cluster.react()
    pod = h.kube.get("pod", geekbench_pod(h))
    limit = pod["spec"]["activeDeadlineSeconds"] + pod["spec"]["terminationGracePeriodSeconds"]
    h.runner.reconcile(h.name)
    h.clock.advance(seconds=limit + 60)
    h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING  # inside the margin: still waiting
    h.clock.advance(seconds=120)
    return pod["metadata"]["name"]


def test_wedged_step_pod_is_force_deleted_and_recorded_as_deadline_exceeded(h):
    name = wedge_geekbench(h)
    h.runner.reconcile(h.name)
    assert name in h.kube.force_deleted
    assert h.codes()["geekbench"] == (-4, "DEADLINE_EXCEEDED")
    assert h.storage.status["geekbench"]["reason"] == "DEADLINE_EXCEEDED"


def test_wedged_step_pod_with_an_uploaded_result_records_that_result(h):
    name = wedge_geekbench(h)
    h.storage.objects[object_key(h.run_key(), "geekbench", "result.json")] = json.dumps(
        {"task": "geekbench", "exit_code": 143, "cancelled": True, "error_msg": "cancelled"}
    ).encode()
    h.runner.reconcile(h.name)
    assert name in h.kube.force_deleted
    assert h.codes()["geekbench"] == (-4, "DEADLINE_EXCEEDED")


def test_wedged_step_pod_whose_task_finished_keeps_its_exit_code(h):
    name = wedge_geekbench(h)
    h.storage.objects[object_key(h.run_key(), "geekbench", "result.json")] = json.dumps(
        {"task": "geekbench", "exit_code": 0}
    ).encode()
    h.runner.reconcile(h.name)
    assert name in h.kube.force_deleted
    assert h.codes()["geekbench"] == (0, None)


def test_wedged_step_pod_without_uploads_is_flushed_before_it_is_given_up(h):
    h.storage.incomplete["geekbench"] = 1
    name = wedge_geekbench(h)
    h.runner.reconcile(h.name)
    assert f"{name}-flush1" in h.pod_names() and name not in h.kube.force_deleted  # the step Pod stays for now
    h.cluster.react()
    h.runner.reconcile(h.name)
    assert name in h.kube.force_deleted and h.task("geekbench").state == TaskState.DONE


def test_pod_stuck_terminating_is_force_deleted_after_grace_plus_margin(h):
    h.start(["geekbench"])
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    anchor = h.pod_names("anchor")[0]
    h.kube.patch_obj("pod", anchor, {"metadata": {"deletionTimestamp": iso(h.clock.now + timedelta(seconds=20))}})
    h.clock.advance(seconds=20 + 100)
    h.runner.reconcile(h.name)
    assert anchor not in h.kube.force_deleted
    h.clock.advance(seconds=30)
    h.runner.reconcile(h.name)
    assert anchor in h.kube.force_deleted


def set_disk_pressure(h, on: bool) -> None:
    for node in h.kube.list("node"):
        status = "True" if on else "False"
        h.kube.patch_obj(
            "node", node["metadata"]["name"], {"status": {"conditions": [{"type": "DiskPressure", "status": status}]}}
        )


def node_with_pressure(h) -> None:
    """The anchor's node comes up with DiskPressure, before the first step."""
    h.runner.reconcile(h.name)
    h.cluster.react()
    set_disk_pressure(h, True)


def test_step_waits_while_the_node_has_disk_pressure_and_launches_when_it_clears(h):
    h.start(["geekbench"])
    node_with_pressure(h)
    for _ in range(3):
        h.runner.reconcile(h.name)
        h.clock.advance(minutes=3)
    assert h.pod_names("step") == []
    set_disk_pressure(h, False)
    h.runner.reconcile(h.name)
    assert h.pod_names("step") != []


def test_step_is_launched_anyway_when_the_pressure_outlasts_the_wait(h):
    h.start(["geekbench"])
    node_with_pressure(h)
    h.runner.reconcile(h.name)
    assert h.pod_names("step") == []
    h.clock.advance(minutes=11)
    h.runner.reconcile(h.name)
    assert h.pod_names("step") != []


def test_step_rejected_for_disk_pressure_is_relaunched_once_after_it_clears(h):
    h.cluster.behaviour["geekbench"] = [{"rejected": True}]
    h.start(["geekbench"])
    h.drive(stop=lambda run: any(n.endswith("geekbench") for n in h.pod_names("step")))
    h.cluster.react()  # the kubelet rejects it
    geekbench_name = geekbench_pod(h)
    set_disk_pressure(h, True)
    h.runner.reconcile(h.name)
    assert h.task("geekbench").state == TaskState.RUNNING  # waits: nothing recorded
    set_disk_pressure(h, False)
    h.runner.reconcile(h.name)
    pod = h.kube.get("pod", geekbench_name)
    assert pod["metadata"]["annotations"][RELAUNCH_ANNOTATION] == "1"  # read back after a restart
    run = h.drive()
    assert h.codes()["geekbench"] == (0, None)
    assert [n for k, n in h.kube.created if k == "pod" and n.endswith("geekbench")].count(geekbench_name) == 2
    assert run.status.phase == RunPhase.PUBLISHING


def test_step_rejected_twice_ends_minus_four_with_the_rejection_message(h):
    h.cluster.behaviour["geekbench"] = [{"rejected": True}, {"rejected": True}]
    h.storage.lost.add("geekbench")  # a verification would fail and start flush Pods: a never-started task has none
    h.start(["geekbench"])
    h.drive()
    assert h.codes()["geekbench"] == (-4, "Pod was rejected: The node had condition: [DiskPressure].")
    assert not [n for k, n in h.kube.created if k == "pod" and "-flush" in n]


def test_old_manifest_status_and_result_are_cleared_before_every_launch(h):
    h.cluster.behaviour["geekbench"] = [{"exit_code": 1}]
    h.start(["geekbench"])
    h.drive()
    assert h.storage.cleared.count("geekbench") == 1 and "lscpu" in h.storage.cleared
    assert h.storage.cleared.index("lscpu") < h.storage.cleared.index("geekbench")


def node_pod(name: str, node: str, cpu: str) -> dict:
    return {
        "metadata": {"name": name, "namespace": "kube-system"},
        "spec": {"nodeName": node, "containers": [{"resources": {"requests": {"cpu": cpu}}}]},
        "status": {"phase": "Running"},
    }


def step_cpu_requests(h) -> list[str]:
    return [
        c["resources"]["requests"]["cpu"]
        for n in h.pod_names("step")
        for c in h.kube.get("pod", n)["spec"]["containers"]
    ]


def test_step_pod_requests_the_cpu_the_other_pods_leave_on_the_node(h):
    h.start(["geekbench"])
    h.kube.foreign_pods.append(node_pod("flannel", f"node-{h.name}-a1", "500m"))
    h.kube.foreign_pods.append(node_pod("elsewhere", "other-node", "1"))
    h.drive(stop=lambda run: any(n.endswith("geekbench") for n in h.pod_names("step")))
    # allocatable cpu 2 (the fake node) minus flannel's 500m, the anchor's 1m and the margin (the largest Pod, 500m)
    assert set(step_cpu_requests(h)) == {"999m"}
    assert [c.get("limits", {}).get("cpu") for c in h.kube.get("pod", geekbench_pod(h))["spec"]["containers"]] == [None]


def test_step_pod_requests_the_floor_when_the_node_pods_cannot_be_listed(h):
    h.kube.node_pods_forbidden = True
    h.start(["geekbench"])
    h.drive(stop=lambda run: any(n.endswith("geekbench") for n in h.pod_names("step")))
    assert set(step_cpu_requests(h)) == {"10m"}


def refused_cpu_pod(h) -> dict:
    h.drive(stop=lambda run: any(n.endswith("geekbench") for n in h.pod_names("step")))
    h.cluster.react()  # the kubelet refuses it
    return h.kube.get("pod", geekbench_pod(h))


def cpu_of(pod: dict) -> str:
    return pod["spec"]["containers"][0]["resources"]["requests"]["cpu"]


def test_step_refused_for_cpu_is_recomputed_once_then_gets_the_floor(h):
    h.cluster.behaviour["geekbench"] = [{"out_of_cpu": True}, {"out_of_cpu": True}]
    h.start(["geekbench"])
    pod = refused_cpu_pod(h)
    name = pod["metadata"]["name"]
    assert cpu_of(pod) == "1899m"
    h.kube.foreign_pods.append(node_pod("newcomer", f"node-{h.name}-a1", "300m"))  # appeared since
    h.runner.reconcile(h.name)
    pod = h.kube.get("pod", name)
    assert pod["metadata"]["annotations"][CPU_REFUSALS_ANNOTATION] == "1"  # read back after a restart
    assert cpu_of(pod) == "1399m"  # recomputed: 2000 - anchor 1m - newcomer 300m - margin 300m (the largest Pod)
    h.cluster.react()  # refused again
    h.runner.reconcile(h.name)
    pod = h.kube.get("pod", name)
    assert pod["metadata"]["annotations"][CPU_REFUSALS_ANNOTATION] == "2"
    assert cpu_of(pod) == "10m"
    run = h.drive()
    assert h.codes()["geekbench"] == (0, None)
    assert run.status.phase == RunPhase.PUBLISHING


def test_step_rejected_for_cpu_three_times_ends_minus_four_with_the_rejection_message(h):
    h.cluster.behaviour["geekbench"] = [{"out_of_cpu": True}] * 3
    h.storage.lost.add("geekbench")
    h.start(["geekbench"])
    h.drive()
    code, message = h.codes()["geekbench"]
    assert code == -4 and message.startswith("Pod was rejected: Node didn't have enough resource: cpu")


def test_nice_of_the_task_reaches_status_json_and_meta(h):
    h.start(["geekbench"])
    h.drive()
    assert h.storage.status["geekbench"]["nice"] == -20


def test_relaunch_markers_carry_over_so_mixed_refusals_stay_bounded(h):
    h.cluster.behaviour["geekbench"] = [{"out_of_cpu": True}, {"rejected": True}, {"out_of_cpu": True}] + [
        {"rejected": True}
    ] * 10
    h.storage.lost.add("geekbench")
    h.start(["geekbench"])
    h.drive()
    pods_created = [n for k, n in h.kube.created if k == "pod" and n.endswith("geekbench")]
    # CPU refusal, DiskPressure rejection (keeps the CPU count), CPU refusal (floor), DiskPressure again: no more
    assert len(pods_created) == 4
    assert h.codes()["geekbench"][0] == -4


def test_a_different_reason_restarts_the_count_so_ice_never_makes_boot_failure_final(h):
    for reason_claim in (_unlaunched_claim, _unlaunched_claim, _launched_claim):
        h.cluster.provision_enabled = False
        h.start(["geekbench"])
        h.runner.reconcile(h.name)
        reason_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim")
        h.clock.advance(hours=3)
        h.runner.reconcile(h.name)
        h.kube.objects.pop("nodeclaim", None)
        h.runner = h.new_runner()
    assert h.kube.list("benchmarkrun") == []
    assert h.entry()["reason"] == "BOOT_FAILURE" and h.entry()["count"] == 1


def test_a_permanent_rejection_removes_the_skus_entry(h):
    launch.record_failure(h.kube, "oci", "VM.Standard.E5.Flex.2-16", "ICE", h.clock.now)
    h.cluster.provision_enabled = False
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    _launch_failed(h, "bench-aaaaa", "Fatal (X): first")
    _launch_failed(h, "bench-bbbbb", "Fatal (X): second")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert launch.read(h.kube, "oci") == {}


def test_a_forced_run_records_its_temporary_failure(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"], force=True)
    h.runner.reconcile(h.name)
    h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), "InsufficientCapacity (X): none")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert h.codes()["geekbench"][1].startswith("SCARPENTER_ICE")


def test_a_daily_limit_waits_until_the_offered_until(h):
    until = h.clock.now + timedelta(hours=9)
    document = {
        "generatedAt": h.clock.now.isoformat(),
        "truncated": False,
        "types": {
            "oci.x": {
                "apiReference": "VM.Standard.E5.Flex.2-16",
                "available": False,
                "capacityTypes": [],
                "reason": "UnavailableCache",
                "offerings": [
                    {
                        "capacityType": "on-demand",
                        "zones": ["z1"],
                        "available": False,
                        "reason": "QuotaExceeded",
                        "until": (until - timedelta(hours=8)).isoformat(),
                    },
                    {
                        "capacityType": "on-demand",
                        "zones": ["z2"],
                        "available": False,
                        "reason": "DailyLimit",
                        "until": until.isoformat(),
                    },
                ],
            }
        },
    }
    h.kube.objects["configmap/scarpenter-system"] = {
        "scarpenter-offered-bench-oci": {
            "metadata": {"name": "scarpenter-offered-bench-oci"},
            "data": {"types.json": json.dumps(document)},
        }
    }
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), "DailyLimit (LimitExceeded): daily limit")
    h.runner.reconcile(h.name)
    assert datetime.fromisoformat(h.entry()["next"]) == until


def test_step_pod_requests_no_memory_and_limits_from_the_node_allocatable(h):
    h.cluster.node_allocatable = {"memory": "77Mi", "cpu": "1"}  # capacity of the fake node is 16Gi
    h.start(["geekbench"])
    h.drive(stop=lambda run: any(n.endswith("geekbench") for n in h.pod_names("step")))
    resources = h.kube.get("pod", geekbench_pod(h))["spec"]["containers"][0]["resources"]
    assert resources["requests"]["memory"] == "0"
    assert resources["limits"]["memory"] == str(int(0.85 * 77 * 1024**2))


# --------------------------------------------------------------------------- review follow-ups


def test_an_interrupted_claim_that_then_vanishes_is_not_a_boot_failure(h):
    _registered_run(h, 5)
    h.kube.merge_patch(
        "nodeclaim",
        "claim-1",
        {"metadata": {"deletionTimestamp": iso(h.clock.now), "annotations": {spot.INTERRUPTION_ANNOTATION: "x"}}},
    )
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.kube.objects.pop("nodeclaim")  # the finalizer is done
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING and h.run.status.uninitialized_claims == []


def test_a_second_claim_does_not_fail_a_run_whose_first_claim_is_initialized(h):
    _boot_run(h)
    _set_conditions(h, "claim-1", Launched=True, Registered=True, Initialized=True)
    _launched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim-2")  # Karpenter made a second one
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=9)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.kube.delete("nodeclaim", "claim-2")
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_an_anchor_that_never_gets_ready_on_an_initialized_node_ends_at_the_anchor_ready_limit(h):
    _boot_run(h)
    _set_conditions(h, "claim-1", Launched=True, Registered=True, Initialized=True)
    h.clock.advance(minutes=14)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ANCHOR_FAILED"


def test_a_node_initialized_just_before_the_budget_gets_its_own_anchor_ready_time(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=58)
    _launched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim-1")
    _set_conditions(h, "claim-1", Launched=True, Registered=True)
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=3)  # past the 60 min budget of the attempt
    _set_conditions(h, "claim-1", Launched=True, Registered=True, Initialized=True)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING  # the image is still being pulled
    h.clock.advance(minutes=16)
    h.runner.reconcile(h.name)
    assert h.entry()["reason"] == "ANCHOR_FAILED"


def test_a_spot_claim_that_vanishes_unseen_is_not_a_boot_failure(h):
    spot_labels = {"karpenter.sh/capacity-type": "spot"}
    _registered_run(h, 5, labels=spot_labels)  # remembered with its capacity type
    assert h.run.status.uninitialized_claims[0].capacity_type == "spot"
    h.kube.objects.pop("nodeclaim")  # annotated, deleted and finalised between two passes
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING and h.run.status.uninitialized_claims == []


def test_a_spot_claim_being_deleted_unseen_is_not_a_boot_failure(h):
    _registered_run(h, 5, labels={"karpenter.sh/capacity-type": "spot"})
    h.kube.merge_patch("nodeclaim", "claim-1", {"metadata": {"deletionTimestamp": iso(h.clock.now)}})
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING


def test_an_on_demand_claim_that_vanishes_unseen_is_a_boot_failure(h):
    _registered_run(h, 5, labels={"karpenter.sh/capacity-type": "on-demand"})
    h.kube.objects.pop("nodeclaim")
    h.clock.advance(minutes=1)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"


def test_an_anchor_that_ended_before_running_is_anchor_failed_not_boot_failure(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.kube.set_status("pod", f"{h.name}-a1", {"phase": "Failed", "reason": "Evicted"})
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ANCHOR_FAILED"


def test_a_missing_registered_time_falls_back_to_the_launch_time(h):
    _boot_run(h)
    h.kube.merge_patch(
        "nodeclaim",
        "claim-1",
        {"status": {"conditions": [{"type": "Launched", "status": "True"}, {"type": "Registered", "status": "True"}]}},
    )
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=16)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"


def test_unknown_task_names_and_a_bad_accelerator_label_do_not_raise(h):
    claim = {"metadata": {"labels": {"scarpenter.sparecores.com/accelerator-count": "many"}}}
    assert runner_mod._has_gpu(claim) is False
    assert runner_mod._has_gpu({"metadata": {"labels": {}}}) is False  # a CPU type has no label
    assert runner_mod._has_gpu({"metadata": {"labels": {"scarpenter.sparecores.com/accelerator-count": "8"}}})
    assert runner_mod._has_gpu({"metadata": {"labels": {"scarpenter.sparecores.com/accelerator-count": "0"}}}) is False
    h.start(["geekbench"])
    run = h.run
    run.spec.tasks.append("no_such_task")
    assert h.runner._is_gpu(run, []) is False


def test_a_gpu_sku_gets_the_long_registration_limit(h):
    h.cluster.provision_enabled = False
    h.start(["nvidia_smi"])
    h.runner.reconcile(h.name)
    _launched_claim(h, h.kube.get("pod", f"{h.name}-a1"), "claim-1")
    h.clock.advance(minutes=13)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []


def test_a_spot_gpu_loop_is_bounded_by_two_lost_spot_claims_then_on_demand(h):
    """SCarpenter deletes each GPU claim whose NVIDIA check fails and Karpenter launches the next VM; a SpotFirst
    run gives up on spot after two such claims, and the on-demand attempt ends at the first one."""
    labels = {"scarpenter.sparecores.com/accelerator-count": "1", "karpenter.sh/capacity-type": "spot"}
    h.cluster.provision_enabled = False
    h.start(["nvidia_smi"], capacity=CapacityMode.SPOT_FIRST)
    h.runner.reconcile(h.name)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    spot_vms = 0
    while h.run.status.attempt == 1 and h.run.status.phase == RunPhase.PROVISIONING:
        assert spot_vms < 5, "the spot loop is not bounded"
        spot_vms += 1
        _launched_claim(h, anchor, f"claim-{spot_vms}")
        _set_conditions(h, f"claim-{spot_vms}", labels=labels, Launched=True, Registered=True)
        h.runner.reconcile(h.name)  # seen registered
        h.clock.advance(minutes=30)
        h.kube.delete("nodeclaim", f"claim-{spot_vms}")  # the check failed, the claim is gone
        h.runner.reconcile(h.name)
    assert spot_vms == 2
    assert h.run.status.phase == RunPhase.PENDING and h.run.status.attempts[0].outcome == AttemptOutcome.LAUNCH_FAILED
    h.runner.reconcile(h.name)  # attempt 2: on-demand only
    assert h.run.status.attempt == 2 and h.run.status.spot_reclaims == 0
    anchor = h.kube.get("pod", f"{h.name}-a2")
    od_labels = {**labels, "karpenter.sh/capacity-type": "on-demand"}
    _launched_claim(h, anchor, "claim-od")
    _set_conditions(h, "claim-od", labels=od_labels, Launched=True, Registered=True)
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=30)
    h.kube.delete("nodeclaim", "claim-od")
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "BOOT_FAILURE"


def _offer(h, available: bool, until: timedelta | None = None, capacity_types=("on-demand",)) -> None:
    """SCarpenter's offered-types ConfigMap for the SKU, with one offering (marked until `until` when unavailable)."""
    offering = {"capacityType": "on-demand", "zones": ["z1"], "available": available}
    if until is not None:
        offering["until"] = (h.clock.now + until).isoformat()
    entry = {
        "apiReference": "VM.Standard.E5.Flex.2-16",
        "available": available,
        "capacityTypes": list(capacity_types) if available else [],
        "offerings": [offering],
    }
    document = {"generatedAt": h.clock.now.isoformat(), "types": {"oci.x": entry}}
    h.kube.objects["configmap/scarpenter-system"] = {
        "scarpenter-offered-bench-oci": {
            "metadata": {"name": "scarpenter-offered-bench-oci"},
            "data": {"types.json": json.dumps(document)},
        }
    }


def _two_ice_failures(h) -> None:
    h.cluster.provision_enabled = False
    h.start(["geekbench"], capacity=CapacityMode.ON_DEMAND)
    h.runner.reconcile(h.name)
    for name in ("bench-aaaaa", "bench-bbbbb"):
        _launch_failed(h, name, "InsufficientCapacity (X): none")


def test_ice_launch_failures_go_on_while_scarpenter_offers_the_sku(h):
    _offer(h, available=True)
    _two_ice_failures(h)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING and len(h.run.status.launch_failures) == 2
    _offer(h, available=False)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ICE"  # deferred: the run is gone


def test_ice_launch_failures_stop_at_the_cap_without_a_readable_listing(h):
    _two_ice_failures(h)  # no ConfigMap
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ICE"


def test_ice_launch_failures_end_with_the_provision_budget_while_the_sku_is_offered(h):
    _offer(h, available=True)
    _two_ice_failures(h)
    h.runner.reconcile(h.name)
    h.clock.advance(hours=2)
    _offer(h, available=True)  # a fresh listing
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ICE"


def test_ice_events_beyond_the_old_window_stay_stable_and_the_cap_ends_the_run(h):
    _offer(h, available=True)
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    for n in range(7):
        _ice_event(h, f"bench-{n}", lastTimestamp=iso(h.clock.now + timedelta(seconds=n)))
    h.runner.reconcile(h.name)
    before = [f.node_claim for f in h.run.status.launch_failures]
    assert len(before) == 7
    for _ in range(3):
        h.runner.reconcile(h.name)
    assert [f.node_claim for f in h.run.status.launch_failures] == before
    for n in range(7, 20):
        _ice_event(h, f"bench-{n}", lastTimestamp=iso(h.clock.now + timedelta(seconds=n)))
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == [] and h.entry()["reason"] == "ICE"


def test_ice_failures_of_an_on_demand_attempt_need_an_on_demand_offering(h):
    _offer(h, available=True, capacity_types=("spot",))
    _two_ice_failures(h)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []


@pytest.mark.parametrize(
    "tail", [NO_QUOTA, NO_CANDIDATE, ICE_TAIL.format("Unsupported (InvalidFlavor): flavor not found")]
)
def test_ice_failures_without_a_capacity_class_stop_at_the_cap_of_two(h, tail):
    _offer(h, available=True)
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    for claim in ("bench-aaaaa", "bench-bbbbb"):
        _ice_event(h, claim, tail)
    h.runner.reconcile(h.name)
    assert h.kube.list("benchmarkrun") == []


def test_ice_failures_with_an_unschedulable_anchor_are_not_a_missing_offering(h):
    _offer(h, available=True)
    _provisioning_without_nodes(h, capacity=CapacityMode.ON_DEMAND)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    anchor["status"]["conditions"] = [{"type": "PodScheduled", "status": "False", "message": "0/3 nodes"}]
    h.kube.objects["pod"][f"{h.name}-a1"] = anchor
    _ice_event(h, "bench-aaaaa", lastTimestamp=iso(h.clock.now))
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=20)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
