"""Runner tests: a fake Kube, a fake Storage and a scripted cluster (tests/fakes.py), real catalogue tasks.

Each test drives `Runner.reconcile` the way the watch would: reconcile, let the cluster react (provision the
anchor's node, finish step Pods as scripted), repeat.
"""

import json
import threading
from datetime import UTC, datetime, timedelta

import pytest
from fakes import Clock, Cluster, FakeKube, FakeStorage, iso

from k8s_inspector import crd
from k8s_inspector import runner as runner_mod
from k8s_inspector.model import (
    AttemptOutcome,
    CapacityMode,
    Config,
    EnvConfig,
    NodePools,
    RunPhase,
    RunSpec,
    Settings,
    TaskState,
    VendorSettings,
)
from k8s_inspector.runner import Runner
from k8s_inspector.storage import object_key

START = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
LSCPU = json.dumps(
    {"lscpu": [{"field": "Model name:", "data": "AMD EPYC 9R14"}, {"field": "Architecture:", "data": "x86_64"}]}
)


def make_config(**settings) -> Config:
    vendors = {"oci": VendorSettings(enabled=True, node_pools=NodePools(cpu="bench-oci", gpu="bench-oci-gpu"))}
    env = EnvConfig(bucket="b", bucket_region="r", sc_run_image="ghcr.io/sparecores/sc-run@sha256:abc")
    return Config(env=env, settings=Settings(vendors=vendors, **settings))


class Harness:
    def __init__(self, cfg: Config | None = None) -> None:
        self.clock = Clock(START)
        self.kube = FakeKube(self.clock)
        self.storage = FakeStorage()
        self.cluster = Cluster(self.kube, self.clock)
        self.cfg = cfg or make_config()
        self.runner = self.new_runner()
        self.name = ""

    def new_runner(self) -> Runner:
        return Runner(self.kube, self.storage, lambda: self.cfg, "inspector", clock=self.clock)

    def start(self, tasks: list[str], **spec) -> None:
        spec = RunSpec(vendor="oci", sku="VM.Standard.E5.Flex.2-16", run_id="r20261120t1000-aaaa", tasks=tasks, **spec)
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
    assert set(timing) == {"api_end", "node_registered", "node_ready", "machine_start"}
    assert h.storage.run_json["attempts"][0]["region"] == "eu-test-1"
    assert set(h.storage.run_json["plan"]) == {"membench", "geekbench", "lscpu"}
    # the anchor was kept alive between steps
    assert h.kube.heartbeats and h.kube.heartbeats[0][1] == "/k8si/runs/r20261120t1000-aaaa"
    # the URLs of a step are signed before its Pod exists, one set of upload slots per task
    assert h.storage.planned == ["lscpu", "geekbench", "membench"]


def test_probe_step_always_has_lscpu_and_learns_facts(h):
    h.start(["geekbench"])
    h.drive(stop=in_step("02-geekbench"))
    probe = h.kube.get("pod", f"{h.name}-a1-01-probes")
    assert [c["name"] for c in probe["spec"]["containers"]] == ["lscpu"]
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
    assert h.kube.get("pod", f"{h.name}-a1-02-membench")["spec"]["runtimeClassName"] == "nvidia"
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


def test_capacity_error_on_the_nodeclaim_fails_the_run_with_minus_one(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), "InsufficientInstanceCapacity in zone")
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.phase == RunPhase.FAILED
    assert h.codes()["geekbench"] == (-1, "SCARPENTER_ICE: InsufficientInstanceCapacity in zone")
    assert h.pod_names() == []
    assert run.status.attempts[0].outcome == AttemptOutcome.PROVISIONING_FAILED


def test_provisioning_deadline_fails_the_run(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.clock.advance(minutes=59)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.PROVISIONING
    h.clock.advance(minutes=2)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.FAILED
    assert h.task("geekbench").exit_code == -1 and "no node after 60 minutes" in h.task("geekbench").error_msg


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
    assert "membench" in h.storage.salvaged
    assert h.kube.get("configmap", "k8s-inspector-spot")["data"]  # per-SKU memory

    run = h.drive()
    assert run.status.attempt == 2 and run.status.phase == RunPhase.PUBLISHING
    assert h.task("geekbench").attempt == 1 and h.task("membench").attempt == 2
    assert [a.outcome for a in run.status.attempts] == [AttemptOutcome.SPOT_INTERRUPTED, AttemptOutcome.COMPLETED]
    # attempt 2 runs the probe step again, but not geekbench
    created = [n.split("-a2-")[1] for k, n in h.kube.created if k == "pod" and "-a2-" in n]
    assert created == ["01-probes", "02-membench"]
    assert [a["outcome"] for a in h.storage.run_json["attempts"]] == ["SpotInterrupted", "Completed"]


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


def test_draining_node_gets_no_new_step(h):
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.cluster.react()  # the node arrives
    notice = {"metadata": {"annotations": {"scarpenter.sparecores.com/interruption": "SpotInterrupted"}}}
    h.kube.patch_obj("nodeclaim", f"claim-node-{h.name}-a1", notice)
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.RUNNING and h.pod_names("step") == []


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
    h.runner.cancel(h.name)
    run = h.run
    assert run.status.phase == RunPhase.CANCELLED
    assert h.codes()["geekbench"] == (-4, "CANCELLED") and h.codes()["membench"] == (-4, "CANCELLED")
    assert h.task("lscpu").exit_code == 0  # finished tasks stay
    assert h.pod_names() == [] and h.kube.list("secret") == []
    assert run.status.attempts[0].outcome == AttemptOutcome.CANCELLED
    assert "geekbench" in h.storage.salvaged and h.storage.status["membench"]["reason"] == "CANCELLED"
    h.runner.reconcile(h.name)  # a terminal run stays as it is
    assert h.run.status.phase == RunPhase.CANCELLED


def test_run_with_a_deletion_timestamp_is_cancelled_by_reconcile(h):
    h.start(["geekbench"])
    h.drive(stop=lambda run: run.status.phase == RunPhase.RUNNING)
    h.kube.patch_obj("benchmarkrun", h.name, {"metadata": {"deletionTimestamp": iso(h.clock.now)}})
    h.runner.reconcile(h.name)
    assert h.run.status.phase == RunPhase.CANCELLED and h.codes()["geekbench"] == (-4, "CANCELLED")


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
    assert h.pod_names("anchor") == [f"{h.name}-a1"]
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


def test_helpers():
    assert runner_mod._quantity("15Gi") == 15 * 2**30 and runner_mod._quantity("3920m") == 3
    assert runner_mod.parse_gpu_facts(b"15360, 7.5\n81920, 8.0\n") == (15360, (7, 5))
    assert runner_mod.parse_gpu_facts(b"garbage") == (None, None)
    assert runner_mod.scarpenter_error("quota exceeded: limit 0").startswith("SCARPENTER_QUOTA_ZERO")
    assert runner_mod.scarpenter_error("no offering matches").startswith("SCARPENTER_NO_OFFERING")
    assert runner_mod.scarpenter_error("vCPU quota exceeded").startswith("SCARPENTER_QUOTA:")
    assert runner_mod.scarpenter_error("boom", "BOOT_FAILURE").startswith("SCARPENTER_BOOT_FAILURE")
