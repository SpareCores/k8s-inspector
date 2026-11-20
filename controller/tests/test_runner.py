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

from k8s_inspector import crd, pods, steps, tasks
from k8s_inspector import kube as kube_mod
from k8s_inspector import runner as runner_mod
from k8s_inspector.kube import WatchEvent
from k8s_inspector.model import (
    AttemptOutcome,
    CapacityMode,
    Config,
    EnvConfig,
    ExitCode,
    NodeFacts,
    RunPhase,
    RunSpec,
    Settings,
    TaskState,
    UploadSettings,
    VendorSettings,
)
from k8s_inspector.runner import RELAUNCH_ANNOTATION, SKU_NOT_SELECTABLE, Runner
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
        self.runner = self.new_runner()
        self.name = ""

    def new_runner(self) -> Runner:
        return Runner(self.kube, self.storage, lambda: self.cfg, "inspector", clock=self.clock)

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


def test_unlaunched_nodeclaim_at_the_deadline_is_boot_failure_not_ice(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    anchor = h.kube.get("pod", f"{h.name}-a1")
    selector = anchor["spec"]["nodeSelector"]
    h.kube.create(  # as Karpenter creates it: pool label only, vendor and SKU in spec.requirements
        "nodeclaim",
        {
            "metadata": {
                "name": "claim-unlaunched",
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
    h.clock.advance(hours=3)
    h.runner.reconcile(h.name)
    assert h.codes()["geekbench"][1].startswith("SCARPENTER_BOOT_FAILURE")


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


def test_capacity_error_on_the_nodeclaim_fails_the_run_with_minus_one(h):
    h.cluster.provision_enabled = False
    h.start(["geekbench"])
    h.runner.reconcile(h.name)
    h.cluster.fail_launch(h.kube.get("pod", f"{h.name}-a1"), "InsufficientInstanceCapacity in zone")
    h.runner.reconcile(h.name)
    run = h.run
    assert run.status.phase == RunPhase.FAILED
    assert h.codes()["geekbench"] == (-1, "SCARPENTER_ICE: InsufficientInstanceCapacity in zone")
    assert h.pod_names() == [f"{h.name}-a1"]  # recorded first, the anchor goes with the next pass
    h.runner.reconcile(h.name)
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
    assert runner_mod.scarpenter_error("quota exceeded: limit 0").startswith("SCARPENTER_QUOTA_ZERO")
    assert runner_mod.scarpenter_error("no offering matches").startswith("SCARPENTER_NO_OFFERING")
    assert runner_mod.scarpenter_error("vCPU quota exceeded").startswith("SCARPENTER_QUOTA:")
    assert runner_mod.scarpenter_error("boom", "BOOT_FAILURE").startswith("SCARPENTER_BOOT_FAILURE")


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
    assert not [d for d in h.kube.deleted if d[0] == "pod"]  # recorded first, no Pod deleted by the give-up
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
