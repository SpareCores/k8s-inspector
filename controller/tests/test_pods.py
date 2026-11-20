import json
from datetime import UTC, datetime, timedelta

import pytest

from k8s_inspector import pods
from k8s_inspector.model import (
    BenchmarkRun,
    CapacityType,
    Config,
    Deadlines,
    EnvConfig,
    NodePools,
    OwnerRef,
    PodRole,
    RunSpec,
    Settings,
    Step,
    StepContainer,
    TaskDef,
    VendorSettings,
)

NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
OWNER = OwnerRef("v1", "Pod", "run-a1", "uid-1")


@pytest.fixture
def cfg() -> Config:
    settings = Settings(
        vendors={"oci": VendorSettings(enabled=True, node_pools=NodePools(cpu="bench-oci", gpu="bench-oci-gpu"))}
    )
    env = EnvConfig(bucket="b", bucket_region="r", sc_run_image="ghcr.io/sparecores/sc-run@sha256:abc")
    return Config(env=env, settings=settings)


@pytest.fixture
def run() -> BenchmarkRun:
    return BenchmarkRun(
        name="oci-vm-1-3f9c1a7e",
        namespace="inspector",
        uid="u",
        resource_version="1",
        created=NOW,
        spec=RunSpec(vendor="oci", sku="VM.Standard.E5.Flex.2-16", run_id="r1"),
    )


def task(name="membench", **kw) -> TaskDef:
    defaults = dict(
        name=name,
        image=f"ghcr.io/sparecores/{name}:main",
        command="-Hv -t 1800",
        timeout=timedelta(minutes=40),
        entrypoint=("/usr/local/bin/resource-tracker", "--", "/usr/local/bin/membench"),
        version_command="-V",
    )
    return TaskDef(**(defaults | kw))


def container(t: TaskDef, **kw) -> StepContainer:
    return StepContainer(key=kw.pop("key", t.name), task=t, image=f"{t.image}@sha256:d", **kw)


def step(*containers: StepContainer, **kw) -> Step:
    return Step(index=7, name="07-" + containers[0].key, containers=containers, **kw)


def test_names_and_labels(run):
    s = step(container(task()))
    assert pods.anchor_name(run.name, 2) == "oci-vm-1-3f9c1a7e-a2"
    assert pods.step_pod_name(run.name, 2, s) == "oci-vm-1-3f9c1a7e-a2-07-membench"
    assert pods.flush_pod_name(run.name, 2, s, 1).endswith("-07-membench-flush1")
    assert pods.urls_secret_name(run.name) == "oci-vm-1-3f9c1a7e-urls"
    assert pods.labels(run, PodRole.STEP, 2, "membench") == {
        "inspector.sparecores.com/run": run.name,
        "inspector.sparecores.com/role": "step",
        "inspector.sparecores.com/vendor": "oci",
        "inspector.sparecores.com/attempt": "2",
        "inspector.sparecores.com/task": "membench",
    }


def test_node_pool(run, cfg):
    assert pods.node_pool(run, cfg, gpu=False, lowmem=False) == "bench-oci"
    assert pods.node_pool(run, cfg, gpu=True, lowmem=False) == "bench-oci-gpu"
    with pytest.raises(ValueError):
        pods.node_pool(run, cfg, gpu=False, lowmem=True)


def test_anchor(run, cfg):
    owner = OwnerRef("inspector.sparecores.com/v1alpha1", "BenchmarkRun", run.name, "u")
    pod = pods.render_anchor(
        run,
        1,
        cfg,
        nodepool="bench-oci",
        capacity=[CapacityType.SPOT, CapacityType.ON_DEMAND],
        deadline_seconds=100,
        owner=owner,
    )
    spec = pod["spec"]
    assert pod["metadata"]["annotations"] == {"karpenter.sh/do-not-disrupt": "true"}
    assert "blockOwnerDeletion" not in pod["metadata"]["ownerReferences"][0]
    assert spec["nodeSelector"] == {
        "karpenter.sh/nodepool": "bench-oci",
        "node.kubernetes.io/instance-type": "oci.VM.Standard.E5.Flex.2-16",
    }
    expr = spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]
    assert expr["matchExpressions"][0]["values"] == ["spot", "on-demand"]
    assert spec["tolerations"] == [{"key": "frankenscube/bench", "operator": "Exists"}]
    assert spec["activeDeadlineSeconds"] == 100
    assert spec["automountServiceAccountToken"] is False
    c = spec["containers"][0]
    assert c["args"] == ["hold", "--install", "/k8si/bin", "--heartbeat-dir", "/k8si/runs/r1", "--idle-timeout", "45m"]
    assert c["readinessProbe"]["exec"]["command"] == ["/sc-run", "ready", "/k8si/bin"]


def test_deadlines(cfg):
    s = step(container(task()))  # 40 min task
    assert pods.step_deadline_seconds(s, cfg) == 4260  # spikes/p0/step-membench.yaml
    probes = step(
        container(task("a", timeout=timedelta(minutes=5))), container(task("b", timeout=timedelta(seconds=30)))
    )
    assert pods.step_deadline_seconds(probes, cfg) == 2160
    assert pods.anchor_deadline_seconds([s, probes], cfg) == 4260 + 2160 + 3600
    cfg2 = Config(cfg.env, Settings(deadlines=Deadlines(image_pull_minutes=1)))
    assert pods.step_deadline_seconds(s, cfg2) == (1 + 1 + 40 + 10 + 5) * 60


def test_step_pod_like_spike(run, cfg):
    pod = pods.render_step(
        run, 1, step(container(task())), cfg, node_name="n1", node_allocatable_memory_bytes=1000, owner=OWNER
    )
    spec = pod["spec"]
    assert spec["nodeName"] == "n1"
    assert spec["activeDeadlineSeconds"] == 4260
    assert spec["terminationGracePeriodSeconds"] == 20
    assert spec["hostNetwork"] and spec["dnsPolicy"] == "Default" and not spec["hostPID"]
    assert spec["automountServiceAccountToken"] is False
    assert "runtimeClassName" not in spec
    assert pod["metadata"]["labels"]["inspector.sparecores.com/task"] == "membench"
    c = spec["containers"][0]
    assert c["command"] == [
        "/k8si/bin/sc-run", "exec", "--task", "membench", "--timeout", "40m",
        "--multipart-mib", "32", "--reput", "60s",
        "--spool", "/k8si/runs/r1/membench", "--urls", "/k8si-urls/urls.json",
        "--heartbeat-dir", "/k8si/runs/r1",
        "--version-cmd", "/usr/local/bin/resource-tracker -- /usr/local/bin/membench -V",
        "--", "/usr/local/bin/resource-tracker", "--", "/usr/local/bin/membench", "-Hv", "-t", "1800",
    ]  # fmt: skip
    env = {e["name"]: e["value"] for e in c["env"]}
    assert env["TRACKER_OUTPUT"] == "/k8si/runs/r1/membench/resource_tracker.jsonl"
    assert env["TRACKER_EXTERNAL_RUN_ID"] == "r1"
    assert c["securityContext"] == {"privileged": True, "seccompProfile": {"type": "Unconfined"}}
    assert c["resources"] == {"requests": {"cpu": "10m", "memory": "64Mi"}}
    assert c["terminationMessagePolicy"] == "File"
    assert [v["name"] for v in spec["volumes"]] == ["k8si", "urls"]
    assert spec["volumes"][1]["secret"]["secretName"] == f"{run.name}-urls"


def test_memory_cap_models_shm_secrets_rlimits_hostpid(run, cfg):
    t = task(
        "vllm",
        memory_cap=0.85,
        shm_gib=4,
        models_cache=True,
        secrets=("geekbench",),
        env=(("A", "1"),),
        privileges=frozenset({"host_pid", "db_rlimits"}),
        collect=("/x:y",),
    )
    pod = pods.render_step(
        run,
        1,
        step(container(t, env=(("A", "2"),))),
        cfg,
        node_name="n",
        node_allocatable_memory_bytes=1000,
        owner=OWNER,
    )
    spec = pod["spec"]
    c = spec["containers"][0]
    assert spec["hostPID"] is True
    assert c["resources"]["limits"] == {"memory": "850"}
    cmd = c["command"]
    assert cmd[cmd.index("--collect") + 1] == "/x:y"
    assert [cmd[i + 1] for i, a in enumerate(cmd) if a == "--rlimit"] == ["nofile=1048576", "memlock=unlimited"]
    env = {e["name"]: e for e in c["env"]}
    assert env["A"]["value"] == "2"  # container env overrides task env
    assert env["BENCHMARK_SECRETS_PASSPHRASE"]["valueFrom"]["secretKeyRef"] == {
        "name": "inspector-secrets",
        "key": "geekbench",
    }
    assert env["HF_TOKEN"]["valueFrom"]["secretKeyRef"]["name"] == "inspector-secrets"
    volumes = {v["name"]: v for v in spec["volumes"]}
    assert volumes["models"]["hostPath"]["path"] == "/var/lib/k8s-inspector/models"
    assert volumes["dshm"]["emptyDir"] == {"medium": "Memory", "sizeLimit": "4Gi"}
    assert {"name": "dshm", "mountPath": "/dev/shm"} in c["volumeMounts"]


def test_probe_pod_has_no_tracker(run, cfg):
    a, b = task("lscpu", parallel=True), task("lstopo", parallel=True)
    pod = pods.render_step(
        run, 1, step(container(a), container(b)), cfg, node_name="n", node_allocatable_memory_bytes=1, owner=OWNER
    )
    assert pod["metadata"]["labels"]["inspector.sparecores.com/task"] == "probe"
    assert [c["name"] for c in pod["spec"]["containers"]] == ["lscpu", "lstopo"]
    assert all(not c["env"] for c in pod["spec"]["containers"])


def test_gpu_step(run, cfg):
    s = step(container(task("nvbandwidth", gpu=True)), gpu_runtime=True)
    pod = pods.render_step(run, 1, s, cfg, node_name="n", node_allocatable_memory_bytes=1, owner=OWNER, gpu_count=4)
    assert pod["spec"]["runtimeClassName"] == "nvidia"
    c = pod["spec"]["containers"][0]
    assert c["resources"]["limits"] == {"nvidia.com/gpu": "4"}
    assert {"name": "NVIDIA_VISIBLE_DEVICES", "value": "all"} in c["env"]


def test_vllm_probe_key_container_name(run, cfg):
    t = task("vllm")
    s = step(container(t, key="vllm-probe-benchmark-vllm-gpu", command="--probe-only"))
    pod = pods.render_step(run, 1, s, cfg, node_name="n", node_allocatable_memory_bytes=1, owner=OWNER)
    c = pod["spec"]["containers"][0]
    assert c["name"] == "vllm-probe-benchmark-vllm-gpu"
    assert c["command"][-1] == "--probe-only"
    assert c["command"][c["command"].index("--task") + 1] == "vllm-probe-benchmark-vllm-gpu"


def test_flush_and_secret(run, cfg):
    a, b = task("lscpu", parallel=True), task("lstopo", parallel=True)
    s = step(container(a), container(b))
    pod = pods.render_flush_step(run, 1, s, 1, cfg, node_name="n", owner=OWNER)
    assert pod["metadata"]["labels"]["inspector.sparecores.com/role"] == "flush"
    assert pod["spec"]["containers"][1]["args"][:4] == ["flush", "--task", "lstopo", "--spool"]
    assert "securityContext" not in pod["spec"]["containers"][0]
    owner = OwnerRef("inspector.sparecores.com/v1alpha1", "BenchmarkRun", run.name, "u")
    secret = pods.render_urls_secret(run, "{}", owner)
    assert secret["stringData"] == {"urls.json": "{}"}
    assert secret["metadata"]["name"] == f"{run.name}-urls"


def test_pod_readers():
    pod = {
        "spec": {"nodeName": "n"},
        "status": {
            "phase": "Running",
            "startTime": "2026-11-20T09:00:00Z",
            "containerStatuses": [{"ready": True}, {"ready": False}],
        },
    }
    assert pods.pod_node(pod) == "n"
    assert pods.is_running(pod) and not pods.is_ready(pod) and not pods.is_finished(pod)
    pod["status"]["containerStatuses"][1]["ready"] = True
    assert pods.is_ready(pod)
    assert pods.is_finished({"status": {"phase": "Failed"}})
    assert not pods.is_ready({"status": {}})


def test_pull_stuck():
    def pod(reason, start="2026-11-20T09:30:00Z", phase="Pending"):
        status = {"phase": phase, "startTime": start}
        if reason:
            status["containerStatuses"] = [{"state": {"waiting": {"reason": reason}}}]
        return {"status": status}

    assert pods.pull_stuck(pod("ImagePullBackOff"), NOW, 15)
    assert pods.pull_stuck(pod("ContainerCreating"), NOW, 15)
    assert not pods.pull_stuck(pod("ImagePullBackOff", "2026-11-20T09:50:00Z"), NOW, 15)
    assert not pods.pull_stuck(pod("CrashLoopBackOff"), NOW, 15)
    assert not pods.pull_stuck({"status": {"phase": "Pending"}}, NOW, 15)  # not started by a kubelet
    assert pods.pull_stuck(pod(None), NOW, 15)


def finished_pod(*statuses, phase="Failed", reason=None):
    return {"status": {"phase": phase, "reason": reason, "containerStatuses": list(statuses)}}


def terminated(name, message=None, reason="Completed", exit_code=0):
    state = {"exitCode": exit_code, "reason": reason}
    if message is not None:
        state["message"] = json.dumps(message) if isinstance(message, dict) else message
    return {"name": name, "state": {"terminated": state}}


def msg(**kw):
    base = {"task": "membench", "exit_code": 0, "start": "2026-11-20T09:00:00Z", "end": "2026-11-20T09:10:00Z"}
    return base | kw


def classify_one(status, reason=None, phase="Failed"):
    c = container(task())
    return pods.classify(finished_pod(status, phase=phase, reason=reason), [c])["membench"]


def test_classify_success_and_message_fields():
    out = classify_one(
        terminated("membench", msg(version="1.2", kernel_version="6.12", upload_complete=True, missing=0)),
        phase="Succeeded",
    )
    assert (out.exit_code, out.from_sc_run, out.version, out.upload_complete) == (0, True, "1.2", True)
    assert out.start == datetime(2026, 11, 20, 9, 0, tzinfo=UTC)


def test_classify_task_exit_timeout_oom_cancel():
    assert classify_one(terminated("membench", msg(exit_code=3))).exit_code == 3
    out = classify_one(
        terminated("membench", msg(exit_code=-6, timed_out=True, error_msg="Execution timed out after 2400s"))
    )
    assert (out.exit_code, out.error_msg, out.timed_out) == (-6, "Execution timed out after 2400s", True)
    out = classify_one(terminated("membench", msg(exit_code=137, oom_kills=1)))
    assert (out.exit_code, out.error_msg) == (137, "OOMKilled")
    out = classify_one(terminated("membench", msg(exit_code=143, cancelled=True, error_msg="cancelled")))
    assert (out.exit_code, out.error_msg) == (-4, "CANCELLED")
    out = classify_one(terminated("membench", msg(exit_code=143, cancelled=True)), reason="DeadlineExceeded")
    assert out.error_msg == "DEADLINE_EXCEEDED"
    out = classify_one(terminated("membench", msg(exit_code=143, cancelled=True)), reason="Evicted")
    assert out.error_msg == "EVICTED"


def test_classify_without_message():
    out = classify_one(terminated("membench", reason="OOMKilled", exit_code=137))
    assert (out.exit_code, out.error_msg, out.from_sc_run) == (137, "OOMKilled", False)
    started = {"name": "membench", "state": {"waiting": {"reason": "CreateContainerError", "message": "no runtime"}}}
    out = classify_one(started)
    assert (out.exit_code, out.container_start_error, out.error_msg) == (256, True, "no runtime")
    out = classify_one(terminated("membench", reason="StartError", exit_code=128, message="exec failed"))
    assert out.container_start_error
    out = classify_one(terminated("membench", reason="Error", exit_code=137), reason="DeadlineExceeded")
    assert (out.exit_code, out.error_msg) == (-4, "DEADLINE_EXCEEDED")
    out = classify_one(terminated("membench", reason="Error", exit_code=137), reason="Evicted")
    assert out.error_msg == "EVICTED"
    out = classify_one(terminated("membench", reason="Error", exit_code=2, message="boom"))
    assert (out.exit_code, out.error_msg, out.container_start_error) == (256, "boom", False)
    out = classify_one(terminated("membench", exit_code=0), phase="Succeeded")
    assert out.exit_code == 256 and "termination message" in out.error_msg
    out = classify_one(terminated("membench", message="not json", exit_code=1))
    assert out.exit_code == 256 and out.error_msg == "not json"


def test_classify_every_key_present_even_without_status():
    a, b = container(task("a")), container(task("b"))
    out = pods.classify(finished_pod(terminated("a", msg()), phase="Failed", reason="DeadlineExceeded"), [a, b])
    assert set(out) == {"a", "b"}
    assert out["a"].exit_code == 0
    assert (out["b"].exit_code, out["b"].error_msg) == (-4, "DEADLINE_EXCEEDED")
    out = pods.classify({"status": {"phase": "Failed", "message": "gone"}}, [a])
    assert (out["a"].exit_code, out["a"].error_msg) == (256, "gone")
