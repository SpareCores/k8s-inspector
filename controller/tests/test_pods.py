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
    settings = Settings(vendors={"oci": VendorSettings(enabled=True)})
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


def test_anchor(run, cfg):
    owner = OwnerRef("inspector.sparecores.com/v1alpha1", "BenchmarkRun", run.name, "u")
    pod = pods.render_anchor(
        run,
        1,
        cfg,
        capacity=[CapacityType.SPOT, CapacityType.ON_DEMAND],
        deadline_seconds=100,
        owner=owner,
    )
    spec = pod["spec"]
    assert pod["metadata"]["annotations"] == {"karpenter.sh/do-not-disrupt": "true"}
    assert "blockOwnerDeletion" not in pod["metadata"]["ownerReferences"][0]
    assert spec["nodeSelector"] == {
        "karpenter.sh/nodepool": "bench",
        "scarpenter.sparecores.com/vendor": "oci",
        "scarpenter.sparecores.com/api-reference": "VM.Standard.E5.Flex.2-16",
    }
    expr = spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]
    assert expr["matchExpressions"][0]["values"] == ["spot", "on-demand"]
    # no toleration for SCarpenter's `gpu-pending` taint: the anchor waits until the driver check removes it
    assert spec["tolerations"] == [{"key": "frankenscube/bench", "operator": "Exists"}]
    assert spec["activeDeadlineSeconds"] == 100
    assert spec["automountServiceAccountToken"] is False
    c = spec["containers"][0]
    assert c["args"] == ["hold", "--install", "/k8si/bin", "--heartbeat-dir", "/k8si/runs/r1", "--idle-timeout", "45m"]
    assert c["readinessProbe"]["exec"]["command"] == ["/sc-run", "ready", "/k8si/bin"]
    assert c["resources"] == {"requests": {"cpu": "1m", "memory": "16Mi"}}  # measured working set 8.5 MiB


def test_deadlines(cfg):
    s = step(container(task()))  # 40 min task
    assert pods.step_deadline_seconds(s, cfg) == 4260  # spikes/p0/step-membench.yaml
    probes = step(
        container(task("a", timeout=timedelta(minutes=5))), container(task("b", timeout=timedelta(seconds=30)))
    )
    assert pods.step_deadline_seconds(probes, cfg) == 2160
    # probe step (5 min task) and a 40 min task, two 15 min flush Pods each, a 1 h hold, 1 h extra
    selected = [task("a", timeout=timedelta(minutes=5), parallel=True), task()]
    flushes = 2 * 2 * 900
    assert pods.anchor_deadline_seconds(selected, cfg, timedelta(hours=1)) == 4260 + 2160 + 3600 * 2 + flushes
    cfg2 = Config(cfg.env, Settings(deadlines=Deadlines(image_pull_minutes=1)))
    assert pods.step_deadline_seconds(s, cfg2) == (1 + 1 + 40 + 10 + 5) * 60


def test_step_pod_network_namespace_for_loopback_tasks(run, cfg):
    def spec_of(*tasks):
        step_ = step(*(container(t) for t in tasks))
        pod = pods.render_step(run, 1, step_, cfg, node_name="n1", node_allocatable_memory_bytes=1000, owner=OWNER)
        return pod["spec"]

    spec = spec_of(task("redis", host_network=False))
    assert spec["hostNetwork"] is False and spec["dnsPolicy"] == "Default"
    assert spec["containers"][0]["securityContext"] == {"privileged": True, "seccompProfile": {"type": "Unconfined"}}
    assert spec_of(task("membench"))["hostNetwork"] is True
    assert spec_of(task("redis", host_network=False), task("membench"))["hostNetwork"] is True


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
        "--heartbeat-dir", "/k8si/runs/r1", "--nice", "-20",
        "--version-cmd", "/usr/local/bin/resource-tracker -- /usr/local/bin/membench -V",
        "--", "/usr/local/bin/resource-tracker", "--", "/usr/local/bin/membench", "-Hv", "-t", "1800",
    ]  # fmt: skip
    env = {e["name"]: e["value"] for e in c["env"]}
    assert env["TRACKER_OUTPUT"] == "/k8si/runs/r1/membench/resource_tracker.jsonl"
    assert env["TRACKER_EXTERNAL_RUN_ID"] == "r1"
    assert c["securityContext"] == {"privileged": True, "seccompProfile": {"type": "Unconfined"}}
    assert c["resources"] == {"requests": {"cpu": "10m", "memory": "0"}}
    assert c["terminationMessagePolicy"] == "File"
    assert [v["name"] for v in spec["volumes"]] == ["k8si", "urls"]
    assert spec["volumes"][1]["secret"]["secretName"] == f"{run.name}-urls"


def pod_requesting(*cpus, init=(), phase="Running"):
    def containers(values):
        return [{"resources": {"requests": {"cpu": c} if c else {}}} for c in values]

    return {"spec": {"containers": containers(cpus), "initContainers": containers(init)}, "status": {"phase": phase}}


def test_cpu_millicores_quantities():
    assert [pods.cpu_millicores(q) for q in ("2", "0.5", "3920m", 4, "100m", "2k")] == [
        2000,
        500,
        3920,
        4000,
        100,
        2_000_000,
    ]
    # sub-millicore values: others' requests round up, our own down
    assert [pods.cpu_millicores(q) for q in ("100n", "1500u", "1001u", "2500000n")] == [1, 2, 2, 3]
    assert [pods.cpu_millicores(q, round_up=False) for q in ("1500u", "1001u", "2500000n", "3920m", "0.5")] == [
        1,
        1,
        2,
        3920,
        500,
    ]


def test_pod_cpu_request_is_containers_sum_or_biggest_init():
    assert pods.pod_cpu_request_millicores(pod_requesting("100m", "250m", None)) == 350
    assert pods.pod_cpu_request_millicores(pod_requesting("100m", init=("1",))) == 1000
    assert pods.pod_cpu_request_millicores({"spec": {"containers": [{}]}}) == 0


def test_pod_cpu_request_counts_sidecars_overhead_and_pod_level_requests():
    def sidecar(cpu):
        return {"restartPolicy": "Always", "resources": {"requests": {"cpu": cpu}}}

    def init(cpu):
        return {"resources": {"requests": {"cpu": cpu}}}

    pod = pod_requesting("100m")
    pod["spec"]["initContainers"] = [sidecar("200m"), init("50m")]
    assert pods.pod_cpu_request_millicores(pod) == 300  # steady: container + sidecar
    pod["spec"]["initContainers"] = [sidecar("200m"), init("500m")]
    assert pods.pod_cpu_request_millicores(pod) == 700  # init peak: sidecar already running + the init container
    pod["spec"]["overhead"] = {"cpu": "50m"}
    assert pods.pod_cpu_request_millicores(pod) == 750
    pod["spec"]["resources"] = {"requests": {"cpu": "2"}}  # Pod-level requests override the container sums
    assert pods.pod_cpu_request_millicores(pod) == 2050


def test_step_cpu_is_what_the_other_pods_leave():
    node_pods = [
        pod_requesting("100m"),  # flannel
        pod_requesting("200m", "50m"),  # anchor and friends
        pod_requesting("1", phase="Succeeded"),  # finished: does not count
        pod_requesting("1", phase="Failed"),
        pod_requesting(None),  # no request
    ]
    # margin: the largest other Pod (250m) beats 100m and 5% of 4000
    assert pods.cpu_margin_millicores(4000, node_pods) == 250
    assert pods.step_cpu_millicores(4000, node_pods) == 4000 - 350 - 250
    assert pods.step_cpu_millicores(3920, []) == 3920 - 196  # 5% of the allocatable
    assert pods.step_cpu_millicores(1000, [pod_requesting("1m")]) == 1000 - 1 - 100  # the 100m minimum


def test_step_cpu_never_drops_below_the_floor():
    assert pods.step_cpu_millicores(1000, [pod_requesting("995m")]) == pods.CPU_FLOOR_M
    assert pods.step_cpu_millicores(1000, [pod_requesting("5")]) == pods.CPU_FLOOR_M
    assert pods.step_cpu_millicores(0, []) == pods.CPU_FLOOR_M  # allocatable unknown


def test_step_pod_requests_the_given_cpu_split_over_containers_without_limit(run, cfg):
    kw = {"node_name": "n", "node_allocatable_memory_bytes": 1, "owner": OWNER}
    pod = pods.render_step(run, 1, step(container(task())), cfg, cpu_millicores=3900, **kw)
    assert pod["spec"]["containers"][0]["resources"] == {"requests": {"cpu": "3900m", "memory": "0"}}
    a, b = task("lscpu", parallel=True), task("lstopo", parallel=True)
    pod = pods.render_step(run, 1, step(container(a), container(b)), cfg, cpu_millicores=3900, **kw)
    assert [c["resources"]["requests"]["cpu"] for c in pod["spec"]["containers"]] == ["1950m", "1950m"]
    pod = pods.render_step(run, 1, step(container(a), container(b)), cfg, cpu_millicores=15, **kw)
    assert [c["resources"]["requests"]["cpu"] for c in pod["spec"]["containers"]] == ["7m", "7m"]
    pod = pods.render_step(run, 1, step(container(a), container(b)), cfg, cpu_millicores=3, **kw)  # 10m for the Pod
    assert [c["resources"]["requests"]["cpu"] for c in pod["spec"]["containers"]] == ["5m", "5m"]
    three = step(container(a), container(b), container(task("lsmem", parallel=True)))
    pod = pods.render_step(run, 1, three, cfg, cpu_millicores=10, **kw)  # at least 1m each
    assert [c["resources"]["requests"]["cpu"] for c in pod["spec"]["containers"]] == ["3m", "3m", "3m"]
    assert all("cpu" not in c["resources"].get("limits", {}) for c in pod["spec"]["containers"])


def test_out_of_cpu_rejection_is_recognised_and_classified_with_its_message():
    message = "Pod was rejected: Node didn't have enough resource: cpu, requested: 3000, used: 100, capacity: 2000"
    rejected = {"status": {"phase": "Failed", "reason": "OutOfcpu", "message": message}}
    assert pods.is_out_of_cpu(rejected) and not pods.is_rejected(rejected)
    out = pods.classify(rejected, [container(task())])["membench"]
    assert (out.exit_code, out.error_msg, out.container_start_error) == (-4, message, True)
    ran = finished_pod({"name": "membench", "state": {"running": {}}}, reason="OutOfcpu")
    assert not pods.is_out_of_cpu(ran)


def test_every_admission_rejection_is_classified_minus_four_with_its_message():
    message = "Pod was rejected: Node didn't have enough resource: memory, requested: 335544320, used: 1, capacity: 2"
    for reason in ("OutOfmemory", "OutOfpods", "Evicted", "NodeAffinity"):
        rejected = {"status": {"phase": "Failed", "reason": reason, "message": message}}
        out = pods.classify(rejected, [container(task())])["membench"]
        assert (out.exit_code, out.error_msg, out.container_start_error) == (-4, message, True), reason
    ran = finished_pod({"name": "membench", "state": {"terminated": {"exitCode": 1}}}, reason="OutOfmemory")
    ran["status"]["message"] = message
    assert not pods.is_refused(ran)  # a container ran: not an admission rejection


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
    assert cmd[cmd.index("--nice") + 1] == "-20"
    assert cmd.index("--nice") < cmd.index("--")
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
    # no memory request: a 0.5 GiB node cannot admit 16Mi per container next to the anchor and the DaemonSets
    assert {c["resources"]["requests"]["memory"] for c in pod["spec"]["containers"]} == {"0"}
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


def test_stuck_containers():
    def pod(*reasons, start="2026-11-20T09:30:00Z", phase="Pending", names=("a", "b")):
        status = {"phase": phase, "startTime": start}
        if reasons:
            status["containerStatuses"] = [
                {"name": n, "state": {"waiting": {"reason": r}} if r else {"terminated": {"exitCode": 0}}}
                for n, r in zip(names, reasons, strict=True)
            ]
        return {"spec": {"containers": [{"name": n} for n in names]}, "status": status}

    timeout = {"a": "IMAGE_PULL_TIMEOUT", "b": "IMAGE_PULL_TIMEOUT"}
    assert pods.stuck_containers(pod("ImagePullBackOff", "ErrImagePull"), NOW, 15) == timeout
    assert pods.stuck_containers(pod("ContainerCreating", None), NOW, 15) == {"a": "IMAGE_PULL_TIMEOUT"}  # b ran
    assert pods.stuck_containers(pod("ImagePullBackOff", None, start="2026-11-20T09:50:00Z"), NOW, 15) == {}
    assert pods.stuck_containers(pod("CrashLoopBackOff", "CrashLoopBackOff"), NOW, 15) == {}
    assert pods.stuck_containers({"status": {"phase": "Pending"}}, NOW, 15) == {}  # not started by a kubelet
    assert pods.stuck_containers(pod(), NOW, 15) == timeout  # no sandbox yet: every container waits
    # waiting states that never resolve fail like stuck pulls and carry the waiting message
    config = pod("CreateContainerConfigError", "InvalidImageName")
    config["status"]["containerStatuses"][0]["state"]["waiting"]["message"] = 'secret "x" not found'
    assert pods.stuck_containers(config, NOW, 15) == {"a": 'secret "x" not found', "b": "InvalidImageName"}
    assert pods.stuck_containers(pod("ErrImageNeverPull", None), NOW, 15) == {"a": "ErrImageNeverPull"}
    assert pods.stuck_containers(pod("ErrImageNeverPull", None, start="2026-11-20T09:50:00Z"), NOW, 15) == {}
    assert pods.unfinished_containers(pod("ContainerCreating", None)) == {"a"}


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


def test_classify_records_the_applied_nice():
    assert classify_one(terminated("membench", msg(nice=-20)), phase="Succeeded").nice == -20
    assert classify_one(terminated("membench", msg(nice=None)), phase="Succeeded").nice is None
    assert classify_one(terminated("membench", msg()), phase="Succeeded").nice is None


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


def test_classify_oom_survived_by_sc_run_is_trusted_from_the_message():
    # P0 (memhog): Pod Succeeded, container exitCode 0 with reason OOMKilled, sc-run's message says 137.
    survived = msg(exit_code=137, error_msg="OOMKilled", oom_kills=3, upload_complete=False, missing=3)
    out = classify_one(terminated("membench", survived, reason="OOMKilled"), phase="Succeeded")
    assert (out.exit_code, out.error_msg, out.from_sc_run, out.oom_kills) == (137, "OOMKilled", True, 3)
    # P0 (oomcheck): stress-ng handled the kills itself, sc-run reported exit 0: reason OOMKilled changes nothing.
    clean = msg(exit_code=0, oom_kills=620, upload_complete=False, missing=3)
    out = classify_one(terminated("membench", clean, reason="OOMKilled"), phase="Succeeded")
    assert (out.exit_code, out.error_msg, out.from_sc_run) == (0, None, True)


def test_overdue_and_terminating_overdue():
    def running(deadline=100, grace=20, **meta):
        return {
            "metadata": meta,
            "spec": {"activeDeadlineSeconds": deadline, "terminationGracePeriodSeconds": grace},
            "status": {"phase": "Running", "startTime": "2026-11-20T10:00:00Z"},
        }

    t0 = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    limit = timedelta(seconds=100 + 20) + pods.WATCHDOG_MARGIN
    assert not pods.overdue(running(), t0 + limit)
    assert pods.overdue(running(), t0 + limit + timedelta(seconds=1))
    assert not pods.overdue({**running(), "status": {"phase": "Failed", "startTime": "2026-11-20T10:00:00Z"}}, NOW)
    assert not pods.overdue(running(deletionTimestamp="2026-11-20T10:00:20Z"), NOW)  # terminating: other rule
    assert not pods.overdue({"spec": {}, "status": {"phase": "Pending"}}, NOW)
    gone = {"metadata": {"deletionTimestamp": "2026-11-20T10:00:20Z"}}
    assert not pods.terminating_overdue(gone, t0 + timedelta(seconds=20) + pods.WATCHDOG_MARGIN)
    assert pods.terminating_overdue(gone, t0 + timedelta(seconds=21) + pods.WATCHDOG_MARGIN)
    assert not pods.terminating_overdue({"metadata": {}}, NOW)


def test_rejected_pod_is_recognised_and_classified_with_its_message():
    message = "Pod was rejected: The node had condition: [DiskPressure]."
    rejected = {"status": {"phase": "Failed", "reason": "Evicted", "message": message}}
    assert pods.is_rejected(rejected)
    out = pods.classify(rejected, [container(task())])["membench"]
    assert (out.exit_code, out.error_msg) == (-4, message)
    evicted_later = finished_pod({"name": "membench", "state": {"running": {}}}, reason="Evicted")
    evicted_later["status"]["message"] = message
    assert not pods.is_rejected(evicted_later)  # a container ran: a real eviction


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        ("VM.Standard.E5.Flex.2-16", True),
        ("m5.large", True),
        ("a", True),
        ("a" * 63, True),
        ("a" * 64, False),
        ("", False),
        ("-m5", False),
        ("m5.", False),
        ("has space", False),
        ("a/b", False),
    ],
)
def test_is_label_value(value, ok):
    assert pods.is_label_value(value) is ok
