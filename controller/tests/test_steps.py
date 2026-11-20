from datetime import UTC, datetime, timedelta

import pytest

from k8s_inspector import steps
from k8s_inspector.model import NodeFacts, TaskDef, TaskOutcome

GPU, CPU, AVX2 = (f"ghcr.io/sparecores/benchmark-vllm-{n}:main" for n in ("gpu", "cpu", "cpu-avx2"))


@pytest.fixture(autouse=True)
def fake_image_ref(monkeypatch):
    # tasks.image_ref needs a catalogue digest; the digest logic is tested with tasks.py.
    monkeypatch.setattr(steps, "image_ref", lambda task, image=None: f"{image or task.image}@sha256:d")


def task(name, priority=1.0, **kw) -> TaskDef:
    defaults = dict(name=name, image=f"img/{name}:main", command=None, timeout=timedelta(minutes=10), priority=priority)
    return TaskDef(**(defaults | kw))


def facts(**kw) -> NodeFacts:
    defaults = dict(
        node_name="n",
        instance_type="oci.x",
        arch="amd64",
        created=datetime(2026, 1, 1, tzinfo=UTC),
        allocatable_memory_bytes=1,
    )
    return NodeFacts(**(defaults | kw))


def outcome(key, code=0, **kw) -> TaskOutcome:
    return TaskOutcome(key=key, exit_code=code, **kw)


VLLM = task("vllm", 14, image="|".join((GPU, CPU, AVX2)), images=(GPU, CPU, AVX2), memory_cap=0.85)


def test_probe_step():
    parallel = [task("dmidecode", 0, parallel=True), task("lscpu", 0, parallel=True)]
    s = steps.probe_step([*parallel, task("geekbench")])
    assert s.name == "01-probes" and s.index == 1
    assert [c.key for c in s.containers] == ["dmidecode", "lscpu"]
    assert not s.gpu_runtime
    assert steps.probe_step([task("geekbench")]) is None
    gpu = steps.probe_step([*parallel, task("nvidia_smi", 0, parallel=True, gpu=True)])
    assert gpu.gpu_runtime


def test_next_step_priority_and_storage_last():
    rem = [
        task("storage", float("inf"), privileges=frozenset({"host_pid"})),
        task("stressnglongrun", float("inf")),
        task("b", 2),
        task("a", 1),
    ]
    order = []
    while rem:
        s = steps.next_step(rem, {}, facts(), index=len(order) + 2)
        order.append(s.name)
        rem = [t for t in rem if t.name != s.containers[0].key]
    assert order == ["02-a", "03-b", "04-stressnglongrun", "05-storage"]


def test_next_step_container_and_name():
    s = steps.next_step([task("nvidia_x", 3)], {}, facts(), index=9)
    assert s.name == "09-nvidia-x"
    assert s.containers[0].image == "img/nvidia_x:main@sha256:d"
    assert not s.gpu_runtime
    assert steps.next_step([], {}, facts(), index=1) is None


def test_gpu_node_runs_every_step_with_runtime():
    s = steps.next_step([task("geekbench")], {}, facts(has_nvidia_gpu=True), index=2)
    assert s.gpu_runtime


@pytest.mark.parametrize(("vram", "buffer"), [(None, 256), (80000, 512), (2048, 256), (1024, 256), (2049, 512)])
def test_nvbandwidth_buffer(vram, buffer):
    s = steps.next_step([task("nvbandwidth", gpu=True)], {}, facts(has_nvidia_gpu=True, gpu_vram_mib=vram), index=2)
    assert s.containers[0].command == f"nvbandwidth -j -b {buffer}"


def test_vllm_ladder_on_gpu_node():
    f = facts(has_nvidia_gpu=True, gpu_compute_cap=(8, 0))
    s = steps.next_step([VLLM], {}, f, index=3)
    c = s.containers[0]
    assert (s.name, c.key, c.command, s.gpu_runtime) == (
        "03-vllm-probe-benchmark-vllm-gpu",
        "vllm-probe-benchmark-vllm-gpu",
        "--probe-only",
        True,
    )
    assert c.timeout == timedelta(minutes=18)
    assert c.image == f"{GPU}@sha256:d"
    assert dict(c.env) == {"BENCHMARK_VLLM_MODE": "gpu"}

    finished = {"vllm-probe-benchmark-vllm-gpu": outcome("x", 1, error_msg="no gpu")}
    s = steps.next_step([VLLM], finished, f, index=4)
    assert s.containers[0].key == "vllm-probe-benchmark-vllm-cpu"
    assert not s.gpu_runtime and dict(s.containers[0].env) == {"BENCHMARK_VLLM_MODE": "cpu"}

    finished["vllm-probe-benchmark-vllm-cpu"] = outcome("x", 0)
    s = steps.next_step([VLLM], finished, f, index=5)
    c = s.containers[0]
    assert (s.name, c.key, c.command, c.image) == ("05-vllm", "vllm", None, f"{CPU}@sha256:d")
    assert c.task is VLLM and c.timeout is None


def test_vllm_gpu_rung_success_runs_gpu_benchmark():
    f = facts(has_nvidia_gpu=True, gpu_compute_cap=(9, 0))
    s = steps.next_step([VLLM], {"vllm-probe-benchmark-vllm-gpu": outcome("x")}, f, index=3)
    assert s.gpu_runtime and s.containers[0].key == "vllm" and s.containers[0].image.startswith(GPU)


@pytest.mark.parametrize("f", [facts(), facts(has_nvidia_gpu=True, gpu_compute_cap=(7, 5))])
def test_vllm_skips_gpu_image(f):
    assert steps.next_step([VLLM], {}, f, index=3).containers[0].key == "vllm-probe-benchmark-vllm-cpu"


def test_vllm_unknown_compute_cap_tries_gpu_image():
    f = facts(has_nvidia_gpu=True)
    assert steps.next_step([VLLM], {}, f, index=3).containers[0].key == "vllm-probe-benchmark-vllm-gpu"


def test_vllm_avx2_skipped_on_arm():
    f = facts(arch="arm64")
    finished = {"vllm-probe-benchmark-vllm-cpu": outcome("x", 1, error_msg="boom")}
    assert steps.next_step([VLLM], finished, f, index=3) is None
    assert steps.vllm_ladder_failure(VLLM, finished, f) == "probe benchmark-vllm-cpu: boom"


def test_vllm_ladder_failure_is_skipped_not_blocking():
    f = facts()
    finished = {
        "vllm-probe-benchmark-vllm-cpu": outcome("x", 1, error_msg="a"),
        "vllm-probe-benchmark-vllm-cpu-avx2": outcome("x", 2),
    }
    assert (
        steps.vllm_ladder_failure(VLLM, finished, f)
        == "probe benchmark-vllm-cpu: a; probe benchmark-vllm-cpu-avx2: exit_code=2"
    )
    ffmpeg = task("ffmpeg", 15)
    s = steps.next_step([VLLM, ffmpeg], finished, f, index=6)
    assert s.containers[0].key == "ffmpeg"
    assert steps.vllm_ladder_failure(VLLM, {}, f) is None


def test_retry_without_gpu():
    cpu_ok = task("llm")
    gpu_only = task("nvbandwidth", gpu=True)
    s = steps.next_step([cpu_ok], {}, facts(has_nvidia_gpu=True), index=2)
    retry = steps.retry_without_gpu(s, {"llm": outcome("llm", 256, container_start_error=True)})
    assert retry is not None and not retry.gpu_runtime and retry.containers == s.containers
    assert steps.retry_without_gpu(s, {"llm": outcome("llm", 1)}) is None
    assert steps.retry_without_gpu(retry, {"llm": outcome("llm", 256, container_start_error=True)}) is None
    g = steps.next_step([gpu_only], {}, facts(has_nvidia_gpu=True), index=2)
    assert steps.retry_without_gpu(g, {"nvbandwidth": outcome("nvbandwidth", 256, container_start_error=True)}) is None


def test_retry_without_gpu_on_any_start_failure():
    s = steps.next_step([task("llm")], {}, facts(has_nvidia_gpu=True), index=2)
    for failure in (outcome("llm", 256), outcome("llm", -4, error_msg="IMAGE_PULL_TIMEOUT")):
        assert steps.retry_without_gpu(s, {"llm": failure}) is not None
    assert steps.retry_without_gpu(s, {"llm": outcome("llm", -6)}) is None


def test_vllm_gpu_image_is_not_retried_without_the_runtime():
    f = facts(has_nvidia_gpu=True, gpu_compute_cap=(8, 0))
    probe = steps.next_step([VLLM], {}, f, index=3)
    assert probe.gpu_runtime
    key = probe.containers[0].key
    assert steps.retry_without_gpu(probe, {key: outcome(key, 256, container_start_error=True)}) is None


def test_vllm_label_ignores_tag_and_digest():
    assert steps.vllm_label("ghcr.io/sparecores/benchmark-vllm-gpu@sha256:abc") == "benchmark-vllm-gpu"
    assert steps.vllm_label(GPU) == "benchmark-vllm-gpu"


def test_step_survives_the_status_round_trip(monkeypatch):
    monkeypatch.setattr(steps, "all_tasks", lambda: [VLLM])
    f = facts(has_nvidia_gpu=True, gpu_compute_cap=(8, 0))
    original = steps.next_step([VLLM], {}, f, index=3)
    restored = steps.from_status(steps.to_status(original))
    assert restored is not None and restored.name == original.name and restored.gpu_runtime
    c, o = restored.containers[0], original.containers[0]
    assert (c.key, c.image, c.command, c.timeout, c.env) == (o.key, o.image, o.command, o.timeout, o.env)
    assert steps.from_status(None) is None and steps.from_status({"containers": [{"key": "nope"}]}) is None
