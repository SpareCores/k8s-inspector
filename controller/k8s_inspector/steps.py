"""Step sequencing and the adaptive decisions of a run: what Pod comes next.

Owner: steps agent. Implements run-model.md "Steps" and gpu.md "k8s-inspector side"; ports sc-inspector's
GPU-first-then-CPU retry, the vLLM image ladder (probe GPU -> CPU -> AVX2 images, benchmark on the first
probe success, skip the GPU image below compute capability 8.0) and the nvbandwidth `-b` buffer size.

Pure functions, no I/O. They are stateless: all state is the finished outcomes of this run, which the
runner keeps in `status.tasks` and re-reads from object storage after a restart. Probe keys of the vLLM ladder
are ordinary entries (`vllm-probe-<image>`).
"""

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import timedelta

from k8s_inspector.model import NodeFacts, Step, StepContainer, TaskDef, TaskOutcome
from k8s_inspector.tasks import image_ref

PROBE_STEP_NAME = "01-probes"
VLLM_PROBE_COMMAND = "--probe-only"
VLLM_PROBE_TIMEOUT = timedelta(minutes=18)  # must exceed the harness /health wait
VLLM_MIN_GPU_COMPUTE_CAP = (8, 0)  # the GPU image ships Ampere+ kernels only
NVBANDWIDTH_BUFFER_MIB = 512
NVBANDWIDTH_BUFFER_MIB_LOW_VRAM = 256
NVBANDWIDTH_LOW_VRAM_MIB = 2048  # bidirectional copy needs 3x the buffer on the GPU


def _container(task: TaskDef) -> StepContainer:
    return StepContainer(key=task.name, task=task, image=image_ref(task))


def probe_step(tasks: Sequence[TaskDef], *, index: int = 1) -> Step | None:
    """The parallel hardware-probe step: one container per `parallel` task (dmidecode, lshw, lscpu, lstopo,
    virtualization, nvidia_smi; `gpu_facts` is added by the catalogue like any task). None when the
    selection has no parallel task. `nvidia_smi` and `gpu_facts` are included only on GPU types, which the
    caller expresses by passing only the tasks it wants (precheck result)."""
    probes = [t for t in tasks if t.parallel]
    if not probes:
        return None
    name = PROBE_STEP_NAME if index == 1 else f"{index:02d}-probes"
    return Step(
        index=index,
        name=name,
        containers=tuple(_container(t) for t in probes),
        gpu_runtime=any(t.gpu for t in probes),
    )


# --------------------------------------------------------------------------- vLLM image ladder


def vllm_label(image: str) -> str:
    """Image name without registry and tag: `benchmark-vllm-gpu`."""
    return image.rsplit("/", 1)[-1].split(":", 1)[0]


def vllm_probe_key(image: str) -> str:
    return f"vllm-probe-{vllm_label(image)}"


def _vllm_rungs(task: TaskDef, facts: NodeFacts) -> list[str]:
    """The ladder images this node can try, in order."""
    rungs = []
    for image in task.images:
        if "benchmark-vllm-gpu" in image:
            if not facts.has_nvidia_gpu:
                continue
            if facts.gpu_compute_cap is not None and facts.gpu_compute_cap < VLLM_MIN_GPU_COMPUTE_CAP:
                continue
        if "benchmark-vllm-cpu-avx2" in image and facts.arch == "arm64":
            continue
        rungs.append(image)
    return rungs


def _vllm_step(task: TaskDef, image: str, *, probe: bool, index: int) -> Step:
    use_gpu = "benchmark-vllm-gpu" in image
    key = vllm_probe_key(image) if probe else task.name
    container_task = replace(task, timeout=VLLM_PROBE_TIMEOUT) if probe else task
    container = StepContainer(
        key=key,
        task=container_task,
        image=image_ref(task, image),
        command=VLLM_PROBE_COMMAND if probe else None,
        env=(("BENCHMARK_VLLM_MODE", "gpu" if use_gpu else "cpu"),),
    )
    return Step(index=index, name=f"{index:02d}-{key}", containers=(container,), gpu_runtime=use_gpu)


def _vllm_next(task: TaskDef, finished: Mapping[str, TaskOutcome], facts: NodeFacts, index: int) -> Step | None:
    """Probe the next ladder image, or run the benchmark on the first image whose probe succeeded."""
    for image in _vllm_rungs(task, facts):
        probe = finished.get(vllm_probe_key(image))
        if probe is None:
            return _vllm_step(task, image, probe=True, index=index)
        if probe.exit_code == 0:
            return _vllm_step(task, image, probe=False, index=index)
    return None


def vllm_ladder_failure(task: TaskDef, finished: Mapping[str, TaskOutcome], facts: NodeFacts) -> str | None:
    """The error message for `task` when every ladder image was probed and failed (or none can run here),
    else None. `next_step` skips such a task; the runner records the message as its failure."""
    if _vllm_next(task, finished, facts, 0) is not None:
        return None
    errors = []
    for image in _vllm_rungs(task, facts):
        probe = finished[vllm_probe_key(image)]
        errors.append(f"probe {vllm_label(image)}: {probe.error_msg or f'exit_code={probe.exit_code}'}")
    return "; ".join(errors) or "No vLLM image passed startup probe"


# --------------------------------------------------------------------------- sequencing


def nvbandwidth_command(vram_mib: int | None) -> str:
    low = vram_mib is not None and vram_mib <= NVBANDWIDTH_LOW_VRAM_MIB
    return f"nvbandwidth -j -b {NVBANDWIDTH_BUFFER_MIB_LOW_VRAM if low else NVBANDWIDTH_BUFFER_MIB}"


def next_step(
    remaining: Sequence[TaskDef],
    finished: Mapping[str, TaskOutcome],
    facts: NodeFacts,
    *,
    index: int,
) -> Step | None:
    """The next step Pod, or None when nothing is left.

    `remaining`: selected tasks without a finished outcome, serial ones only (the probe step is handled by
    `probe_step`), any order. `finished`: outcomes of this run by container key (includes probes and vLLM
    probe keys). Picks the lowest priority; applies the adaptive rules:
    - vllm: next unprobed ladder image as a probe container (`--probe-only`, key `vllm-probe-<image>`);
      after the first probe success the full benchmark step with that image; none succeeded -> None and the
      runner records the failure; GPU image skipped when `facts.gpu_compute_cap` < (8, 0) or no GPU.
    - nvbandwidth: command with `-b <MiB>` derived from `facts.gpu_vram_mib`.
    - GPU types: `gpu_runtime=True` for every step; `GPU_VRAM_MIB`/`GPU_COMPUTE_CAP` env where the legacy
      passed them.
    `storage` (priority inf) is always last. `index` numbers the Pod (`NN-<task>`).

    A vLLM task whose ladder failed is skipped (not a reason to stop the run); the runner asks
    `vllm_ladder_failure` for its error. The legacy passed `GPU_VRAM_MIB`/`GPU_COMPUTE_CAP` to the inspector
    process, never to a task container, so no such env is emitted."""
    # Host-PID tasks (storage) last: they may unmount and overwrite free disks.
    for task in sorted(remaining, key=lambda t: (t.priority, "host_pid" in t.privileges, t.name)):
        if task.images:
            step = _vllm_next(task, finished, facts, index)
            if step is None:
                continue
            return step
        command = nvbandwidth_command(facts.gpu_vram_mib) if task.name == "nvbandwidth" else None
        container = replace(_container(task), command=command)
        name = f"{index:02d}-{task.name.replace('_', '-')}"
        return Step(index=index, name=name, containers=(container,), gpu_runtime=facts.has_nvidia_gpu)
    return None


def retry_without_gpu(step: Step, outcomes: Mapping[str, TaskOutcome]) -> Step | None:
    """GPU-first-then-CPU: when a container of a `gpu_runtime` step has `container_start_error`, the same
    step without the runtime class (same containers; the runner creates it once). None when no container
    failed to start or a failed container's task has `gpu=True` (no CPU fallback)."""
    if not step.gpu_runtime:
        return None
    failed = [c for c in step.containers if c.key in outcomes and outcomes[c.key].container_start_error]
    if not failed or any(c.task.gpu for c in failed):
        return None
    return replace(step, gpu_runtime=False)
