"""Step sequencing and the adaptive decisions of a run: what Pod comes next.

Owner: steps agent. Implements run-model.md "Steps" and gpu.md "k8s-inspector side"; ports sc-inspector's
GPU-first-then-CPU retry, the vLLM image ladder (probe GPU -> CPU -> AVX2 images, benchmark on the first
probe success, skip the GPU image below compute capability 8.0) and the nvbandwidth `-b` buffer size.

Pure functions, no I/O. They are stateless: all state is the finished outcomes of this run, which the
runner keeps in `status.tasks` and re-reads from object storage after a restart. Probe keys of the vLLM ladder
are ordinary entries (`vllm-probe-<image>`).
"""

from collections.abc import Mapping, Sequence

from k8s_inspector.model import NodeFacts, Step, TaskDef, TaskOutcome

PROBE_STEP_NAME = "01-probes"


def probe_step(tasks: Sequence[TaskDef], *, index: int = 1) -> Step | None:
    """The parallel hardware-probe step: one container per `parallel` task (dmidecode, lshw, lscpu, lstopo,
    virtualization, nvidia_smi; `gpu_facts` is added by the catalogue like any task). None when the
    selection has no parallel task. `nvidia_smi` and `gpu_facts` are included only on GPU types, which the
    caller expresses by passing only the tasks it wants (precheck result)."""
    raise NotImplementedError


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
    `storage` (priority inf) is always last. `index` numbers the Pod (`NN-<task>`)."""
    raise NotImplementedError


def retry_without_gpu(step: Step, outcomes: Mapping[str, TaskOutcome]) -> Step | None:
    """GPU-first-then-CPU: when a container of a `gpu_runtime` step has `container_start_error`, the same
    step without the runtime class (same containers; the runner creates it once). None when no container
    failed to start or a failed container's task has `gpu=True` (no CPU fallback)."""
    raise NotImplementedError
