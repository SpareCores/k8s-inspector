"""Render Pods and the urls Secret as plain dicts; classify a finished Pod.

Owner: pods agent. Pure functions: no Kubernetes calls, no clock except the `now` argument. Implements
docs/reference/objects.md (anchor, step, flush, urls Secret), run-model.md, node-profile.md "Benchmark Pod
settings", security.md, and exit-codes.md "Where the controller gets the facts". The command lines must match
`cmd/sc-run/main.go` flags (hold, exec, flush) and spikes/p0/*.yaml.

All dicts are ready for `Kube.create("pod", ...)`.
"""

from collections.abc import Sequence
from datetime import datetime

from k8s_inspector.model import (
    BenchmarkRun,
    CapacityType,
    Config,
    OwnerRef,
    PodRole,
    Step,
    StepContainer,
    TaskOutcome,
)

NODE_DIR = "/var/lib/k8s-inspector"  # hostPath on the node; mounted at /k8si in every Pod
MOUNT_DIR = "/k8si"
URLS_MOUNT = "/k8si-urls"
PRIORITY_CLASS = "benchmark"
BENCH_TAINT = "frankenscube/bench"
TERMINATION_GRACE_SECONDS = 20  # sc-run --grace 15s plus 5 s


# --------------------------------------------------------------------------- names and labels


def anchor_name(run_name: str, attempt: int) -> str:
    """`<run name>-a<attempt>`."""
    raise NotImplementedError


def step_pod_name(run_name: str, attempt: int, step: Step) -> str:
    """`<run name>-a<attempt>-<step.name>`; the flush step appends `-flush<n>` via `flush_pod_name`."""
    raise NotImplementedError


def flush_pod_name(run_name: str, attempt: int, step: Step, n: int) -> str:
    raise NotImplementedError


def urls_secret_name(run_name: str) -> str:
    """`<run name>-urls`."""
    raise NotImplementedError


def labels(run: BenchmarkRun, role: PodRole, attempt: int | None = None, task: str | None = None) -> dict[str, str]:
    """`inspector.sparecores.com/{run,role,vendor,attempt,task}` (objects.md "Labels")."""
    raise NotImplementedError


# --------------------------------------------------------------------------- rendering


def node_pool(run: BenchmarkRun, cfg: Config, *, gpu: bool, lowmem: bool) -> str:
    """NodePool name from `cfg.settings.vendors[vendor].node_pools` (cpu | lowmem | gpu). `ValueError` when
    the vendor lacks the pool."""
    raise NotImplementedError


def render_anchor(
    run: BenchmarkRun,
    attempt: int,
    cfg: Config,
    *,
    nodepool: str,
    capacity: Sequence[CapacityType],
    deadline_seconds: int,
    owner: OwnerRef,
) -> dict:
    """The anchor Pod (objects.md "Anchor Pod"): nodeSelector (`karpenter.sh/nodepool`,
    `node.kubernetes.io/instance-type: <vendor>.<sku>`), capacity-type node affinity from `capacity`
    (`[spot, on-demand]` or `[on-demand]`), `do-not-disrupt`, toleration `frankenscube/bench`, container
    `hold` with `cfg.env.sc_run_image` and args `hold --install /k8si/bin --heartbeat-dir
    /k8si/runs/<run_id> --idle-timeout <deadlines.hold_idle_minutes>m`, readiness probe `/sc-run ready
    /k8si/bin`, hostPath volume, owner reference = the BenchmarkRun. `deadline_seconds` is the anchor's
    `activeDeadlineSeconds` (`anchor_deadline_seconds`)."""
    raise NotImplementedError


def step_deadline_seconds(step: Step, cfg: Config) -> int:
    """`activeDeadlineSeconds` of a step: image pull (`deadlines.image_pull_minutes`) + version 1 min + the
    longest container timeout + upload 10 min + slack 5 min. (spikes/p0/step-membench.yaml: 4260 s for a 40 min
    task. objects.md's geekbench example says 5820 s = 11 min slack for 60 min; the spike's 5 min is used.)"""
    raise NotImplementedError


def anchor_deadline_seconds(steps: Sequence[Step], cfg: Config) -> int:
    """Sum of step deadlines plus 1 h (objects.md)."""
    raise NotImplementedError


def render_step(
    run: BenchmarkRun,
    attempt: int,
    step: Step,
    cfg: Config,
    *,
    node_name: str,
    node_allocatable_memory_bytes: int,
    owner: OwnerRef,
) -> dict:
    """A step Pod (objects.md "Step Pod"): `nodeName`, bare Pod, `hostNetwork`, `dnsPolicy: Default`,
    `hostPID` iff any task has `host_pid`, `runtimeClassName: nvidia` iff `step.gpu_runtime`, tolerations
    `Exists`, no service account token, `terminationGracePeriodSeconds: 20`, `activeDeadlineSeconds`
    from `step_deadline_seconds`, owner = the anchor Pod. One container per `step.containers`, each:
    image `container.image`, `command: [/k8si/bin/sc-run, exec, --task <key>, --timeout <t>, --spool
    /k8si/runs/<run_id>/<key>, --urls /k8si-urls/urls.json, --version-cmd ..., --collect ..., --, <entrypoint
    + resource-tracker/nice wrapper + command>]`, `privileged` + `seccompProfile: Unconfined`, requests
    `cpu 10m, memory 64Mi`, memory limit = `task.memory_cap * node_allocatable_memory_bytes` when set (no
    CPU limits ever), `terminationMessagePolicy: File`, env: `TRACKER_*` (project, job name, external run id
    = `run_id`, output path in the spool) + `HF_TOKEN` for llm/vllm only + secrets from `inspector-secrets`
    (`task.secrets`) + `task.env` + `container.env`. Volumes: hostPath `/var/lib/k8s-inspector`, the urls
    Secret, `/models` hostPath when `task.models_cache`, memory emptyDir at /dev/shm when `task.shm_gib`.
    DB tasks (`db_rlimits`) add the sc-run `--rlimit` flags (see cmd/sc-run for what exists today; extend
    the Go flag in a separate change if missing)."""
    raise NotImplementedError


def render_flush_step(
    run: BenchmarkRun,
    attempt: int,
    step: Step,
    n: int,
    cfg: Config,
    *,
    node_name: str,
    owner: OwnerRef,
) -> dict:
    """The `flush` step (role `flush`): sc-run image, `args: [flush, --task <key>, --spool ..., --urls ...]`
    for each container of `step` (one container per key), same mounts, no privileges needed."""
    raise NotImplementedError


def render_urls_secret(run: BenchmarkRun, urls_json: str, owner: OwnerRef) -> dict:
    """Secret `<run>-urls` with `stringData: {"urls.json": urls_json}`, labels, owner = BenchmarkRun."""
    raise NotImplementedError


# --------------------------------------------------------------------------- reading Pods


def pod_node(pod: dict) -> str | None:
    """`spec.nodeName`."""
    raise NotImplementedError


def is_running(pod: dict) -> bool:
    """`status.phase == Running` (anchor ready check also needs `containerStatuses[].ready`)."""
    raise NotImplementedError


def is_ready(pod: dict) -> bool:
    """All containers ready (the `ready` probe passed: `sc-run` is installed in /k8si/bin)."""
    raise NotImplementedError


def pull_stuck(pod: dict, now: datetime, limit_minutes: int) -> bool:
    """True when a container waits in `ErrImagePull`/`ImagePullBackOff`/`ContainerCreating` for longer than
    `limit_minutes` since the Pod started -> the runner deletes the Pod and records `-4 IMAGE_PULL_TIMEOUT`."""
    raise NotImplementedError


def is_finished(pod: dict) -> bool:
    """Phase Succeeded or Failed (including `DeadlineExceeded` and eviction)."""
    raise NotImplementedError


def classify(pod: dict, containers: Sequence[StepContainer]) -> dict[str, TaskOutcome]:
    """Outcome per container key of a FINISHED step Pod (never call for a running Pod).

    Source order, per container (exit-codes.md):
    1. the termination message (JSON written by sc-run, see `internal/task`: `task, exit_code, error_msg,
       timed_out, cancelled, oom_kills, start, end, version, kernel_version, btime, upload_complete,
       missing`) -> `from_sc_run=True`; `timed_out` -> exit -6 with `Execution timed out after <N>s`;
       `oom_kills > 0` and non-zero exit -> 137 + `OOMKilled`; `cancelled` -> -4 `CANCELLED`.
    2. no message: container state `OOMKilled` -> 137 `OOMKilled`; `StartError`/`CreateContainerError`/
       `RunContainerError` -> exit 256 with `container_start_error=True`; Pod reason `DeadlineExceeded` ->
       -4 `DEADLINE_EXCEEDED`; Pod `Evicted` -> -4 `EVICTED`; anything else non-zero -> 256 with the
       message, `from_sc_run=False`.
    Every key of `containers` is present in the result. Missing message but a clean exit 0 is a harness
    error (256): sc-run always writes one."""
    raise NotImplementedError
