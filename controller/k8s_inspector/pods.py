"""Render Pods and the urls Secret as plain dicts; classify a finished Pod.

Owner: pods agent. Pure functions: no Kubernetes calls, no clock except the `now` argument. Implements
docs/reference/objects.md (anchor, step, flush, urls Secret), run-model.md, node-profile.md "Benchmark Pod
settings", security.md, and exit-codes.md "Where the controller gets the facts". The command lines must match
`cmd/sc-run/main.go` flags (hold, exec, flush) and spikes/p0/*.yaml.

All dicts are ready for `Kube.create("pod", ...)`.
"""

import json
import re
import shlex
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from k8s_inspector.model import (
    BenchmarkRun,
    CapacityType,
    Config,
    ExitCode,
    OwnerRef,
    PodRole,
    Reason,
    Step,
    StepContainer,
    TaskDef,
    TaskOutcome,
)
from k8s_inspector.steps import VLLM_PROBE_TIMEOUT

NODEPOOL_LABEL = "karpenter.sh/nodepool"
VENDOR_LABEL = "scarpenter.sparecores.com/vendor"
API_REFERENCE_LABEL = "scarpenter.sparecores.com/api-reference"
_LABEL_VALUE = re.compile(r"(?:[A-Za-z0-9](?:[-_.A-Za-z0-9]{0,61}[A-Za-z0-9])?)?")

NODE_DIR = "/var/lib/k8s-inspector"  # hostPath on the node; mounted at /k8si in every Pod
MOUNT_DIR = "/k8si"
URLS_MOUNT = "/k8si-urls"
PRIORITY_CLASS = "benchmark"
BENCH_TAINT = "frankenscube/bench"
TERMINATION_GRACE_SECONDS = 20  # sc-run --grace 15s plus 5 s


SC_RUN = f"{MOUNT_DIR}/bin/sc-run"
HEARTBEAT_ROOT = f"{MOUNT_DIR}/runs"
MODELS_DIR = f"{NODE_DIR}/models"
SECRETS_NAME = "inspector-secrets"
FLUSH_DEADLINE_SECONDS = 900  # upload budget 10 min plus 5 min
FLUSH_STEPS = 2  # flush Pods per step before its outputs are given up
WATCHDOG_MARGIN = timedelta(minutes=2)  # a Pod this long past deadline + grace is wedged (containerd), not slow
DEFAULT_GRACE_SECONDS = 30  # Kubernetes' terminationGracePeriodSeconds when the spec has none
STEP_SLACK = timedelta(minutes=5)
STEP_VERSION = timedelta(minutes=1)
STEP_UPLOAD = timedelta(minutes=10)
ANCHOR_EXTRA = timedelta(hours=1)
DB_RLIMITS = ("nofile=1048576", "memlock=unlimited")  # docker era: nofile 1048576, memlock -1
# `inspector-secrets` key -> environment variable of the container.
SECRET_ENV = {
    "sentinel": "SENTINEL_API_TOKEN",
    "geekbench": "BENCHMARK_SECRETS_PASSPHRASE",
    "huggingface": "HF_TOKEN",
}
START_ERRORS = {"StartError", "CreateContainerError", "RunContainerError", "ContainerCannotRun"}
PULL_WAITING = {"ErrImagePull", "ImagePullBackOff", "ContainerCreating"}
# Waiting reasons that never resolve by themselves (bad image name, missing Secret key, `imagePullPolicy: Never`).
CONFIG_WAITING = {"CreateContainerConfigError", "InvalidImageName", "ErrImageNeverPull"}


# --------------------------------------------------------------------------- names and labels


def anchor_name(run_name: str, attempt: int) -> str:
    """`<run name>-a<attempt>`."""
    return f"{run_name}-a{attempt}"


def step_pod_name(run_name: str, attempt: int, step: Step) -> str:
    """`<run name>-a<attempt>-<step.name>`; the flush step appends `-flush<n>` via `flush_pod_name`."""
    return f"{anchor_name(run_name, attempt)}-{step.name}"


def flush_pod_name(run_name: str, attempt: int, step: Step, n: int) -> str:
    return f"{step_pod_name(run_name, attempt, step)}-flush{n}"


def urls_secret_name(run_name: str) -> str:
    """`<run name>-urls`."""
    return f"{run_name}-urls"


def container_name(key: str) -> str:
    """DNS label of a container: the key lowercased, other characters replaced by `-`."""
    return re.sub(r"[^a-z0-9-]", "-", key.lower())[:63]


LABEL_ROLE = "inspector.sparecores.com/role"


def labels(run: BenchmarkRun, role: PodRole, attempt: int | None = None, task: str | None = None) -> dict[str, str]:
    """`inspector.sparecores.com/{run,role,vendor,attempt,task}` (objects.md "Labels")."""
    result = {
        "inspector.sparecores.com/run": run.name,
        LABEL_ROLE: role.value,
        "inspector.sparecores.com/vendor": run.spec.vendor,
    }
    if attempt is not None:
        result["inspector.sparecores.com/attempt"] = str(attempt)
    if task is not None:
        result["inspector.sparecores.com/task"] = task
    return result


def _step_task_label(step: Step) -> str:
    return step.containers[0].key if len(step.containers) == 1 else "probe"


def _owner_refs(owner: OwnerRef) -> list[dict]:
    # No blockOwnerDeletion: the controller's ServiceAccount cannot set finalizers on the owner.
    return [
        {"apiVersion": owner.api_version, "kind": owner.kind, "name": owner.name, "uid": owner.uid, "controller": True}
    ]


def _duration(value: timedelta) -> str:
    """Go duration for sc-run flags: `40m`, `26h`, `90s`."""
    seconds = int(value.total_seconds())
    for unit, size in (("h", 3600), ("m", 60)):
        if seconds % size == 0:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


# --------------------------------------------------------------------------- rendering


def is_label_value(value: str) -> bool:
    """Kubernetes label value rules: at most 63 characters, alphanumeric ends, `-_.` and alphanumerics inside."""
    return _LABEL_VALUE.fullmatch(value) is not None and bool(value)


def render_anchor(
    run: BenchmarkRun,
    attempt: int,
    cfg: Config,
    *,
    capacity: Sequence[CapacityType],
    deadline_seconds: int,
    owner: OwnerRef,
) -> dict:
    """The anchor Pod (objects.md "Anchor Pod"): nodeSelector (`karpenter.sh/nodepool: <benchPool>`,
    `scarpenter.sparecores.com/vendor: <vendor>`, `scarpenter.sparecores.com/api-reference: <sku>`),
    capacity-type node affinity from `capacity` (`[spot, on-demand]` or `[on-demand]`),
    `do-not-disrupt`, toleration `frankenscube/bench`, container
    `hold` with `cfg.env.sc_run_image` and args `hold --install /k8si/bin --heartbeat-dir
    /k8si/runs/<run_id> --idle-timeout <deadlines.hold_idle_minutes>m`, readiness probe `/sc-run ready
    /k8si/bin`, hostPath volume, owner reference = the BenchmarkRun. `deadline_seconds` is the anchor's
    `activeDeadlineSeconds` (`anchor_deadline_seconds`)."""
    spec = run.spec
    idle = cfg.settings.deadlines.hold_idle_minutes
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": anchor_name(run.name, attempt),
            "namespace": cfg.env.namespace,
            "labels": labels(run, PodRole.ANCHOR, attempt),
            "annotations": {"karpenter.sh/do-not-disrupt": "true"},
            "ownerReferences": _owner_refs(owner),
        },
        "spec": {
            "restartPolicy": "Never",
            "activeDeadlineSeconds": deadline_seconds,
            "priorityClassName": PRIORITY_CLASS,
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "hostNetwork": True,
            "dnsPolicy": "Default",
            "nodeSelector": {
                "karpenter.sh/nodepool": cfg.settings.bench_pool,
                VENDOR_LABEL: spec.vendor,
                API_REFERENCE_LABEL: spec.sku,
            },
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "karpenter.sh/capacity-type",
                                        "operator": "In",
                                        "values": [c.value for c in capacity],
                                    }
                                ]
                            }
                        ]
                    }
                }
            },
            "tolerations": [{"key": BENCH_TAINT, "operator": "Exists"}],
            "containers": [
                {
                    "name": "hold",
                    "image": cfg.env.sc_run_image,
                    "args": [
                        "hold",
                        "--install",
                        f"{MOUNT_DIR}/bin",
                        "--heartbeat-dir",
                        f"{HEARTBEAT_ROOT}/{spec.run_id}",
                        "--idle-timeout",
                        f"{idle}m",
                    ],
                    "readinessProbe": {
                        "exec": {"command": ["/sc-run", "ready", f"{MOUNT_DIR}/bin"]},
                        "periodSeconds": 2,
                    },
                    "resources": {"requests": {"cpu": "1m", "memory": "16Mi"}},
                    "volumeMounts": [{"name": "k8si", "mountPath": MOUNT_DIR}],
                }
            ],
            "volumes": [{"name": "k8si", "hostPath": {"path": NODE_DIR, "type": "DirectoryOrCreate"}}],
        },
    }


def _step_budget(longest: timedelta, cfg: Config) -> timedelta:
    """Image pull (`deadlines.image_pull_minutes`) + version 1 min + the longest container timeout + upload
    10 min + slack 5 min."""
    pull = timedelta(minutes=cfg.settings.deadlines.image_pull_minutes)
    return pull + STEP_VERSION + longest + STEP_UPLOAD + STEP_SLACK


def step_deadline_seconds(step: Step, cfg: Config) -> int:
    """`activeDeadlineSeconds` of a step (a 40 min task: 4260 s with a 15 min pull)."""
    longest = max(container_timeout(c) for c in step.containers)
    return int(_step_budget(longest, cfg).total_seconds())


def anchor_deadline_seconds(selected: Sequence[TaskDef], cfg: Config, hold_after: timedelta = timedelta(0)) -> int:
    """The anchor's `activeDeadlineSeconds`: the budgets of the probe step and of every serial task (one probe
    step per vLLM image besides), two flush Pods for each of them, `hold_after` and 1 h (objects.md)."""
    longest = [max((t.timeout for t in selected if t.parallel), default=STEP_SLACK)]
    for task in selected:
        if not task.parallel:
            longest += [task.timeout, *[VLLM_PROBE_TIMEOUT for _ in task.images]]
    flushes = timedelta(seconds=FLUSH_DEADLINE_SECONDS * FLUSH_STEPS)
    total = sum((_step_budget(t, cfg) + flushes for t in longest), hold_after + ANCHOR_EXTRA)
    return int(total.total_seconds())


def _env(container: StepContainer, run_id: str, *, gpu_runtime: bool) -> list[dict]:
    """Environment of a step container. A dict keyed by name, so a later source overrides an earlier one."""
    task = container.task
    env: dict[str, dict] = {}

    def add(name: str, value: str) -> None:
        env[name] = {"name": name, "value": value}

    if not task.parallel:  # the probe group runs without resource-tracker, as in sc-inspector
        add("TRACKER_PROJECT_NAME", "inspector")
        add("TRACKER_JOB_NAME", task.name)
        add("TRACKER_EXTERNAL_RUN_ID", run_id)
        add("TRACKER_CONTAINER_IMAGE", container.image)
        add("TRACKER_QUIET", "false")
        add("TRACKER_OUTPUT", f"{HEARTBEAT_ROOT}/{run_id}/{container.key}/resource_tracker.jsonl")
    secrets = list(task.secrets)
    if task.models_cache and "huggingface" not in secrets:  # llm and vllm download models from the Hub
        secrets.append("huggingface")
    for key in secrets:
        name = SECRET_ENV[key]
        env[name] = {"name": name, "valueFrom": {"secretKeyRef": {"name": SECRETS_NAME, "key": key}}}
    if gpu_runtime:
        add("NVIDIA_VISIBLE_DEVICES", "all")
        add("NVIDIA_DRIVER_CAPABILITIES", "all")
    for name, value in (*task.env, *container.env):
        add(name, value)
    return list(env.values())


def _docker_argv(task: TaskDef, command: str | None) -> list[str]:
    """`docker run <image> <command>` semantics: the image's entrypoint restated plus the command
    (shell-split like docker does for a string)."""
    argv = [*task.entrypoint, *(shlex.split(command) if command else [])]
    if not argv:
        raise ValueError(f"task {task.name!r} has neither entrypoint nor command")
    return argv


def _argv(container: StepContainer) -> list[str]:
    command = container.command if container.command is not None else container.task.command
    return _docker_argv(container.task, command)


def _version_cmd(task: TaskDef) -> str:
    """`--version-cmd` text. sc-run runs it with `sh -c`, so the legacy value (arguments of the image, like
    `--version`) becomes a full command line: entrypoint plus version command, shell-quoted."""
    return shlex.join(_docker_argv(task, task.version_command))


def container_timeout(container: StepContainer) -> timedelta:
    """The container's own timeout, else its task's."""
    return container.timeout or container.task.timeout


def _step_container(
    container: StepContainer,
    run_id: str,
    cfg: Config,
    *,
    gpu_runtime: bool,
    gpu_count: int,
    single: bool,
    allocatable: int,
) -> dict:
    task = container.task
    spool = f"{HEARTBEAT_ROOT}/{run_id}/{container.key}"
    uploads = cfg.settings.uploads
    command = [
        SC_RUN,
        "exec",
        "--task",
        container.key,
        "--timeout",
        _duration(container_timeout(container)),
        "--multipart-mib",
        str(uploads.single_put_max_mib),
        "--reput",
        f"{uploads.reput_seconds}s",
        "--spool",
        spool,
        "--urls",
        f"{URLS_MOUNT}/urls.json",
        "--heartbeat-dir",
        f"{HEARTBEAT_ROOT}/{run_id}",
    ]
    if task.version_command:
        command += ["--version-cmd", _version_cmd(task)]
    for spec in task.collect:
        command += ["--collect", spec]
    if "db_rlimits" in task.privileges:
        for rlimit in DB_RLIMITS:
            command += ["--rlimit", rlimit]
    command += ["--", *_argv(container)]

    mounts = [
        {"name": "k8si", "mountPath": MOUNT_DIR},
        {"name": "urls", "mountPath": URLS_MOUNT, "readOnly": True},
    ]
    if task.models_cache:
        mounts.append({"name": "models", "mountPath": "/models"})
    if task.shm_gib:
        mounts.append({"name": "dshm", "mountPath": "/dev/shm"})
    resources: dict = {"requests": {"cpu": "10m", "memory": "64Mi"}}
    if task.memory_cap is not None:
        resources["limits"] = {"memory": str(int(task.memory_cap * allocatable))}
    if gpu_runtime and gpu_count and single:
        # Extended resources are exclusive: only a single-container step can claim every GPU.
        resources.setdefault("limits", {})["nvidia.com/gpu"] = str(gpu_count)
    return {
        "name": container_name(container.key),
        "image": container.image,
        "command": command,
        "env": _env(container, run_id, gpu_runtime=gpu_runtime),
        "securityContext": {"privileged": True, "seccompProfile": {"type": "Unconfined"}},
        "resources": resources,
        "terminationMessagePolicy": "File",
        "volumeMounts": mounts,
    }


def _volumes(run: BenchmarkRun, step: Step) -> list[dict]:
    volumes = [
        {"name": "k8si", "hostPath": {"path": NODE_DIR, "type": "Directory"}},
        {"name": "urls", "secret": {"secretName": urls_secret_name(run.name)}},
    ]
    if any(c.task.models_cache for c in step.containers):
        volumes.append({"name": "models", "hostPath": {"path": MODELS_DIR, "type": "DirectoryOrCreate"}})
    for c in step.containers:
        if c.task.shm_gib:
            volumes.append({"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": f"{c.task.shm_gib}Gi"}})
            break
    return volumes


def render_step(
    run: BenchmarkRun,
    attempt: int,
    step: Step,
    cfg: Config,
    *,
    node_name: str,
    node_allocatable_memory_bytes: int,
    owner: OwnerRef,
    gpu_count: int = 0,
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
    DB tasks (`db_rlimits`) add the sc-run `--rlimit` flags.

    `gpu_count` (extra keyword, default 0): the node's GPU count; a single-container GPU step then also
    requests all `nvidia.com/gpu`. Every GPU step exposes the GPUs through `NVIDIA_VISIBLE_DEVICES=all`."""
    run_id = run.spec.run_id
    single = len(step.containers) == 1
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": step_pod_name(run.name, attempt, step),
            "namespace": cfg.env.namespace,
            "labels": labels(run, PodRole.STEP, attempt, _step_task_label(step)),
            "ownerReferences": _owner_refs(owner),
        },
        "spec": {
            "nodeName": node_name,
            "restartPolicy": "Never",
            "activeDeadlineSeconds": step_deadline_seconds(step, cfg),
            "terminationGracePeriodSeconds": TERMINATION_GRACE_SECONDS,
            "hostNetwork": True,
            "dnsPolicy": "Default",
            "hostPID": any("host_pid" in c.task.privileges for c in step.containers),
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "tolerations": [{"operator": "Exists"}],
            **({"runtimeClassName": "nvidia"} if step.gpu_runtime else {}),
            "containers": [
                _step_container(
                    c,
                    run_id,
                    cfg,
                    gpu_runtime=step.gpu_runtime,
                    gpu_count=gpu_count,
                    single=single,
                    allocatable=node_allocatable_memory_bytes,
                )
                for c in step.containers
            ],
            "volumes": _volumes(run, step),
        },
    }


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
    run_id = run.spec.run_id
    containers = [
        {
            "name": container_name(c.key),
            "image": cfg.env.sc_run_image,
            "args": [
                "flush",
                "--task",
                c.key,
                "--spool",
                f"{HEARTBEAT_ROOT}/{run_id}/{c.key}",
                "--urls",
                f"{URLS_MOUNT}/urls.json",
                "--multipart-mib",
                str(cfg.settings.uploads.single_put_max_mib),
            ],
            "resources": {"requests": {"cpu": "1m", "memory": "16Mi"}},
            "terminationMessagePolicy": "File",
            "volumeMounts": [
                {"name": "k8si", "mountPath": MOUNT_DIR},
                {"name": "urls", "mountPath": URLS_MOUNT, "readOnly": True},
            ],
        }
        for c in step.containers
    ]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": flush_pod_name(run.name, attempt, step, n),
            "namespace": cfg.env.namespace,
            "labels": labels(run, PodRole.FLUSH, attempt, _step_task_label(step)),
            "ownerReferences": _owner_refs(owner),
        },
        "spec": {
            "nodeName": node_name,
            "restartPolicy": "Never",
            "activeDeadlineSeconds": FLUSH_DEADLINE_SECONDS,
            "hostNetwork": True,
            "dnsPolicy": "Default",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "tolerations": [{"operator": "Exists"}],
            "containers": containers,
            "volumes": _volumes(run, Step(step.index, step.name, ())),
        },
    }


def render_urls_secret(run: BenchmarkRun, urls_json: str, owner: OwnerRef) -> dict:
    """Secret `<run>-urls` with `stringData: {"urls.json": urls_json}`, labels, owner = BenchmarkRun."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": urls_secret_name(run.name),
            "namespace": run.namespace,
            "labels": {
                "inspector.sparecores.com/run": run.name,
                "inspector.sparecores.com/vendor": run.spec.vendor,
            },
            "ownerReferences": _owner_refs(owner),
        },
        "type": "Opaque",
        "stringData": {"urls.json": urls_json},
    }


# --------------------------------------------------------------------------- reading Pods


def pod_node(pod: dict) -> str | None:
    """`spec.nodeName`."""
    return pod.get("spec", {}).get("nodeName")


def _phase(pod: dict) -> str | None:
    return pod.get("status", {}).get("phase")


def _container_statuses(pod: dict) -> list[dict]:
    return pod.get("status", {}).get("containerStatuses") or []


def is_running(pod: dict) -> bool:
    """`status.phase == Running` (anchor ready check also needs `containerStatuses[].ready`)."""
    return _phase(pod) == "Running"


def is_ready(pod: dict) -> bool:
    """All containers ready (the `ready` probe passed: `sc-run` is installed in /k8si/bin)."""
    statuses = _container_statuses(pod)
    return bool(statuses) and all(s.get("ready") for s in statuses)


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def stuck_containers(pod: dict, now: datetime, limit_minutes: int) -> dict[str, str]:
    """Container name -> `error_msg` of the containers that wait for longer than `limit_minutes` since the Pod
    started: pulling their image (`ErrImagePull`/`ImagePullBackOff`/`ContainerCreating`, all of them while the
    kubelet has not created the sandbox) -> `IMAGE_PULL_TIMEOUT`; or in a waiting state that never resolves
    (`CONFIG_WAITING`) -> the waiting message. The runner ends their tasks with `-4`; containers that started
    are left alone."""
    started = _parse_time(pod.get("status", {}).get("startTime"))
    if started is None or now - started <= timedelta(minutes=limit_minutes):
        return {}
    statuses = _container_statuses(pod)
    if not statuses:  # admitted by the kubelet, sandbox still being created
        if _phase(pod) != "Pending":
            return {}
        return {c["name"]: Reason.IMAGE_PULL_TIMEOUT.value for c in pod.get("spec", {}).get("containers", [])}
    stuck = {}
    for s in statuses:
        waiting = s.get("state", {}).get("waiting", {})
        if waiting.get("reason") in PULL_WAITING:
            stuck[s["name"]] = Reason.IMAGE_PULL_TIMEOUT.value
        elif waiting.get("reason") in CONFIG_WAITING:
            stuck[s["name"]] = waiting.get("message") or waiting["reason"]
    return stuck


def overdue(pod: dict, now: datetime) -> bool:
    """A Pod that is not finished although `activeDeadlineSeconds` + its grace period + `WATCHDOG_MARGIN` have
    passed since `startTime`: the kubelet should have failed it long ago (P0: a wedged containerd left it
    `Running` for 10 minutes). Not for Pods that are being deleted (`terminating_overdue`)."""
    started = _parse_time(pod.get("status", {}).get("startTime"))
    deadline = pod.get("spec", {}).get("activeDeadlineSeconds")
    if started is None or deadline is None or is_finished(pod) or terminating(pod):
        return False
    grace = pod["spec"].get("terminationGracePeriodSeconds", DEFAULT_GRACE_SECONDS)
    return now > started + timedelta(seconds=deadline + grace) + WATCHDOG_MARGIN


def terminating(pod: dict) -> bool:
    return bool(pod.get("metadata", {}).get("deletionTimestamp"))


def terminating_overdue(pod: dict, now: datetime) -> bool:
    """Deleted, but still there `WATCHDOG_MARGIN` after the end of its grace period (`deletionTimestamp` is the
    request time plus the grace period)."""
    deleted = _parse_time(pod.get("metadata", {}).get("deletionTimestamp"))
    return deleted is not None and now > deleted + WATCHDOG_MARGIN


def is_rejected(pod: dict) -> bool:
    """The kubelet refused the Pod at admission (`Failed`, reason `Evicted`, "Pod was rejected: ...", e.g. while
    the node has DiskPressure): no container ever started."""
    status = pod.get("status", {})
    return (
        status.get("phase") == "Failed"
        and status.get("reason") == "Evicted"
        and str(status.get("message", "")).startswith("Pod was rejected")
        and not any(
            "running" in s.get("state", {}) or "terminated" in s.get("state", {}) for s in _container_statuses(pod)
        )
    )


def unfinished_containers(pod: dict) -> set[str]:
    """Names of the containers that have not terminated."""
    return {s["name"] for s in _container_statuses(pod) if "terminated" not in s.get("state", {})}


def is_finished(pod: dict) -> bool:
    """Phase Succeeded or Failed (including `DeadlineExceeded` and eviction)."""
    return _phase(pod) in ("Succeeded", "Failed")


def _termination_message(status: dict) -> dict | None:
    """sc-run's JSON termination message of a container, None when absent or not sc-run's."""
    raw = status.get("state", {}).get("terminated", {}).get("message")
    if not raw:
        return None
    try:
        message = json.loads(raw)
    except ValueError:
        return None
    return message if isinstance(message, dict) and "exit_code" in message else None


def outcome_from_message(key: str, msg: dict, container: StepContainer, pod_reason: str | None) -> TaskOutcome:
    exit_code = int(msg["exit_code"])
    error_msg = msg.get("error_msg") or None
    if msg.get("timed_out"):
        exit_code = ExitCode.TIMEOUT
        error_msg = error_msg or f"Execution timed out after {int(container_timeout(container).total_seconds())}s"
    elif msg.get("cancelled"):
        # The kubelet's SIGTERM at the Pod deadline or an eviction looks like a cancel to sc-run.
        exit_code = ExitCode.NEVER_FINISHED
        error_msg = {"DeadlineExceeded": Reason.DEADLINE_EXCEEDED, "Evicted": Reason.EVICTED}.get(
            pod_reason, Reason.CANCELLED
        ).value
    elif msg.get("oom_kills", 0) > 0 and exit_code != 0:
        exit_code, error_msg = 137, Reason.OOM_KILLED.value
    return TaskOutcome(
        key=key,
        exit_code=int(exit_code),
        error_msg=error_msg,
        start=_parse_time(msg.get("start")),
        end=_parse_time(msg.get("end")),
        version=msg.get("version") or None,
        kernel_version=msg.get("kernel_version") or None,
        btime=datetime.fromtimestamp(msg["btime"], UTC) if msg.get("btime") else None,
        timed_out=bool(msg.get("timed_out")),
        cancelled=bool(msg.get("cancelled")),
        oom_kills=int(msg.get("oom_kills", 0)),
        upload_complete=msg.get("upload_complete"),
        missing_files=int(msg.get("missing", 0)),
    )


def _outcome_from_state(key: str, status: dict | None, pod: dict) -> TaskOutcome:
    """No sc-run message: derive the outcome from the container state and the Pod."""
    state = (status or {}).get("state", {})
    terminated = state.get("terminated", {})
    waiting = state.get("waiting", {})
    reason = terminated.get("reason") or waiting.get("reason")
    detail = terminated.get("message") or waiting.get("message") or reason
    pod_reason = pod.get("status", {}).get("reason")
    outcome = TaskOutcome(
        key=key,
        exit_code=ExitCode.HARNESS_ERROR,
        start=_parse_time(terminated.get("startedAt")),
        end=_parse_time(terminated.get("finishedAt")),
        from_sc_run=False,
    )
    if reason == "OOMKilled":
        outcome.exit_code, outcome.error_msg = 137, Reason.OOM_KILLED.value
    elif reason in START_ERRORS:
        outcome.error_msg, outcome.container_start_error = detail, True
    elif pod_reason == "DeadlineExceeded":
        outcome.exit_code, outcome.error_msg = ExitCode.NEVER_FINISHED, Reason.DEADLINE_EXCEEDED.value
    elif is_rejected(pod):
        outcome.exit_code, outcome.error_msg = ExitCode.NEVER_FINISHED, pod["status"]["message"]
        outcome.container_start_error = True  # never started: nothing to flush
    elif pod_reason == "Evicted":
        outcome.exit_code, outcome.error_msg = ExitCode.NEVER_FINISHED, Reason.EVICTED.value
    elif terminated and terminated.get("exitCode") == 0:
        outcome.error_msg = "no termination message from sc-run"  # sc-run always writes one
    elif terminated:
        outcome.error_msg = detail or f"exit code {terminated.get('exitCode')}"
    else:
        outcome.error_msg = detail or pod.get("status", {}).get("message") or "container did not run"
    return outcome


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
    by_name = {container_name(s.get("name", "")): s for s in _container_statuses(pod)}
    pod_reason = pod.get("status", {}).get("reason")
    result = {}
    for container in containers:
        status = by_name.get(container_name(container.key))
        message = _termination_message(status) if status else None
        if message is not None:
            result[container.key] = outcome_from_message(container.key, message, container, pod_reason)
        else:
            result[container.key] = _outcome_from_state(container.key, status, pod)
    return result


def _claim_requirement(claim: dict, key: str) -> str | None:
    """The single value of an `In` requirement in `spec.requirements` (copied from the anchor's nodeSelector)."""
    for req in claim.get("spec", {}).get("requirements") or []:
        if req.get("key") == key and req.get("operator") == "In" and len(req.get("values") or []) == 1:
            return req["values"][0]
    return None


def claim_api_reference(claim: dict) -> str | None:
    """A NodeClaim's raw SKU name: the label (after launch), else the `spec.requirements` entry (from creation)."""
    labels = claim.get("metadata", {}).get("labels") or {}
    return labels.get(API_REFERENCE_LABEL) or _claim_requirement(claim, API_REFERENCE_LABEL)


def claim_vendor(claim: dict) -> str | None:
    """A NodeClaim's vendor: the label (after launch), else the vendor requirement (before launch only
    `spec.requirements` carries it)."""
    labels = claim.get("metadata", {}).get("labels") or {}
    return labels.get(VENDOR_LABEL) or _claim_requirement(claim, VENDOR_LABEL)
