"""The task catalogue, the task hash and task selection.

Owner: tasks agent. Implements docs/reference/tasks.md. The catalogue is data in `catalogue/tasks.yaml`
(next to this file, shipped with the package, conventions at the top of the file), ported from sc-inspector
`inspector/tasks.py` and `dbaas_tasks.py`; this module loads it into `model.TaskDef`s.

The hash equals sc-inspector's `lib.task_hash` byte for byte (golden test against hashes computed from the legacy
module and present in the sc-inspector-data metas): SHA-1 over the sorted keys of {command, transform_output, image,
workload_proxy, durability} that the task has, each key name (ascii) followed by `str(value)`; `transform_output` is
the concatenation of the legacy `inspect.getsource` text of each transform function. A key counts as "had" when
the legacy task object has the attribute: `command` and `image` always (None command hashes as "None"),
`workload_proxy` and `durability` only when not None.
With `revision > 0`, `revision` joins the key set before sorting (docs say "a sixth key" without an order; no
existing hash is affected, so this is a free choice).
"""

import hashlib
import re
from datetime import timedelta
from functools import cache
from pathlib import Path

import yaml

from k8s_inspector.model import NodeFacts, TaskDef

# Verbatim legacy source of each transform function (`inspect.getsource` text of sc-inspector
# inspector/transform.py `raw` and `fetch_geekbench_results`, including the trailing newline). Part of the
# hash, so it must never be reformatted. Keys are the names used in TaskDef.transforms.
TRANSFORM_SOURCES: dict[str, str] = {
    "raw": r"""def raw(meta, task, task_dir, stdout, stderr) -> list[str]:
    outputs: list[str] = []
    for name in ("stdout", "stderr"):
        path = os.path.join(task_dir, name)
        data = locals()[name]
        if len(data):
            with open(path, "wb") as f:
                f.write(data)
            outputs.append(name)
        elif os.path.exists(path):
            # Drop stale stream from a prior run so meta.outputs matches the tree.
            os.remove(path)

    return outputs
""",
    "fetch_geekbench_results": r"""def fetch_geekbench_results(meta, task, task_dir, stdout, stderr) -> list[str]:
    from geekbench import (
        geekbench_html_to_json,
        geekbench_upload_document_from_stderr,
        geekbench_upload_document_to_json,
    )

    outputs: list[str] = []
    document = geekbench_upload_document_from_stderr(stderr)
    if document:
        results = geekbench_upload_document_to_json(document)
    else:
        urls = re.findall(
            re.compile(r'https://[^\s"]*/v6/cpu[^\s"]*'), stdout.decode("utf-8")
        )
        if not urls:
            return outputs
        # float timeout = connect+read budget (requests sessions.py / adapters)
        res = requests.get(urls[0], timeout=60)
        assert 200 <= res.status_code < 300, f"Status code is {res.status_code}"
        results = geekbench_html_to_json(res.text)
    with open(os.path.join(task_dir, "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    outputs.append("results.json")

    return outputs
""",
}

CATALOGUE_PATH = Path(__file__).parent / "catalogue" / "tasks.yaml"

_NAME = re.compile(r"[a-z0-9_]+")
_DURATION = re.compile(r"(?:\d+[smh])+")
_DURATION_PART = re.compile(r"(\d+)([smh])")
_SECONDS = {"s": 1, "m": 60, "h": 3600}

# YAML keys that are copied as they are; the others are converted in `_task_from_yaml`.
_PLAIN_KEYS = {
    "digest",
    "version_command",
    "priority",
    "parallel",
    "parse",
    "memory_cap",
    "shm_gib",
    "gpu",
    "precheck_command",
    "precheck_regex",
    "workload_proxy",
    "dbaas",
    "durability",
    "revision",
    "spot",
    "models_cache",
}
_TUPLE_KEYS = {"entrypoint", "transforms", "secrets", "collect", "large_outputs"}
_OTHER_KEYS = {"name", "command", "image", "images", "digests", "timeout", "env", "privileges"}


def _parse_duration(text: str) -> timedelta:
    if not _DURATION.fullmatch(text):
        raise ValueError(f"bad duration {text!r}, expected for example 30s, 40m, 26h or 1h30m")
    seconds = sum(int(n) * _SECONDS[unit] for n, unit in _DURATION_PART.findall(text))
    return timedelta(seconds=seconds)


def _task_from_yaml(entry: dict) -> TaskDef:
    unknown = entry.keys() - _PLAIN_KEYS - _TUPLE_KEYS - _OTHER_KEYS
    if unknown:
        raise ValueError(f"task {entry.get('name')!r}: unknown keys {sorted(unknown)}")
    name = entry.get("name", "")
    if not _NAME.fullmatch(name):
        raise ValueError(f"bad task name {name!r}: lowercase letters, digits and underscore only")
    if "timeout" not in entry:
        raise ValueError(f"task {name}: timeout is required")

    fields: dict = {key: entry[key] for key in _PLAIN_KEYS if key in entry}
    fields.update({key: tuple(entry[key]) for key in _TUPLE_KEYS if key in entry})
    fields["name"] = name
    fields["timeout"] = _parse_duration(entry["timeout"])
    fields["env"] = tuple((key, str(value)) for key, value in entry.get("env", {}).items())
    fields["privileges"] = frozenset(entry.get("privileges", ()))
    fields["command"] = entry.get("command")  # a missing key is null
    if "priority" in fields:
        fields["priority"] = float(fields["priority"])

    if "images" in entry:  # vLLM ladder: the hashed image is the three joined with "|"
        fields["images"] = tuple(entry["images"])
        fields["image"] = "|".join(entry["images"])
        fields["image_digests"] = dict(entry.get("digests", {}))
    elif "image" in entry:
        fields["image"] = entry["image"]
    else:
        raise ValueError(f"task {name}: image or images is required")

    for transform in fields.get("transforms", ()):
        if transform not in TRANSFORM_SOURCES:
            raise ValueError(f"task {name}: unknown transform {transform!r}")
    return TaskDef(**fields)


def load_catalogue(path: Path = CATALOGUE_PATH) -> list[TaskDef]:
    """Parse the YAML into TaskDefs (durations like `40m`/`26h`, `digest` optional), validating names
    (lowercase `[a-z0-9_]`) and that every transform name is in `TRANSFORM_SOURCES`."""
    entries = yaml.safe_load(path.read_text())["tasks"]
    tasks = [_task_from_yaml(entry) for entry in entries]
    names = [task.name for task in tasks]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"duplicate task names: {duplicates}")
    return tasks


@cache
def _catalogue() -> tuple[TaskDef, ...]:
    return tuple(sorted(load_catalogue(), key=lambda task: (task.priority, not task.parallel, task.name)))


def all_tasks() -> list[TaskDef]:
    """Every runnable task of the catalogue, ordered by (priority, not parallel, name). Cached. The legacy
    `timing` task is not in the catalogue: timing files are publisher output."""
    return list(_catalogue())


def get_task(name: str) -> TaskDef:
    """Raise `KeyError` for an unknown name."""
    for task in _catalogue():
        if task.name == name:
            return task
    raise KeyError(name)


def task_hash(task: TaskDef) -> str:
    """Legacy-compatible hash, see the module docstring."""
    values: dict[str, object] = {
        "command": task.command,
        "transform_output": "".join(TRANSFORM_SOURCES[name] for name in task.transforms),
        "image": task.image,
    }
    if task.workload_proxy is not None:
        values["workload_proxy"] = task.workload_proxy
    if task.durability is not None:
        values["durability"] = task.durability
    if task.revision > 0:
        values["revision"] = task.revision

    digest = hashlib.sha1()
    for key in sorted(values):
        digest.update(key.encode("ascii"))
        digest.update(str(values[key]).encode("ascii"))
    return digest.hexdigest()


def image_ref(task: TaskDef, image: str | None = None) -> str:
    """Pod image reference: `<image>@<digest>` (digest of the task); `image` selects a rung of the vLLM
    ladder, default `task.image`. Raise `ValueError` when the task has no digest."""
    if task.images:  # the ladder: a rung must be named, each has its own digest
        if image not in task.images:
            raise ValueError(f"task {task.name}: image must be one of {list(task.images)}, got {image!r}")
        digest = task.image_digests.get(image)
    else:
        if image not in (None, task.image):
            raise ValueError(f"task {task.name}: unknown image {image!r}")
        image = task.image
        digest = task.digest
    if digest is None:
        raise ValueError(f"task {task.name}: no digest for image {image}")
    return f"{image}@{digest}"


def select_tasks(names: list[str]) -> list[TaskDef]:
    """The tasks of a run: `names` (empty = every server task of the catalogue, DBaaS tasks only when named),
    ordered like `all_tasks()`. Raise `ValueError` on unknown names. Eligibility (Keeper block reasons,
    due-ness) is the planner's job."""
    known = {task.name for task in _catalogue()}
    unknown = sorted(set(names) - known)
    if unknown:
        raise ValueError(f"unknown tasks: {unknown}")
    if not names:
        return [task for task in _catalogue() if not task.dbaas]
    return [task for task in _catalogue() if task.name in names]


def precheck(task: TaskDef, facts: NodeFacts) -> str | None:
    """None when the task may run on the node, else the reason text for exit code -3 (for example
    "GPU task on a node without an NVIDIA GPU": `task.gpu` and `not facts.has_nvidia_gpu`)."""
    if task.gpu and not facts.has_nvidia_gpu:
        return "GPU task on a node without an NVIDIA GPU"
    return None


def allows_spot(tasks: list[TaskDef], spot_max_task_minutes: int) -> bool:
    """False when any task has `spot=False` or a timeout above `spot_max_task_minutes` (spot.md rule 6)."""
    limit = timedelta(minutes=spot_max_task_minutes)
    return all(task.spot and task.timeout <= limit for task in tasks)
