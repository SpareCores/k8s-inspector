"""The task catalogue, the task hash and task selection.

Owner: tasks agent. Implements docs/reference/tasks.md. The catalogue is data in `catalogue/tasks.yaml`
(next to this file, shipped with the package), ported from sc-inspector `inspector/tasks.py` and
`lib.py`; this module loads it into `model.TaskDef`s.

The hash must equal sc-inspector's `lib.task_hash` byte for byte (golden test against sc-inspector-data
`schedule/` metas): SHA-1 over the sorted keys of {command, transform_output, image, workload_proxy,
durability} that the task has, each key name (ascii) followed by `str(value)`; `transform_output` is the
concatenation of the legacy `inspect.getsource` text of each transform function. A key counts as "had" when
the legacy task object has the attribute: `command` and `image` always (None command hashes as "None"),
`workload_proxy` and `durability` only when not None.
With `revision > 0`, `revision` joins the key set before sorting (docs say "a sixth key" without an order; no
existing hash is affected, so this is a free choice).
"""

from pathlib import Path

from k8s_inspector.model import NodeFacts, TaskDef

# Verbatim legacy source of each transform function (`inspect.getsource` text of sc-inspector
# inspector/transform.py `raw` and `fetch_geekbench_results`, including the trailing newline). Part of the
# hash, so it must never be reformatted. Keys are the names used in TaskDef.transforms.
TRANSFORM_SOURCES: dict[str, str] = {}

CATALOGUE_PATH = Path(__file__).parent / "catalogue" / "tasks.yaml"


def load_catalogue(path: Path = CATALOGUE_PATH) -> list[TaskDef]:
    """Parse the YAML into TaskDefs (durations like `40m`/`26h`, `digest` optional), validating names
    (lowercase `[a-z0-9_]`) and that every transform name is in `TRANSFORM_SOURCES`."""
    raise NotImplementedError


def all_tasks() -> list[TaskDef]:
    """Every runnable task of the catalogue, ordered by (priority, not parallel, name). Cached. The legacy
    `timing` task is not in the catalogue: timing files are publisher output."""
    raise NotImplementedError


def get_task(name: str) -> TaskDef:
    """Raise `KeyError` for an unknown name."""
    raise NotImplementedError


def task_hash(task: TaskDef) -> str:
    """Legacy-compatible hash, see the module docstring."""
    raise NotImplementedError


def image_ref(task: TaskDef, image: str | None = None) -> str:
    """Pod image reference: `<image>@<digest>` (digest of the task); `image` selects a rung of the vLLM
    ladder, default `task.image`. Raise `ValueError` when the task has no digest."""
    raise NotImplementedError


def select_tasks(names: list[str]) -> list[TaskDef]:
    """The tasks of a run: `names` (empty = every task of the catalogue), ordered like `all_tasks()`.
    Raise `ValueError` on unknown names. Eligibility (Keeper block reasons, due-ness) is the planner's job."""
    raise NotImplementedError


def precheck(task: TaskDef, facts: NodeFacts) -> str | None:
    """None when the task may run on the node, else the reason text for exit code -3 (for example
    "GPU task on a node without an NVIDIA GPU": `task.gpu` and `not facts.has_nvidia_gpu`)."""
    raise NotImplementedError


def allows_spot(tasks: list[TaskDef], spot_max_task_minutes: int) -> bool:
    """False when any task has `spot=False` or a timeout above `spot_max_task_minutes` (spot.md rule 6)."""
    raise NotImplementedError
