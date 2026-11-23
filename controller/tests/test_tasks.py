import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from k8s_inspector import tasks
from k8s_inspector.model import NodeFacts, TaskDef

FIXTURES = Path(__file__).parent / "fixtures" / "tasks"
# Hashes of sc-inspector's own `lib.task_hash` for every legacy task (see fixtures/tasks/generate_legacy_hashes.py).
# All of them were also found in real sc-inspector-data meta.json files. `timing` is not a catalogue task.
LEGACY_HASHES = json.loads((FIXTURES / "legacy_hashes.json").read_text())["hashes"]
NOT_PORTED = {"timing"}
NEW_TASKS = {"gpu_facts"}


def node(*, gpu: bool) -> NodeFacts:
    return NodeFacts(
        node_name="n1",
        instance_type="aws.m5.large",
        arch="amd64",
        created=datetime(2026, 1, 1, tzinfo=UTC),
        allocatable_memory_bytes=8 * 1024**3,
        has_nvidia_gpu=gpu,
    )


def test_every_legacy_task_is_ported():
    names = {task.name for task in tasks.all_tasks()}
    assert names == (LEGACY_HASHES.keys() - NOT_PORTED) | NEW_TASKS


@pytest.mark.parametrize("name", sorted(LEGACY_HASHES.keys() - NOT_PORTED))
def test_hash_equals_legacy(name):
    assert tasks.task_hash(tasks.get_task(name)) == LEGACY_HASHES[name]


def test_hash_revision_changes_only_when_positive():
    task = tasks.get_task("redis")
    assert tasks.task_hash(task) == LEGACY_HASHES["redis"]
    bumped = TaskDef(**{**task.__dict__, "revision": 1})
    assert tasks.task_hash(bumped) != LEGACY_HASHES["redis"]


def test_hash_ignores_non_hashed_fields():
    task = tasks.get_task("redis")
    changed = TaskDef(**{**task.__dict__, "digest": "sha256:abc", "timeout": timedelta(hours=9), "priority": 99.0})
    assert tasks.task_hash(changed) == tasks.task_hash(task)


def test_order_is_priority_then_parallel_then_name():
    names = [task.name for task in tasks.all_tasks()]
    assert names[:6] == ["dmidecode", "gpu_facts", "lscpu", "lshw", "lstopo", "nvidia_smi"]
    assert names[6] == "virtualization"
    assert names[7:10] == ["lsblk", "lsblk_discard", "lsblk_topo"]
    assert names[-2:] == ["storage", "stressnglongrun"]
    assert names.index("pgbench_postgres_dbaas_ro_durable") < names.index("stressngfull")


def test_probe_group_is_the_parallel_tasks():
    parallel = {task.name for task in tasks.all_tasks() if task.parallel}
    assert parallel == {"dmidecode", "lshw", "lscpu", "lstopo", "virtualization", "nvidia_smi", "gpu_facts"}


def test_catalogue_facts_ported():
    geekbench, vllm, storage = (tasks.get_task(n) for n in ("geekbench", "vllm", "storage"))
    assert geekbench.memory_cap == 0.85
    assert geekbench.transforms == ("raw", "fetch_geekbench_results")
    assert geekbench.secrets == ("sentinel", "geekbench")
    assert (vllm.memory_cap, vllm.shm_gib, vllm.models_cache) == (0.85, 4, True)
    assert vllm.image == "|".join(vllm.images)
    assert len(vllm.images) == 3
    assert storage.privileges == {"host_pid"}
    assert storage.priority == float("inf")
    assert tasks.get_task("llm").timeout == timedelta(minutes=90)
    assert tasks.get_task("pgbench_postgres_ro_durable").collect == ("/tmp/pg-server.log:postgres.log",)
    assert "huggingface" in tasks.get_task("llm").secrets
    assert not any("huggingface" in task.secrets for task in tasks.all_tasks() if task.name not in ("llm", "vllm"))
    assert {t.name for t in tasks.all_tasks() if t.gpu} == {"nvidia_smi", "gpu_facts", "nvbandwidth"}


def test_only_loopback_tasks_use_the_pod_network():
    assert {t.name for t in tasks.all_tasks() if not t.host_network} == {"redis", "static_web"}


def test_select_tasks():
    everything = [task.name for task in tasks.select_tasks([])]
    assert "pgbench_postgres_dbaas_ro_durable" not in everything
    assert everything[0] == "dmidecode"
    assert [t.name for t in tasks.select_tasks(["storage", "openssl", "lshw"])] == ["lshw", "openssl", "storage"]
    assert [t.name for t in tasks.select_tasks(["pgbench_postgres_dbaas_ro_durable"])] == [
        "pgbench_postgres_dbaas_ro_durable"
    ]
    with pytest.raises(ValueError, match="nope"):
        tasks.select_tasks(["openssl", "nope"])


def test_keeper_gated_tasks_are_selected_only_by_name():
    assert [t.name for t in tasks.all_tasks() if t.keeper_gated] == ["stressnglongrun"]
    assert "stressnglongrun" not in [t.name for t in tasks.select_tasks([])]
    named = tasks.select_tasks(["lscpu", "stressnglongrun"])
    assert [t.name for t in named] == ["lscpu", "stressnglongrun"]


def test_get_task_unknown():
    with pytest.raises(KeyError):
        tasks.get_task("nope")


def test_dbaas_tasks_are_selected_only_by_name():
    assert tasks.get_task("pgbench_postgres_dbaas_ro_durable").dbaas
    assert not tasks.get_task("pgbench_postgres_ro_durable").dbaas
    assert "pgbench_postgres_dbaas_ro_durable" not in [t.name for t in tasks.select_tasks([])]


def test_precheck_gpu_tasks():
    for name in ("nvidia_smi", "nvbandwidth", "gpu_facts"):
        task = tasks.get_task(name)
        assert tasks.precheck(task, node(gpu=False)) == "GPU task on a node without an NVIDIA GPU"
        assert tasks.precheck(task, node(gpu=True)) is None
    assert tasks.precheck(tasks.get_task("vllm"), node(gpu=False)) is None
    assert tasks.precheck(tasks.get_task("openssl"), node(gpu=False)) is None


def test_allows_spot():
    short = [tasks.get_task("openssl"), tasks.get_task("vllm")]  # 1 h, 3 h
    assert tasks.allows_spot(short, 240)
    assert not tasks.allows_spot(short, 120)
    assert not tasks.allows_spot([tasks.get_task("stressnglongrun")], 240)
    assert not tasks.allows_spot([tasks.get_task("stressnglongrun")], 24 * 60)  # spot=False, whatever the limit
    assert tasks.allows_spot([], 240)


def test_image_ref(tmp_path):
    catalogue = tmp_path / "tasks.yaml"
    catalogue.write_text(
        "tasks:\n"
        "  - {name: nodigest, image: 'x/n:main', command: run, timeout: 1m}\n"
        "  - {name: a, image: 'x/a:main', digest: 'sha256:aa', command: run, timeout: 1m}\n"
        "  - name: v\n"
        "    images: ['x/g:main', 'x/c:main']\n"
        "    digests: {'x/g:main': 'sha256:gg', 'x/c:main': 'sha256:cc'}\n"
        "    command: null\n"
        "    timeout: 1h30m\n"
    )
    nodigest, a, v = tasks.load_catalogue(catalogue)
    with pytest.raises(ValueError, match="no digest"):
        tasks.image_ref(nodigest)
    assert tasks.image_ref(a) == "x/a:main@sha256:aa"
    assert tasks.image_ref(a, "x/a:main") == "x/a:main@sha256:aa"
    assert v.image == "x/g:main|x/c:main"
    assert v.timeout == timedelta(minutes=90)
    assert tasks.image_ref(v, "x/c:main") == "x/c:main@sha256:cc"
    with pytest.raises(ValueError):
        tasks.image_ref(v)
    with pytest.raises(ValueError):
        tasks.image_ref(a, "other")


@pytest.mark.parametrize(
    ("yaml_text", "message"),
    [
        ("{name: Bad-Name, image: i, command: c, timeout: 1m}", "bad task name"),
        ("{name: a, image: i, command: c, timeout: 1m, typo: 1}", "unknown keys"),
        ("{name: a, image: i, command: c, timeout: 1x}", "bad duration"),
        ("{name: a, image: i, command: c, timeout: 1m, transforms: [nope]}", "unknown transform"),
        ("{name: a, command: c, timeout: 1m}", "image or images"),
        ("{name: a, image: i, command: c}", "timeout is required"),
    ],
)
def test_load_catalogue_validation(tmp_path, yaml_text, message):
    catalogue = tmp_path / "tasks.yaml"
    catalogue.write_text(f"tasks:\n  - {yaml_text}\n")
    with pytest.raises(ValueError, match=message):
        tasks.load_catalogue(catalogue)


def test_load_catalogue_duplicate(tmp_path):
    catalogue = tmp_path / "tasks.yaml"
    entry = "  - {name: a, image: i, command: c, timeout: 1m}\n"
    catalogue.write_text("tasks:\n" + entry + entry)
    with pytest.raises(ValueError, match="duplicate"):
        tasks.load_catalogue(catalogue)


def test_every_catalogue_image_has_a_digest():
    """`scripts/refresh_digests.py` pins them; a new task needs a run of it."""
    for task in tasks.all_tasks():
        for image in task.images or (task.image,):
            assert tasks.image_ref(task, image if task.images else None).startswith(f"{image}@sha256:")


def test_load_catalogue_rejects_duplicate_yaml_keys(tmp_path):
    catalogue = tmp_path / "tasks.yaml"
    catalogue.write_text(
        "tasks:\n  - name: v\n    images: [a, b]\n    digests:\n      a: sha256:1\n      a: sha256:2\n    timeout: 1m\n"
    )
    with pytest.raises(ValueError, match=r"duplicate YAML keys \['a'\]"):
        tasks.load_catalogue(catalogue)
