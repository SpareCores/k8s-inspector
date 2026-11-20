"""Publisher against a local bare git repo, fake storage and an in-memory BenchmarkRun store (no cluster, no S3)."""

import json
import shutil
import subprocess
import threading
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from k8s_inspector import crd, publisher
from k8s_inspector import storage as storage_keys
from k8s_inspector.model import (
    BenchmarkRun,
    EnvConfig,
    PublishSettings,
    RunKey,
    RunPhase,
    RunSpec,
    RunStatus,
    Settings,
    TaskDef,
    TaskState,
    TaskStatus,
    VendorSettings,
)

GOLDEN = Path(__file__).parent / "fixtures" / "publish" / "hcloud_cax31" / "0"
DAY = date(2026, 9, 29)
RUN_ID = "r20260929t1257-abcd"
KEY = RunKey("hcloud", "cax31", DAY, RUN_ID)
ENABLED = Settings(vendors={"hcloud": VendorSettings(enabled=True)})

TASKS = {
    "lscpu": TaskDef("lscpu", "img", None, timedelta(minutes=5)),
    "geekbench": TaskDef(
        "geekbench", "img", None, timedelta(minutes=60), transforms=("raw", "fetch_geekbench_results")
    ),
    "openssl": TaskDef("openssl", "img", None, timedelta(minutes=5), parse="openssl"),
}


def git(cwd: Path, *args: str) -> str:
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    out = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True, env=env)
    return out.stdout


@pytest.fixture
def origin(tmp_path) -> Path:
    """Bare repo with `main` holding a README and the golden hcloud/cax31 schedule meta of another task."""
    bare = tmp_path / "origin.git"
    git(tmp_path, "init", "--bare", "-b", "main", str(bare))
    git(bare, "config", "uploadpack.allowFilter", "true")
    seed = tmp_path / "seed"
    git(tmp_path, "clone", str(bare), str(seed))
    (seed / "README.md").write_text("data\n")
    (seed / ".gitignore").write_text("**/resource_tracker.jsonl\n")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "seed")
    git(seed, "push", "origin", "HEAD:main")
    return bare


def clone(origin: Path, tmp_path: Path, name: str = "check") -> Path:
    target = tmp_path / name
    shutil.rmtree(target, ignore_errors=True)
    git(tmp_path, "clone", origin.as_uri(), str(target))
    return target


def okey(run: RunKey, rest: str) -> str:
    return f"logs/inspect/{run.vendor}/{run.sku}/{run.day:%Y/%m/%d}/{run.run_id}/{rest}"


class FakeStorage:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_task(self, key: str, status: dict, stdout: bytes = b"", stderr: bytes = b"", run: RunKey = KEY):
        base = okey(run, f"tasks/{key}")
        self.objects[f"{base}/status.json"] = json.dumps(status).encode()
        if stdout:
            self.objects[f"{base}/stdout"] = stdout
        if stderr:
            self.objects[f"{base}/stderr"] = stderr

    def read_json(self, run, key, name):
        raw = self.objects.get(okey(run, f"tasks/{key}/{name}"))
        return json.loads(raw) if raw else None

    def read_bytes(self, object_key):
        return self.objects.get(object_key)

    def list_status_keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix) and k.endswith("status.json"))


@pytest.fixture(autouse=True)
def fake_key_layout(monkeypatch):
    """Storage key layout stand-in (storage.py is implemented separately): `<vendor>/<sku>/<day>/<run>/...`."""
    monkeypatch.setattr(
        storage_keys,
        "object_key",
        lambda run, key, name: okey(run, f"tasks/{key}/{name}"),
    )
    monkeypatch.setattr(storage_keys, "run_json_key", lambda run: okey(run, "run.json"))


class FakeCluster:
    """crd functions over a dict of BenchmarkRuns."""

    def __init__(self, monkeypatch):
        self.runs: dict[str, BenchmarkRun] = {}
        monkeypatch.setattr(crd, "list_runs", lambda kube: list(self.runs.values()))
        monkeypatch.setattr(
            crd, "run_key", lambda r: RunKey(r.spec.vendor, r.spec.sku, r.created.date(), r.spec.run_id)
        )
        monkeypatch.setattr(crd, "update_status", self.update_status)
        monkeypatch.setattr(crd, "delete_run", lambda kube, name: self.runs.pop(name))

    def update_status(self, kube, name, mutate):
        mutate(self.runs[name].status)
        return self.runs[name]

    def add_run(self, names: list[str], phase=RunPhase.PUBLISHING, vendor="hcloud", sku="cax31") -> BenchmarkRun:
        t0 = datetime(2026, 9, 29, 12, 57, 13, tzinfo=UTC)
        status = RunStatus(
            phase=phase,
            tasks=[
                TaskStatus(n, TaskState.DONE, 1, t0 + timedelta(minutes=i), t0 + timedelta(minutes=i + 1), 0)
                for i, n in enumerate(names)
            ],
        )
        run = BenchmarkRun(f"{vendor}-{sku}", "inspector", "u", "1", t0, RunSpec(vendor, sku, RUN_ID), status)
        self.runs[run.name] = run
        return run


@pytest.fixture
def cluster(monkeypatch):
    return FakeCluster(monkeypatch)


def golden_status(name: str, root: Path = GOLDEN) -> dict:
    meta = json.loads((root / name / "meta.json").read_text())
    meta.pop("outputs")
    return {**meta, "run_id": RUN_ID}


def put_golden(storage: FakeStorage, name: str, run: RunKey = KEY) -> None:
    read = lambda n: (GOLDEN / name / n).read_bytes() if (GOLDEN / name / n).exists() else b""  # noqa: E731
    storage.put_task(name, golden_status(name), read("stdout"), read("stderr"), run)


def make_publisher(tmp_path, origin, storage, **kwargs) -> publisher.Publisher:
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    return publisher.Publisher(None, storage, env, tmp_path / "work", get_task=TASKS.__getitem__, **kwargs)


@pytest.fixture
def storage() -> FakeStorage:
    s = FakeStorage()
    for name in ("lscpu", "geekbench", "openssl"):
        put_golden(s, name)
    s.put_task("vllm-probe-vllm_x", {"exit_code": 0})
    s.objects[okey(KEY, "run.json")] = json.dumps({"timing": {"machine_start": "2026-09-29T12:58:07Z"}}).encode()
    return s


NAMES = ["lscpu", "geekbench", "openssl", "vllm-probe-vllm_x"]


def test_publish_once_writes_the_legacy_tree_one_commit_and_marks_published(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES)
    pub = make_publisher(tmp_path, origin, storage)
    result = pub.publish_once(ENABLED)

    assert (result.tasks_published, result.run_ids) == (4, (RUN_ID,))
    check = clone(origin, tmp_path)
    log = git(check, "log", "--format=%s").splitlines()
    assert log == [f"k8s-inspector: publish 4 results ({RUN_ID})", "seed"]
    assert result.commit == git(check, "rev-parse", "HEAD").strip()

    inst = check / "data/hcloud/cax31"
    assert (inst / "0/variant.json").read_text() == (GOLDEN / "variant.json").read_text().replace(
        '"source": "backfill"', '"source": "observed"'
    )
    for name, files in {
        "lscpu": ["stdout"],
        "geekbench": ["stdout", "stderr", "results.json"],
        "openssl": ["stdout", "parsed.json"],
    }.items():
        for f in files:
            assert (inst / "0" / name / f).read_bytes() == (GOLDEN / name / f).read_bytes()
        meta = json.loads((inst / "0" / name / "meta.json").read_text())
        assert meta["run_id"] == RUN_ID and meta["exit_code"] == 0
        assert (inst / "schedule" / name / "meta.json").read_text() == (inst / "0" / name / "meta.json").read_text()
    assert not (inst / "0/vllm-probe-vllm_x").exists()
    timing = inst / "0/timing" / RUN_ID
    assert (timing / "machine_start").read_text() == "2026-09-29T12:58:07Z\n"
    assert (timing / "api_start").read_text() == "2026-09-29T12:57:13Z\n"
    assert (timing / "inspector_end").read_text() == "2026-09-29T13:01:13Z\n"

    assert all(t.published and t.commit == result.commit for t in run.status.tasks)
    assert run.status.variant == 0 and run.status.phase == RunPhase.DONE
    assert [c["type"] for c in run.status.conditions] == ["Published"]
    assert pub.publish_once(ENABLED) == publisher.PublishResult(0, None, ())


def test_crash_between_push_and_patch_republishes_without_a_new_commit(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES)
    pub = make_publisher(tmp_path, origin, storage)
    first = pub.publish_once(ENABLED)
    for t in run.status.tasks:
        t.published, t.commit = False, None
    run.status.conditions.clear()
    run.status.phase = RunPhase.PUBLISHING
    again = pub.publish_once(ENABLED)
    assert again.commit is None and again.tasks_published == 4
    assert all(t.published and t.commit == first.commit for t in run.status.tasks)
    assert len(git(clone(origin, tmp_path), "log", "--format=%s").splitlines()) == 2


def test_non_fast_forward_starts_again_from_a_fresh_fetch(tmp_path, origin, storage, cluster):
    cluster.add_run(NAMES)
    competing = clone(origin, tmp_path, "competing")

    class Racing(publisher.Publisher):
        raced = False

        def _push(self):
            if not self.raced:
                Racing.raced = True
                (competing / "other.txt").write_text("someone else\n")
                git(competing, "add", "-A")
                git(competing, "commit", "-m", "competing")
                git(competing, "push", "origin", "HEAD:main")
            return super()._push()

    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = Racing(None, storage, env, tmp_path / "work", get_task=TASKS.__getitem__)
    result = pub.publish_once(ENABLED)
    log = git(clone(origin, tmp_path), "log", "--format=%s").splitlines()
    assert log == [f"k8s-inspector: publish 4 results ({RUN_ID})", "competing", "seed"]
    assert result.commit is not None


def test_vendor_must_be_enabled(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES)
    pub = make_publisher(tmp_path, origin, storage)
    for settings in (Settings(), Settings(vendors={"hcloud": VendorSettings(enabled=False)}), None):
        assert pub.publish_once(settings).tasks_published == 0
    assert not any(t.published for t in run.status.tasks)
    assert git(clone(origin, tmp_path), "log", "--format=%s").splitlines() == ["seed"]
    assert not publisher.vendor_allowed(Settings(vendors={"oci": VendorSettings(enabled=True)}), "hcloud")


def test_settings_provider_is_the_default_allowlist(tmp_path, origin, storage, cluster):
    cluster.add_run(NAMES)
    assert (
        make_publisher(tmp_path, origin, storage, settings_provider=lambda: ENABLED).publish_once().tasks_published == 4
    )


def test_no_node_result_goes_to_schedule_only(tmp_path, origin, storage, cluster):
    run = RunKey("hcloud", "cax31", DAY, "r2")
    storage.put_task(
        "geekbench",
        {
            "start": "2026-10-01T10:00:00",
            "end": "2026-10-01T10:01:00",
            "exit_code": -1,
            "error_msg": "SCARPENTER_ICE: x",
            "task_hash": "h",
            "run_id": "r2",
        },
        run=run,
    )
    cluster.add_run(["geekbench"])
    cluster.runs["hcloud-cax31"].spec.run_id = "r2"
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert json.loads((inst / "schedule/geekbench/meta.json").read_text())["exit_code"] == -1
    assert [p.name for p in inst.iterdir()] == ["schedule"]


def test_finished_task_without_lscpu_waits(tmp_path, origin, cluster):
    storage = FakeStorage()
    put_golden(storage, "openssl")
    run = cluster.add_run(["openssl"])
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.tasks_published == 0 and not run.status.tasks[0].published


def test_new_hardware_gets_the_next_variant_and_a_known_one_is_reused(tmp_path, origin, storage, cluster):
    pub = make_publisher(tmp_path, origin, storage)
    cluster.add_run(["lscpu"])
    pub.publish_once(ENABLED)
    # second run on different hardware (Model name changed)
    other = RunKey("hcloud", "cax31", DAY, "r2")
    lscpu = json.loads((GOLDEN / "lscpu/stdout").read_text())
    for row in lscpu["lscpu"]:
        if row["field"] == "Model name:":
            row["data"] = "Neoverse-N2"
    storage.put_task("lscpu", golden_status("lscpu") | {"run_id": "r2"}, json.dumps(lscpu).encode(), run=other)
    run2 = cluster.add_run(["lscpu"])
    run2.name, run2.spec.run_id = "run2", "r2"
    cluster.runs["run2"] = run2
    pub.publish_once(ENABLED)
    assert run2.status.variant == 1
    check = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert json.loads((check / "1/variant.json").read_text())["lscpu"]["Model name:"] == "Neoverse-N2"
    assert (check / "1/timing/r2").is_dir()


def test_task_that_cannot_render_stays_unpublished_others_go_out(tmp_path, origin, storage, cluster):
    storage.objects[okey(KEY, "tasks/openssl/stdout")] = b"\xff\xfe not utf-8"
    run = cluster.add_run(NAMES)
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    done = {t.name: t.published for t in run.status.tasks}
    assert done == {"lscpu": True, "geekbench": True, "openssl": False, "vllm-probe-vllm_x": True}
    assert result.tasks_published == 3


def test_unsafe_names_are_refused(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES, sku="..")
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 0
    assert not any(t.published for t in run.status.tasks)


def test_delete_expired_runs(tmp_path, origin, storage, cluster):
    pub = make_publisher(tmp_path, origin, storage)
    fresh, old, open_run = cluster.add_run(NAMES), cluster.add_run(NAMES, sku="old"), cluster.add_run(NAMES, sku="open")
    for run in (fresh, old, open_run):
        for t in run.status.tasks:
            t.published = True
    now = datetime.now(UTC)
    stamp = lambda dt: [{"type": "Published", "lastTransitionTime": dt.isoformat().replace("+00:00", "Z")}]  # noqa: E731
    fresh.status.conditions = stamp(now - timedelta(hours=1))
    old.status.conditions = stamp(now - timedelta(hours=25))
    open_run.status.conditions = stamp(now - timedelta(hours=99))
    open_run.status.tasks[0].published = False
    assert pub.delete_expired_runs(Settings(publish=PublishSettings(keep_run_hours=24))) == 1
    assert sorted(cluster.runs) == ["hcloud-cax31", "hcloud-open"]


def test_rescan_publishes_what_git_lacks(tmp_path, origin, storage, cluster):
    # git knows the SKU (so the rescan finds it) but not this run
    seed = clone(origin, tmp_path, "seed2")
    (seed / "data/hcloud/cax31/schedule").mkdir(parents=True)
    (seed / "data/hcloud/cax31/schedule/.keep").write_text("")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "sku exists")
    git(seed, "push", "origin", "HEAD:main")
    pub = make_publisher(tmp_path, origin, storage)
    today = datetime.now(UTC).date()
    old = KEY
    recent = RunKey("hcloud", "cax31", today, "r-recent")
    for name in ("lscpu", "openssl"):
        put_golden(storage, name, recent)
    result = pub.rescan(7, settings=ENABLED)
    assert result.run_ids == ("r-recent",) and result.tasks_published == 2  # the 2026-09-29 run is older than 7 days
    check = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert (check / "0/openssl/parsed.json").exists() and (check / "0/timing/r-recent/inspector_start").exists()
    assert old.run_id not in {p.name for p in (check / "0/timing").iterdir()}
    assert pub.rescan(7, settings=ENABLED).commit is None  # now git has it
    assert pub.rescan(7).tasks_published == 0  # no settings: fail closed


def test_run_forever_cycles_on_trigger_survives_errors_and_stops(tmp_path, origin, storage, cluster):
    calls = []
    seen = threading.Event()

    class Counting(publisher.Publisher):
        def publish_once(self, settings=None):
            calls.append(settings)
            seen.set()
            if len(calls) == 1:
                raise RuntimeError("boom")  # logged, never fatal
            return publisher.PublishResult(0, None, ())

    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = Counting(None, storage, env, tmp_path / "work")
    stop = threading.Event()
    settings = Settings(publish=PublishSettings(interval_minutes=60))
    thread = threading.Thread(target=pub.run_forever, args=(lambda: settings, stop), daemon=True)
    thread.start()
    assert seen.wait(5)
    seen.clear()
    pub.trigger()  # the 60-minute wait is cut short
    assert seen.wait(5)
    stop.set()
    thread.join(5)
    assert not thread.is_alive() and len(calls) == 2 and calls[0] is settings


def test_deploy_key_is_passed_to_git_through_ssh_command(tmp_path, origin, storage, monkeypatch):
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["env"] = kwargs["env"]
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    env = EnvConfig(bucket="b", bucket_region="r", git_url="git@example.test:x/y.git", git_key_file="/keys/deploy key")
    publisher.Publisher(None, storage, env, tmp_path / "work")._git("status")
    command = seen["env"]["GIT_SSH_COMMAND"]
    assert "-i '/keys/deploy key'" in command and "IdentitiesOnly=yes" in command
    assert seen["env"]["GIT_TERMINAL_PROMPT"] == "0"
