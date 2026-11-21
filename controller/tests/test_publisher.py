"""Publisher against a local bare git repo, fake storage and an in-memory BenchmarkRun store (no cluster, no S3)."""

import json
import shutil
import subprocess
import threading
from dataclasses import replace
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
ENABLED = Settings(vendors={"hcloud": VendorSettings(enabled=True, publish=True)})

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

    def put_run_json(self, run: RunKey, lscpu_stdout: str) -> None:
        """`run.json` with the lscpu output of attempt 1 (the attempt of the golden statuses)."""
        attempt = {
            "attempt": 1,
            "api_start": "2026-09-29T12:57:13Z",
            "machine_start": "2026-09-29T12:58:07Z",
            "lscpu_stdout": lscpu_stdout,
        }
        doc = {"attempts": [attempt]}
        self.objects[okey(run, "run.json")] = json.dumps(doc).encode()

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
    return {**meta, "run_id": RUN_ID, "attempt": 1}


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
    s.put_run_json(KEY, (GOLDEN / "lscpu/stdout").read_text())
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
    assert not publisher.vendor_allowed(Settings(vendors={"oci": VendorSettings(enabled=True, publish=True)}), "hcloud")
    assert not publisher.vendor_allowed(Settings(vendors={"hcloud": VendorSettings(enabled=True)}), "hcloud")


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


def _put_openssl_without_lscpu(storage: FakeStorage, ended: datetime) -> None:
    status = golden_status("openssl") | {"end": ended.replace(tzinfo=None).isoformat()}
    storage.put_task("openssl", status, (GOLDEN / "openssl/stdout").read_bytes())


def test_finished_task_without_lscpu_waits(tmp_path, origin, cluster):
    storage = FakeStorage()
    _put_openssl_without_lscpu(storage, datetime.now(UTC))
    run = cluster.add_run(["openssl"])
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.tasks_published == 0 and result.unpublishable == 0 and not run.status.tasks[0].published


def test_finished_task_without_lscpu_is_given_up_after_keep_run_hours(tmp_path, origin, cluster, caplog):
    storage = FakeStorage()
    _put_openssl_without_lscpu(storage, datetime.now(UTC) - timedelta(hours=25))
    run = cluster.add_run(["openssl"])
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    task = run.status.tasks[0]
    assert result == publisher.PublishResult(0, None, (), unpublishable=1)
    assert task.published and task.commit is None
    assert "not publishable" in caplog.text


def test_harness_error_of_a_run_without_a_node_goes_to_schedule_only(tmp_path, origin, cluster):
    storage = FakeStorage()
    status = {"start": "2026-10-01T10:00:00", "end": "2026-10-01T10:01:00", "exit_code": 256, "error_msg": "x",
              "task_hash": "h", "run_id": RUN_ID, "attempt": 1}  # fmt: skip
    storage.put_task("geekbench", status)
    run = cluster.add_run(["geekbench"])
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert json.loads((inst / "schedule/geekbench/meta.json").read_text())["exit_code"] == 256
    assert [p.name for p in inst.iterdir()] == ["schedule"] and run.status.tasks[0].published


def test_harness_error_of_a_run_with_a_node_still_waits_for_lscpu(tmp_path, origin, cluster):
    storage = FakeStorage()
    status = golden_status("openssl") | {"exit_code": 256, "end": datetime.now(UTC).replace(tzinfo=None).isoformat()}
    storage.put_task("openssl", status)
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": [{"attempt": 1, "node": "n1"}]}).encode()
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
    storage.put_run_json(other, json.dumps(lscpu))
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
    assert pub.delete_expired_runs(replace(ENABLED, publish=PublishSettings(keep_run_hours=24))) == 1
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
    storage.put_run_json(recent, (GOLDEN / "lscpu/stdout").read_text())
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


def test_read_schedule_metas_checks_out_only_the_schedule_metas_and_leaves_the_publisher_working(
    tmp_path, origin, storage
):
    seed = clone(origin, tmp_path, "seed2")
    for sku, task in (("a", "lscpu"), ("a", "membench"), ("b", "lscpu")):
        (seed / "data" / "hcloud" / sku / "schedule" / task).mkdir(parents=True)
        (seed / "data" / "hcloud" / sku / "schedule" / task / "meta.json").write_text(json.dumps({"exit_code": 0}))
    (seed / "data" / "hcloud" / "a" / "0" / "lscpu").mkdir(parents=True)
    (seed / "data" / "hcloud" / "a" / "0" / "lscpu" / "stdout").write_text("x")
    (seed / "data" / "other" / "z" / "schedule" / "lscpu").mkdir(parents=True)
    (seed / "data" / "other" / "z" / "schedule" / "lscpu" / "meta.json").write_text("{}")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "more")
    git(seed, "push", "origin", "HEAD:main")
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = publisher.Publisher(None, storage, env, tmp_path / "work")

    metas = pub.read_schedule_metas("hcloud")

    assert metas["a"] == {"lscpu": {"exit_code": 0}, "membench": {"exit_code": 0}}
    assert metas["b"] == {"lscpu": {"exit_code": 0}} and "z" not in metas
    assert not (tmp_path / "work" / "repo" / "data" / "hcloud" / "a" / "0").exists()
    assert pub.read_schedule_metas("nothing") == {}
    pub._sync(["data/hcloud/a"])  # the publisher's cone checkout still works afterwards
    assert (tmp_path / "work" / "repo" / "data" / "hcloud" / "a" / "0" / "lscpu" / "stdout").exists()


def test_read_schedule_metas_marks_a_malformed_meta_as_none(tmp_path, origin, storage, caplog):
    seed = clone(origin, tmp_path, "seed2")
    for sku, text in (("a", "{not json"), ("a2", "[1]"), ("b", json.dumps({"exit_code": 0}))):
        (seed / "data" / "hcloud" / sku / "schedule" / "lscpu").mkdir(parents=True)
        (seed / "data" / "hcloud" / sku / "schedule" / "lscpu" / "meta.json").write_text(text)
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "more")
    git(seed, "push", "origin", "HEAD:main")
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    metas = publisher.Publisher(None, storage, env, tmp_path / "work").read_schedule_metas("hcloud")
    assert metas == {"a": {"lscpu": None}, "a2": {"lscpu": None}, "b": {"lscpu": {"exit_code": 0}}}
    assert "malformed data/hcloud/a/schedule/lscpu/meta.json" in caplog.text


def test_git_command_times_out_into_a_failed_cycle_and_ssh_has_timeouts(tmp_path, storage, monkeypatch):
    seen = {}

    def fake_run(cmd, **kwargs):
        seen.update(kwargs)
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fake_run)
    key = tmp_path / "key"
    key.write_text("key")
    env = EnvConfig(bucket="b", bucket_region="r", git_url="git@example.test:x/y.git", git_key_file=str(key))
    with pytest.raises(RuntimeError, match="git fetch timed out after 600 s"):
        publisher.Publisher(None, storage, env, tmp_path / "work")._git("fetch")
    assert seen["timeout"] == 600
    command = seen["env"]["GIT_SSH_COMMAND"]
    assert "ConnectTimeout=30" in command and "ServerAliveInterval=15" in command and "ServerAliveCountMax=4" in command


def _stalled_cluster(cluster, minutes_ago: int, vendor="hcloud"):
    run = cluster.add_run(["lscpu"], vendor=vendor)
    ended = datetime.now(UTC) - timedelta(minutes=minutes_ago)
    run.status.tasks[0].end = ended
    return run


def test_publishing_stalled_after_two_publish_intervals(cluster):
    runs = list(cluster.runs.values())
    assert not publisher.publishing_stalled(ENABLED, "hcloud", runs)
    _stalled_cluster(cluster, 9)
    assert not publisher.publishing_stalled(ENABLED, "hcloud", list(cluster.runs.values()))  # 2 x 5 min = 10
    cluster.runs.clear()
    run = _stalled_cluster(cluster, 11)
    runs = list(cluster.runs.values())
    assert publisher.publishing_stalled(ENABLED, "hcloud", runs)
    assert not publisher.publishing_stalled(ENABLED, "oci", runs)
    run.status.tasks[0].published = True
    assert not publisher.publishing_stalled(ENABLED, "hcloud", runs)
    run.status.tasks[0].published = False
    run.status.phase = RunPhase.RUNNING
    assert not publisher.publishing_stalled(ENABLED, "hcloud", runs)


def test_broken_or_half_cloned_checkout_is_cloned_again(tmp_path, origin, storage):
    pub = make_publisher(tmp_path, origin, storage)
    repo = tmp_path / "work" / "repo"
    pub._sync([])
    (repo / ".git" / "HEAD").write_text("ref: refs/heads/nowhere\n")  # HEAD cannot be resolved
    (repo / "stale").write_text("x")
    pub._sync([])
    assert git(repo, "rev-parse", "HEAD").strip() and not (repo / "stale").exists()
    shutil.rmtree(repo / ".git" / "objects")  # not a repository any more
    pub._sync([])
    assert git(repo, "rev-parse", "HEAD").strip()


def test_failed_clone_leaves_no_partial_checkout(tmp_path, storage):
    env = EnvConfig(bucket="b", bucket_region="r", git_url=(tmp_path / "missing.git").as_uri())
    pub = publisher.Publisher(None, storage, env, tmp_path / "work")
    with pytest.raises(subprocess.CalledProcessError):
        pub._sync([])
    assert not (tmp_path / "work" / "repo").exists()


def test_deploy_key_is_passed_to_git_through_ssh_command(tmp_path, origin, storage, monkeypatch):
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["env"] = kwargs["env"]
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    key = tmp_path / "deploy key"
    key.write_text("key")
    env = EnvConfig(bucket="b", bucket_region="r", git_url="git@example.test:x/y.git", git_key_file=str(key))
    publisher.Publisher(None, storage, env, tmp_path / "work")._git("status")
    command = seen["env"]["GIT_SSH_COMMAND"]
    copy = tmp_path / "work" / "deploy-key"
    assert f"-i {copy}" in command and "IdentitiesOnly=yes" in command
    assert "StrictHostKeyChecking=yes" in command and str(publisher.KNOWN_HOSTS) in command
    hosts = publisher.KNOWN_HOSTS.read_text().splitlines()
    assert len([line for line in hosts if line.startswith("github.com ")]) == 3
    assert seen["env"]["GIT_TERMINAL_PROMPT"] == "0"


def test_deploy_key_is_a_private_copy_refreshed_on_rotation(tmp_path, storage, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kwargs: subprocess.CompletedProcess(cmd, 0, "", ""))
    key = tmp_path / "secret-key"
    key.write_text("one")
    key.chmod(0o440)
    env = EnvConfig(bucket="b", bucket_region="r", git_url="git@example.test:x/y.git", git_key_file=str(key))
    pub = publisher.Publisher(None, storage, env, tmp_path / "work")
    pub._git("status")
    copy = tmp_path / "work" / "deploy-key"
    assert copy.read_text() == "one" and copy.stat().st_mode & 0o777 == 0o600
    key.chmod(0o600)
    key.write_text("two")
    pub._git("status")
    assert copy.read_text() == "two" and copy.stat().st_mode & 0o777 == 0o600


def test_absent_deploy_key_is_not_passed_and_ssh_url_fails_clearly(tmp_path, storage, monkeypatch):
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["env"] = kwargs["env"]
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
    env = EnvConfig(
        bucket="b", bucket_region="r", git_url="git@example.test:x/y.git", git_key_file=str(tmp_path / "absent")
    )
    pub = publisher.Publisher(None, storage, env, tmp_path / "work")
    pub._git("status")
    assert "GIT_SSH_COMMAND" not in seen["env"]
    with pytest.raises(RuntimeError, match="SSH URL but there is no deploy key"):
        pub._sync(["data/x"])


# --------------------------------------------------------------------------- review fixes


def test_publish_allowlist_per_sku():
    only = Settings(vendors={"hcloud": VendorSettings(enabled=True, publish=("cax31",))})
    assert publisher.publish_allowed(only, "hcloud", "cax31") and not publisher.publish_allowed(only, "hcloud", "cx22")
    assert publisher.publish_allowed(ENABLED, "hcloud", "anything")
    assert not publisher.publish_allowed(
        Settings(vendors={"hcloud": VendorSettings(enabled=False, publish=True)}), "hcloud", "cax31"
    )


def test_listed_sku_publishes_and_others_wait(tmp_path, origin, storage, cluster):
    only = Settings(vendors={"hcloud": VendorSettings(enabled=True, publish=("cax31",))})
    run = cluster.add_run(NAMES)
    other = cluster.add_run(NAMES, sku="cx22")
    assert make_publisher(tmp_path, origin, storage).publish_once(only).tasks_published == 4
    assert all(t.published for t in run.status.tasks) and not any(t.published for t in other.status.tasks)


def test_finished_runs_that_will_never_be_published_are_deleted_to_free_the_lease(tmp_path, origin, storage, cluster):
    pub = make_publisher(tmp_path, origin, storage)
    only = replace(ENABLED, vendors={"hcloud": VendorSettings(enabled=True, publish=("cax31",))}, from_configmap=True)
    never = cluster.add_run(NAMES, sku="cx22")
    keep = cluster.add_run(NAMES)  # publishable, not published yet
    running = cluster.add_run(NAMES, sku="cx32", phase=RunPhase.RUNNING)
    assert pub.delete_expired_runs(only) == 1
    assert never.name not in cluster.runs and keep.name in cluster.runs and running.name in cluster.runs


def test_without_the_configmap_unpublished_runs_are_not_deleted(tmp_path, origin, storage, cluster):
    pub = make_publisher(tmp_path, origin, storage)
    unpublished = cluster.add_run(NAMES)
    assert pub.delete_expired_runs(Settings()) == 0  # defaults: nothing is allowed, which is not a decision
    assert unpublished.name in cluster.runs


def test_mark_ignores_a_task_recorded_again_after_it_was_read(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES)
    pub = make_publisher(tmp_path, origin, storage)
    original = pub._publish

    def racing(items, settings):
        task = run.status.tasks[1]  # the runner records geekbench again while the publisher renders
        task.end, task.attempt = task.end + timedelta(minutes=5), 2
        return original(items, settings)

    pub._publish = racing
    pub.publish_once(ENABLED)
    assert {t.name: t.published for t in run.status.tasks}["geekbench"] is False
    assert run.status.tasks[0].published


def _two_attempt_run_json(lscpu_a: str, lscpu_b: str) -> bytes:
    return json.dumps(
        {
            "timing": {"api_start": "2026-09-29T13:30:00Z"},
            "attempts": [
                {"attempt": 1, "api_start": "2026-09-29T12:50:00Z", "machine_start": "2026-09-29T12:58:07Z",
                 "lscpu_stdout": lscpu_a},
                {"attempt": 2, "api_start": "2026-09-29T13:30:00Z", "machine_start": "2026-09-29T13:31:00Z",
                 "lscpu_stdout": lscpu_b},
            ],
        }
    ).encode()  # fmt: skip


def test_variant_comes_from_the_attempt_that_ran_the_task_and_timing_from_the_latest(
    tmp_path, origin, storage, cluster
):
    first = (GOLDEN / "lscpu/stdout").read_text()
    other = json.loads(first)
    for row in other["lscpu"]:
        if row["field"] == "Model name:":
            row["data"] = "Neoverse-N2"
    storage.objects[okey(KEY, "run.json")] = _two_attempt_run_json(first, json.dumps(other))
    storage.objects[okey(KEY, "tasks/lscpu/stdout")] = json.dumps(other).encode()  # overwritten by attempt 2
    for name, attempt in (("lscpu", 2), ("openssl", 1)):
        status = json.loads(storage.objects[okey(KEY, f"tasks/{name}/status.json")]) | {"attempt": attempt}
        storage.objects[okey(KEY, f"tasks/{name}/status.json")] = json.dumps(status).encode()
    cluster.add_run(["lscpu", "openssl"])
    for t in cluster.runs["hcloud-cax31"].status.tasks:
        t.attempt = 2 if t.name == "lscpu" else 1
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert (inst / "0/lscpu").is_dir() and (inst / "1/openssl").is_dir()  # openssl ran on attempt 1 hardware
    timing = inst / "0/timing" / RUN_ID  # the latest attempt's hardware
    assert (timing / "machine_start").read_text() == "2026-09-29T13:31:00Z\n"
    assert (timing / "api_start").read_text() == "2026-09-29T13:30:00Z\n"


def test_multi_attempt_run_without_per_attempt_lscpu_does_not_guess_a_variant(tmp_path, origin, storage, cluster):
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": [{"attempt": 1}, {"attempt": 2}]}).encode()
    _put_openssl_without_lscpu(storage, datetime.now(UTC))
    run = cluster.add_run(["openssl"])
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 0
    assert not run.status.tasks[0].published


def test_failing_task_is_published_without_parser_output_after_repeated_failures(
    tmp_path, origin, storage, cluster, monkeypatch
):
    monkeypatch.setattr(publisher, "MAX_RENDER_FAILURES", 2)
    storage.objects[okey(KEY, "tasks/openssl/stdout")] = b"\xff\xfe not utf-8"
    run = cluster.add_run(NAMES)
    pub = make_publisher(tmp_path, origin, storage)
    for _ in range(2):
        pub.publish_once(ENABLED)
        assert not {t.name: t.published for t in run.status.tasks}["openssl"]
    pub.publish_once(ENABLED)
    assert all(t.published for t in run.status.tasks)
    inst = clone(origin, tmp_path) / "data/hcloud/cax31/0/openssl"
    assert (inst / "meta.json").exists() and not (inst / "parsed.json").exists()


def test_variant_json_is_not_left_behind_by_a_task_that_fails_to_render(tmp_path, origin, storage, cluster):
    storage.objects[okey(KEY, "tasks/openssl/stdout")] = b"\xff\xfe not utf-8"
    cluster.add_run(["openssl"])
    storage.objects[okey(KEY, "tasks/lscpu/status.json")] = storage.objects[okey(KEY, "tasks/lscpu/status.json")]
    pub = make_publisher(tmp_path, origin, storage)
    pub.publish_once(ENABLED)
    assert git(clone(origin, tmp_path), "log", "--format=%s").splitlines() == ["seed"]


def test_forced_run_of_an_unknown_sku_without_a_node_publishes_nothing(tmp_path, origin, cluster):
    storage = FakeStorage()
    run_key = RunKey("hcloud", "typo", DAY, RUN_ID)
    status = {"start": "2026-10-01T10:00:00", "end": "2026-10-01T10:01:00", "exit_code": -1, "task_hash": "h",
              "run_id": RUN_ID}  # fmt: skip
    storage.put_task("geekbench", status, run=run_key)
    run = cluster.add_run(["geekbench"], sku="typo")
    run.spec.force = True
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.tasks_published == 0 and run.status.tasks[0].published and run.status.tasks[0].commit is None
    assert not (clone(origin, tmp_path) / "data").exists()


def test_rescan_routes_dbaas_runs_by_run_json(tmp_path, origin, cluster):
    storage = FakeStorage()
    seed = clone(origin, tmp_path, "seed3")
    (seed / "data/hcloud/cax31").mkdir(parents=True)
    (seed / "data/hcloud/cax31/.keep").write_text("")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "sku exists")
    git(seed, "push", "origin", "HEAD:main")
    today = datetime.now(UTC).date()
    run = RunKey("hcloud", "cax31", today, "r-db")
    database = {"vendor": "hcloud", "id": "db1", "engineVersion": 16}
    storage.put_task("pgbench", {"start": "2026-10-01T10:00:00", "end": "2026-10-01T10:05:00", "exit_code": 0,
                                 "task_hash": "h", "run_id": "r-db"}, b"tps", run=run)  # fmt: skip
    storage.objects[okey(run, "run.json")] = json.dumps({"database": database}).encode()
    tasks = {**TASKS, "pgbench": TaskDef("pgbench", "img", None, timedelta(minutes=5))}
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = publisher.Publisher(None, storage, env, tmp_path / "work", get_task=tasks.__getitem__)
    assert pub.rescan(1, settings=ENABLED).tasks_published == 1
    assert (clone(origin, tmp_path) / "dbaas/hcloud/db1/postgres/16/standalone/pgbench/meta.json").exists()


def test_old_failed_runs_of_temporary_launch_reasons_are_deleted_unpublished(tmp_path, origin, storage, cluster):
    def failed(sku, error_msg, code=-1):
        run = cluster.add_run(["geekbench"], phase=RunPhase.FAILED, sku=sku)
        run.status.tasks[0].exit_code, run.status.tasks[0].error_msg = code, error_msg
        return run

    failed("ice", "SCARPENTER_ICE: x")
    failed("legacy", "SCARPENTER_LAUNCH_FAILED: y")
    failed("rejected", "SCARPENTER_LAUNCH_REJECTED: Fatal (X): z")  # permanent: published as before
    failed("other", "NODE_LOST", code=-4)
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert sorted(cluster.runs) == ["hcloud-other", "hcloud-rejected"]
