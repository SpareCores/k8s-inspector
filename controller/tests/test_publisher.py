"""Publisher against a local bare git repo, fake storage and an in-memory BenchmarkRun store (no cluster, no S3)."""

import json
import shutil
import subprocess
import threading
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from k8s_inspector import crd, keeper, launch, publisher
from k8s_inspector import storage as storage_keys
from k8s_inspector.kube import NotFound
from k8s_inspector.model import (
    BenchmarkRun,
    DbPhase,
    DbStatus,
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
        doc = {"vendor": run.vendor, "sku": run.sku, "run_id": run.run_id, "attempts": [attempt]}
        self.objects[okey(run, "run.json")] = json.dumps(doc).encode()

    def read_json(self, run, key, name):
        raw = self.objects.get(okey(run, f"tasks/{key}/{name}"))
        return json.loads(raw) if raw else None

    def read_bytes(self, object_key):
        return self.objects.get(object_key)

    def list_runs(self, vendor, since):
        keys = (k.split("/") for k in self.objects if k.endswith("/run.json"))
        found = [RunKey(p[2], p[3], date(int(p[4]), int(p[5]), int(p[6])), p[7]) for p in keys if p[2] == vendor]
        return [r for r in found if r.day >= since]

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
        self.deferred: list[tuple[str, str, str]] = []  # (vendor, sku, reason) of `launch.record_failure`
        monkeypatch.setattr(
            launch,
            "record_failure",
            lambda kube, vendor, sku, reason, now, until=None: self.deferred.append((vendor, sku, reason)) or 1,
        )
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
    assert log == [f"k8s-inspector: run records ({RUN_ID})", f"k8s-inspector: publish 4 results ({RUN_ID})", "seed"]
    assert result.commit == git(check, "rev-parse", "HEAD~1").strip()

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
    head = git(clone(origin, tmp_path), "rev-parse", "HEAD").strip()  # no new diff: marked with the tree's HEAD
    assert first.commit and all(t.published and t.commit == head for t in run.status.tasks)
    assert len(git(clone(origin, tmp_path), "log", "--format=%s").splitlines()) == 3  # seed, results, run record


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
    assert log[1:] == [f"k8s-inspector: publish 4 results ({RUN_ID})", "competing", "seed"]
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
    database = {"vendor": "hcloud", "id": "db1", "engineVersion": 16, "region": "r", "vcpus": 2, "memoryGib": 8.0}
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


def _rejected_run(cluster, storage, *, lscpu_state=TaskState.DONE):
    """The live run vultr-vc2-1c-0.5gb: every step Pod rejected by kubelet admission (lscpu included, exit code 256
    with an error message) on a node the run did get."""
    message = "Pod was rejected: Node didn't have enough resource: memory, requested: 335544320"
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)  # recent: the keepRunHours give-up does not apply
    start = (now - timedelta(minutes=1)).isoformat()
    status = {"start": start, "end": now.isoformat(), "exit_code": 256, "error_msg": message, "task_hash": "h",
              "run_id": RUN_ID, "attempt": 1}  # fmt: skip
    names = ["lscpu", "geekbench", "openssl"]
    for name in names:
        storage.put_task(name, status)
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": [{"attempt": 1, "node": "bench-nh4mb"}]}).encode()
    run = cluster.add_run(names)
    for task in run.status.tasks:
        task.exit_code, task.error_msg = 256, message
    run.status.tasks[0].state = lscpu_state
    return run


def _published_metas(origin, tmp_path) -> dict[str, int]:
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    return {
        str(p.relative_to(inst)): json.loads(p.read_text()).get("exit_code") for p in sorted(inst.glob("*/*/meta.json"))
    }


def test_run_whose_lscpu_failed_files_its_failures_in_the_schedule_and_finishes(tmp_path, origin, cluster, caplog):
    storage = FakeStorage()
    run = _rejected_run(cluster, storage)
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.tasks_published == 3 and all(t.published for t in run.status.tasks)
    assert run.status.phase == RunPhase.DONE and run.status.variant is None
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert [p.name for p in inst.iterdir()] == ["schedule"]  # no variant directory
    metas = {
        t: json.loads((inst / "schedule" / t / "meta.json").read_text()) for t in ("lscpu", "geekbench", "openssl")
    }
    assert {m["exit_code"] for m in metas.values()} == {256}
    assert metas["openssl"]["error_msg"].startswith("Pod was rejected")  # what autoPlan's due/backoff logic reads
    assert [r.levelname for r in caplog.records if "its lscpu task failed" in r.message] == ["WARNING"]
    assert cluster.deferred == [("hcloud", "cax31", "NO_LSCPU")]  # the SKU waits before the planner starts it again
    assert not publisher.publishing_stalled(ENABLED, "hcloud", list(cluster.runs.values()))


def test_success_of_a_run_whose_lscpu_failed_is_not_filed_under_a_guessed_variant(tmp_path, origin, cluster):
    seed = clone(origin, tmp_path, "seed2")
    (seed / "data/hcloud/cax31/2/openssl").mkdir(parents=True)
    (seed / "data/hcloud/cax31/2/openssl/meta.json").write_text('{"exit_code": 0}')
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "variant 2")
    git(seed, "push", "origin", "HEAD:main")
    storage = FakeStorage()
    run = _rejected_run(cluster, storage)
    storage.put_task("openssl", golden_status("openssl") | {"end": datetime.now(UTC).replace(tzinfo=None).isoformat()},
                     (GOLDEN / "openssl/stdout").read_bytes())  # fmt: skip
    run.status.tasks[2].exit_code = 0
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.unpublishable == 1 and result.tasks_published == 2
    assert all(t.published for t in run.status.tasks) and run.status.phase == RunPhase.DONE
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert json.loads((inst / "2/openssl/meta.json").read_text()) == {"exit_code": 0}  # untouched
    assert sorted(p.name for p in inst.iterdir()) == ["2", "schedule"]
    assert not (inst / "schedule/openssl").exists()
    assert cluster.deferred == [("hcloud", "cax31", "NO_LSCPU")]
    assert not publisher.publishing_stalled(ENABLED, "hcloud", list(cluster.runs.values()))


def test_lscpu_that_succeeded_on_attempt_2_does_not_fall_back(tmp_path, origin, storage, cluster):
    # attempt 1: lscpu and openssl failed; attempt 2 retries lscpu, which is still running
    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    failed = golden_status("openssl") | {"exit_code": 256, "error_msg": "rejected", "end": now}
    storage.put_task("openssl", failed)
    storage.put_task("lscpu", failed)
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": [{"attempt": 1, "node": "n1"}]}).encode()
    run = cluster.add_run(["lscpu", "openssl"])
    run.status.tasks[0].exit_code, run.status.tasks[0].state = 256, TaskState.RUNNING
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 0
    # attempt 2: lscpu succeeded, and run.json holds its output
    lscpu_out = (GOLDEN / "lscpu/stdout").read_text()
    attempts = [{"attempt": 1, "node": "n1"}, {"attempt": 2, "node": "n2", "lscpu_stdout": lscpu_out}]
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": attempts}).encode()
    for name in ("lscpu", "openssl"):
        storage.put_task(name, golden_status(name) | {"attempt": 2})
    for task in run.status.tasks:
        task.exit_code, task.state, task.attempt = 0, TaskState.DONE, 2
    assert not publisher._no_lscpu_coming(run)
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert (clone(origin, tmp_path) / "data/hcloud/cax31/0/openssl/meta.json").exists()
    assert cluster.deferred == []


def test_tasks_of_a_good_attempt_still_go_to_their_variant_when_lscpu_failed_on_attempt_2(
    tmp_path, origin, storage, cluster
):
    lscpu_out = (GOLDEN / "lscpu/stdout").read_text()
    attempts = [{"attempt": 1, "node": "n1", "lscpu_stdout": lscpu_out}, {"attempt": 2, "node": "n2"}]
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": attempts}).encode()
    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    failed = {"exit_code": 256, "error_msg": "rejected", "attempt": 2, "end": now}
    storage.put_task("lscpu", golden_status("lscpu") | failed)
    storage.put_task("geekbench", golden_status("geekbench") | failed)
    storage.put_task("openssl", golden_status("openssl") | {"end": now}, (GOLDEN / "openssl/stdout").read_bytes())
    run = cluster.add_run(["lscpu", "geekbench", "openssl"])
    for task in run.status.tasks[:2]:
        task.exit_code, task.attempt = 256, 2
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert all(t.published for t in run.status.tasks) and result.unpublishable == 0
    inst = clone(origin, tmp_path) / "data/hcloud/cax31"
    assert (inst / "0/openssl/meta.json").exists()  # attempt 1 had an lscpu result: its variant
    assert not (inst / "0/geekbench").exists() and not (inst / "0/lscpu").exists()
    assert json.loads((inst / "schedule/geekbench/meta.json").read_text())["exit_code"] == 256


def test_forced_run_of_an_unknown_sku_whose_lscpu_failed_publishes_nothing(tmp_path, origin, cluster):
    storage = FakeStorage()
    run = _rejected_run(cluster, storage)
    run.spec.force = True  # data/hcloud/cax31 is not in git
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert all(t.published and t.commit is None for t in run.status.tasks)
    assert not (clone(origin, tmp_path) / "data").exists()


def test_forced_run_whose_lscpu_failed_records_no_launch_deferral(tmp_path, origin, cluster):
    storage = FakeStorage()
    run = _rejected_run(cluster, storage)
    run.spec.force = True  # the operator's run
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert all(t.published for t in run.status.tasks) and cluster.deferred == []


def test_launch_deferral_waits_until_the_whole_run_is_published(tmp_path, origin, cluster):
    storage = FakeStorage()
    run = _rejected_run(cluster, storage)
    del storage.objects[okey(KEY, "tasks/geekbench/status.json")]  # not readable yet: stays unpublished
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert [t.published for t in run.status.tasks] == [True, False, True] and cluster.deferred == []
    storage.put_task("geekbench", json.loads(storage.objects[okey(KEY, "tasks/openssl/status.json")]))
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert all(t.published for t in run.status.tasks) and cluster.deferred == [("hcloud", "cax31", "NO_LSCPU")]
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert cluster.deferred == [("hcloud", "cax31", "NO_LSCPU")]  # not again


def test_failing_finalise_of_a_run_does_not_abort_the_cycle(tmp_path, origin, storage, cluster, monkeypatch):
    done = cluster.add_run(["openssl"], phase=RunPhase.PUBLISHING)
    done.status.tasks[0].published = True
    done.name = "gone"
    cluster.runs["gone"] = done
    real = crd.update_status

    def update_status(kube, name, mutate):
        if name == "gone":
            raise NotFound(name)
        return real(kube, name, mutate)

    monkeypatch.setattr(crd, "update_status", update_status)
    cluster.add_run(NAMES, vendor="hcloud", sku="cax31")  # another run still has tasks to publish
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 4


def test_run_whose_tasks_were_all_published_while_releasing_ends_done(tmp_path, origin, cluster):
    run = cluster.add_run(["openssl"], phase=RunPhase.PUBLISHING)
    run.status.tasks[0].published = True
    make_publisher(tmp_path, origin, FakeStorage()).publish_once(ENABLED)
    assert run.status.phase == RunPhase.DONE
    assert [c["type"] for c in run.status.conditions] == ["Published"]


def test_run_waits_while_lscpu_or_another_task_is_still_to_end(tmp_path, origin, cluster):
    storage = FakeStorage()
    run = _rejected_run(cluster, storage, lscpu_state=TaskState.RUNNING)  # lscpu may still produce a result
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 0
    run.status.tasks[0].state = TaskState.DONE
    run.status.tasks[2].state = TaskState.RUNNING  # lscpu ended, the run is not done
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 0
    assert not any(t.published for t in run.status.tasks)


def test_run_whose_lscpu_succeeded_never_falls_back(tmp_path, origin, storage, cluster):
    # lscpu succeeded but the task's attempt has no lscpu output in run.json: the hardware is unknown, wait
    storage.objects[okey(KEY, "run.json")] = json.dumps({"attempts": [{"attempt": 1, "node": "n"}]}).encode()
    for name in ("lscpu", "openssl"):
        storage.put_task(name, golden_status(name) | {"end": datetime.now(UTC).replace(tzinfo=None).isoformat()})
    run = cluster.add_run(["lscpu", "openssl"])
    assert make_publisher(tmp_path, origin, storage).publish_once(ENABLED).tasks_published == 0
    assert not any(t.published for t in run.status.tasks)


def _cancelled_run(cluster, storage, reason="CANCELLED"):
    """A cancelled run: lscpu and openssl finished, geekbench was unfinished (`-4 reason`)."""
    storage.put_task("geekbench", golden_status("geekbench") | {"exit_code": -4, "error_msg": reason})
    run = cluster.add_run(["lscpu", "geekbench", "openssl"], phase=RunPhase.CANCELLED)
    run.status.tasks[1].exit_code, run.status.tasks[1].error_msg = -4, reason
    return run


def test_cancelled_task_writes_nothing_and_the_run_finishes(tmp_path, origin, storage, cluster):
    run = _cancelled_run(cluster, storage)
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.unpublishable == 0 and result.tasks_published == 2
    assert all(t.published for t in run.status.tasks) and run.status.phase == RunPhase.CANCELLED
    assert not (clone(origin, tmp_path) / "data/hcloud/cax31/schedule/geekbench").exists()
    assert not publisher.publishing_stalled(ENABLED, "hcloud", list(cluster.runs.values()))


def test_cancelled_task_keeps_the_previous_schedule_meta(tmp_path, origin, storage, cluster):
    seed = clone(origin, tmp_path, "seed2")
    (seed / "data/hcloud/cax31/schedule/geekbench").mkdir(parents=True)
    (seed / "data/hcloud/cax31/schedule/geekbench/meta.json").write_text('{"exit_code": 0}')
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "previous")
    git(seed, "push", "origin", "HEAD:main")
    _cancelled_run(cluster, storage)
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    meta = clone(origin, tmp_path) / "data/hcloud/cax31/schedule/geekbench/meta.json"
    assert json.loads(meta.read_text()) == {"exit_code": 0}


def test_finished_tasks_of_a_cancelled_run_publish_normally(tmp_path, origin, storage, cluster):
    storage.put_task("openssl", golden_status("openssl") | {"exit_code": 1, "error_msg": "boom"})
    run = _cancelled_run(cluster, storage)
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert all(t.published for t in run.status.tasks)
    metas = _published_metas(origin, tmp_path)
    assert metas["0/lscpu/meta.json"] == 0 and metas["0/openssl/meta.json"] == 1
    assert "schedule/geekbench/meta.json" not in metas


def test_other_never_finished_reasons_still_publish(tmp_path, origin, storage, cluster):
    run = _cancelled_run(cluster, storage, reason="EVICTED")
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.unpublishable == 0 and all(t.published for t in run.status.tasks)
    assert _published_metas(origin, tmp_path)["schedule/geekbench/meta.json"] == -4


def test_cancelled_task_of_a_run_that_was_not_cancelled_still_publishes(tmp_path, origin, storage, cluster):
    """A SIGTERM of a step Pod is also `-4 CANCELLED` (pods.py): it counts as a start retry."""
    run = _cancelled_run(cluster, storage)
    run.status.phase = RunPhase.PUBLISHING
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.unpublishable == 0 and all(t.published for t in run.status.tasks)
    meta = json.loads((clone(origin, tmp_path) / "data/hcloud/cax31/schedule/geekbench/meta.json").read_text())
    assert (meta["exit_code"], meta["error_msg"], meta["start_retries"]) == (-4, "CANCELLED", 1)


def test_run_cancelled_before_lscpu_finished_does_not_defer_the_sku(tmp_path, origin, cluster):
    storage = FakeStorage()
    for name in ("lscpu", "geekbench", "openssl"):
        storage.put_task(name, golden_status(name) | {"exit_code": -4, "error_msg": "CANCELLED"})
    run = cluster.add_run(["lscpu", "geekbench", "openssl"], phase=RunPhase.CANCELLED)
    for task in run.status.tasks:
        task.exit_code, task.error_msg = -4, "CANCELLED"
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.tasks_published == 0 and result.unpublishable == 0 and all(t.published for t in run.status.tasks)
    assert cluster.deferred == []
    assert not (clone(origin, tmp_path) / "data").exists()


def test_success_without_lscpu_in_a_cancelled_run_is_discarded_at_once(tmp_path, origin, cluster):
    storage = FakeStorage()
    storage.put_task("lscpu", golden_status("lscpu") | {"exit_code": -4, "error_msg": "CANCELLED"})
    now = datetime.now(UTC).replace(tzinfo=None).isoformat()
    storage.put_task("openssl", golden_status("openssl") | {"end": now}, (GOLDEN / "openssl/stdout").read_bytes())
    run = cluster.add_run(["lscpu", "openssl"], phase=RunPhase.CANCELLED)
    run.status.tasks[0].exit_code, run.status.tasks[0].error_msg = -4, "CANCELLED"
    result = make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert result.tasks_published == 0 and result.unpublishable == 0
    assert all(t.published for t in run.status.tasks)
    assert not publisher.publishing_stalled(ENABLED, "hcloud", list(cluster.runs.values()))
    assert cluster.deferred == [] and not (clone(origin, tmp_path) / "data").exists()


def test_read_newest_variants_lists_the_highest_variant_and_its_tasks_from_git(tmp_path, origin, storage):
    seed = clone(origin, tmp_path, "seed2")
    files = [
        "a/0/lscpu/meta.json",
        "a/0/membench/meta.json",
        "a/2/lscpu/meta.json",
        "a/2/lscpu/stdout",
        "a/2/membench/stdout",  # no meta.json: not done
        "a/10/variant.json",  # a variant with no tasks yet; 10 is newer than 2
        "b/0/lscpu/meta.json",
        "b/0/variant.json",
        "b/schedule/lscpu/meta.json",
        "c/schedule/lscpu/meta.json",  # no variant at all
    ]
    for name in files:
        path = seed / "data" / "hcloud" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "more")
    git(seed, "push", "origin", "HEAD:main")
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = publisher.Publisher(None, storage, env, tmp_path / "work")
    pub.read_schedule_metas("hcloud")  # fetches origin/main

    assert pub.read_newest_variants("hcloud") == {"a": (10, frozenset()), "b": (0, frozenset({"lscpu"}))}
    assert pub.read_newest_variants("nothing") == {}


def test_read_first_seen_reads_variant_json_from_git(tmp_path, origin, storage):
    seed = clone(origin, tmp_path, "seed3")
    files = {
        "a/1/variant.json": '{"first_seen": "2026-03-01T10:00:00", "id": "x"}',
        "a/2/variant.json": '{"first_seen": null}',
        "a/3/variant.json": '{"first_seen": "yesterday"}',
        "a/4/variant.json": "{damaged",
        "a/5/lscpu/meta.json": "{}",  # no variant.json
    }
    for name, text in files.items():
        path = seed / "data" / "hcloud" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "variants")
    git(seed, "push", "origin", "HEAD:main")
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = publisher.Publisher(None, storage, env, tmp_path / "work")
    pub.read_schedule_metas("hcloud")  # fetches origin/main

    assert pub.read_first_seen("hcloud", "a", 1) == datetime(2026, 3, 1, 10)
    assert [pub.read_first_seen("hcloud", "a", n) for n in (2, 3, 4, 5, 6)] == [None] * 5
    assert pub.read_first_seen("hcloud", "nothing", 0) is None


# --------------------------------------------------------------------------- DBaaS


def test_read_dbaas_metas_checks_out_the_databases_metas(tmp_path, origin, storage, caplog):
    seed = clone(origin, tmp_path, "seed4")
    files = {
        "db-a/postgres/18/standalone/pgbench/meta.json": json.dumps({"exit_code": 0}),
        "db-a/postgres/17/standalone/pgbench/meta.json": json.dumps({"exit_code": -1}),
        "db-b/postgres/18/standalone/pgbench/meta.json": "{damaged",
        "db-b/postgres/18/standalone/pgbench/stdout": "x",
    }
    for name, text in files.items():
        path = seed / "dbaas" / "gcp" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    other = seed / "dbaas" / "aws" / "z" / "postgres" / "18" / "standalone" / "pgbench" / "meta.json"
    other.parent.mkdir(parents=True)
    other.write_text("{}")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "dbaas")
    git(seed, "push", "origin", "HEAD:main")

    metas = make_publisher(tmp_path, origin, storage).read_dbaas_metas("gcp")

    assert metas == {
        "db-a": {"18": {"pgbench": {"exit_code": 0}}, "17": {"pgbench": {"exit_code": -1}}},
        "db-b": {"18": {"pgbench": None}},
    }
    assert "malformed dbaas/gcp/db-b/postgres/18/standalone/pgbench/meta.json" in caplog.text
    assert make_publisher(tmp_path, origin, storage).read_dbaas_metas("nothing") == {}


def test_a_dbaas_result_is_published_with_the_bootstrap_settings_merged_into_stdout(tmp_path, origin, cluster):
    storage = FakeStorage()
    seed = clone(origin, tmp_path, "seed5")
    (seed / "data/hcloud/cax31").mkdir(parents=True)
    (seed / "data/hcloud/cax31/.keep").write_text("")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "sku exists")
    git(seed, "push", "origin", "HEAD:main")
    run = RunKey("hcloud", "cax31", datetime.now(UTC).date(), "r-db")
    status = {
        "start": "2026-10-01T10:00:00",
        "end": "2026-10-01T10:05:00",
        "exit_code": 0,
        "task_hash": "h",
        "run_id": "r-db",
    }
    storage.put_task("pgbench", status, b'{"score": 7, "sslmode": ""}', run=run)
    storage.objects[okey(run, "tasks/pgbench/postgres.json")] = json.dumps(
        {
            "postgres": {"version": "PostgreSQL 18.1"},
            "extra": {"sslmode": "require", "sync_commit_session_settable": True},
        }
    ).encode()
    storage.objects[okey(run, "run.json")] = json.dumps(
        {
            "database": {
                "vendor": "gcp",
                "id": "db1",
                "engineVersion": 18,
                "region": "r",
                "vcpus": 4,
                "memoryGib": 16.0,
            }
        }
    ).encode()
    tasks = {**TASKS, "pgbench": TaskDef("pgbench", "img", None, timedelta(minutes=5))}
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    settings = Settings(
        vendors={
            "hcloud": VendorSettings(enabled=True, publish=True),
            "gcp": VendorSettings(enabled=True, publish=True),
        }
    )
    pub = publisher.Publisher(None, storage, env, tmp_path / "work", get_task=tasks.__getitem__)
    assert pub.rescan(1, settings=settings).tasks_published == 1
    base = clone(origin, tmp_path) / "dbaas/gcp/db1/postgres/18/standalone/pgbench"
    published = json.loads((base / "stdout").read_text())
    assert published["score"] == 7 and published["postgres"] == {"version": "PostgreSQL 18.1"}
    assert published["sslmode"] == "require" and published["sync_commit_session_settable"] is True
    assert not (base / "postgres.json").exists()  # merged, not a file of its own
    assert {k: published[k] for k in published if k not in ("score", "postgres", "sslmode")} == {
        "sync_commit_session_settable": True,
        "db_vcpus": 4,
        "db_mem_gib": 16.0,
        "profile_vus": [1, 2, 4, 8],
        "profile_search": False,
        "profile_max_clients": 8,
        "profile_hard_max_clients": 8,
        "benchmark_image": "img",
        "durability": "durable",
        "workload_kind": "pgbench_ro",
        "cpu_scale": 1,
        "schema_gib": 0.17,
    }  # the fields the legacy runner added


def test_a_dbaas_run_is_published_by_its_database_not_by_its_client_sku():
    database = {"vendor": "gcp", "id": "db1", "engineVersion": "18", "region": "r"}
    run = BenchmarkRun(
        "gcp-x-1", "inspector", "u", "1", datetime.now(UTC), RunSpec("gcp", "client-sku", "r", database=database)
    )
    only_listed = Settings(vendors={"gcp": VendorSettings(enabled=True, publish=("db1",))})
    assert publisher._publishable(only_listed, run)  # the client SKU is not in the list, the database is
    assert not publisher._publishable(Settings(vendors={"gcp": VendorSettings(enabled=True, publish=("other",))}), run)
    server_run = BenchmarkRun("gcp-x-1", "inspector", "u", "1", datetime.now(UTC), RunSpec("gcp", "client-sku", "r"))
    assert not publisher._publishable(only_listed, server_run)


def test_dbaas_timing_is_published_and_the_destroy_files_follow_later(tmp_path, origin):
    t = datetime(2026, 11, 24, 21, 50, 0, 123, tzinfo=UTC)
    database = {"vendor": "gcp", "id": "db1", "engineVersion": "18", "region": "r"}
    task = TaskStatus("pgbench", state=TaskState.DONE, start=t + timedelta(minutes=10), end=t + timedelta(minutes=30))
    task.published = True
    run = BenchmarkRun("gcp-x-1", "inspector", "u", "1", t, RunSpec("gcp", "client-sku", "r-db", database=database))
    run.status.tasks = [task]
    run.status.database = DbStatus(DbPhase.READY, t, t, timing={"api_start": t, "api_end": t + timedelta(minutes=8)})
    settings = Settings(vendors={"gcp": VendorSettings(enabled=True, publish=True)})
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = publisher.Publisher(None, FakeStorage(), env, tmp_path / "work")

    pub._publish_dbaas_timing([run], settings)
    timing = clone(origin, tmp_path, "check1") / "dbaas/gcp/db1/postgres/18/standalone/timing/r-db"
    assert {p.name: p.read_text() for p in timing.iterdir()} == {
        "api_start": "2026-11-24T21:50:00Z\n",
        "api_end": "2026-11-24T21:58:00Z\n",
        "inspector_start": "2026-11-24T22:00:00Z\n",
        "inspector_end": "2026-11-24T22:20:00Z\n",
    }  # no destroy files while the destroy has not run

    run.status.database.timing |= {
        "api_destroy_start": t + timedelta(minutes=31),
        "api_destroy_end": t + timedelta(minutes=35),
    }
    pub._publish_dbaas_timing([run], settings)
    timing = clone(origin, tmp_path, "check2") / "dbaas/gcp/db1/postgres/18/standalone/timing/r-db"
    assert (timing / "api_destroy_start").read_text() == "2026-11-24T22:21:00Z\n"
    assert (timing / "api_destroy_end").read_text() == "2026-11-24T22:25:00Z\n"


def test_dbaas_timing_skips_runs_without_recorded_timing_and_prunes_what_is_gone(tmp_path, origin):
    t = datetime(2026, 11, 24, 21, 50, 0, tzinfo=UTC)
    database = {"vendor": "gcp", "id": "db1", "engineVersion": "18", "region": "r"}
    task = TaskStatus("pgbench", state=TaskState.DONE, start=t, end=t + timedelta(minutes=30))
    task.published = True
    run = BenchmarkRun("gcp-x-1", "inspector", "u", "1", t, RunSpec("gcp", "client-sku", "r-old", database=database))
    run.status.tasks = [task]
    run.status.database = DbStatus(DbPhase.READY, t, t)  # no timing recorded
    settings = Settings(vendors={"gcp": VendorSettings(enabled=True, publish=True)})
    env = EnvConfig(bucket="b", bucket_region="r", git_url=origin.as_uri())
    pub = publisher.Publisher(None, FakeStorage(), env, tmp_path / "work")

    pub._publish_dbaas_timing([run], settings)
    assert not (clone(origin, tmp_path, "check") / "dbaas/gcp/db1/postgres/18/standalone/timing").exists()

    pub._timing_rendered["gone"] = {"api_start": "x"}
    pub._publish_dbaas_timing([run], settings)
    assert pub._timing_rendered == {}


def run_record(origin, tmp_path, rest="data/hcloud/cax31/runs") -> dict:
    return json.loads((clone(origin, tmp_path) / rest / f"{RUN_ID}.json").read_text())


def test_run_record_is_written_when_the_run_ended_and_everything_is_published(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES)
    doc = json.loads(storage.objects[okey(KEY, "run.json")])
    doc["attempts"][0] |= {"outcome": "Completed", "capacity_type": "spot"}
    storage.objects[okey(KEY, "run.json")] = json.dumps(doc).encode()
    pub = make_publisher(tmp_path, origin, storage)
    pub.publish_once(ENABLED)
    record = run_record(origin, tmp_path)
    assert record["phase"] == "Done" and record["attempts"][0]["capacity_type"] == "spot"
    assert "lscpu_stdout" not in record["attempts"][0]
    commits = git(clone(origin, tmp_path), "log", "--format=%s").splitlines()
    assert commits[0] == f"k8s-inspector: run records ({RUN_ID})"
    pub.publish_once(ENABLED)  # written once
    assert git(clone(origin, tmp_path), "log", "--format=%s").splitlines() == commits
    assert run.name in pub._records_written


def test_run_record_waits_for_unpublished_tasks_and_a_run_that_has_not_ended(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES, phase=RunPhase.RUNNING)
    for t in run.status.tasks:
        t.published = True
    pub = make_publisher(tmp_path, origin, storage)
    pub.publish_once(ENABLED)
    assert "runs" not in git(clone(origin, tmp_path), "ls-tree", "-r", "--name-only", "HEAD")
    run.status.phase = RunPhase.FAILED
    run.status.tasks[0].published = False
    run.status.tasks[0].state = TaskState.PENDING
    assert not pub._record_due(run, ENABLED)


def test_cancelled_run_gets_a_record(tmp_path, origin, storage, cluster):
    run = cluster.add_run(["lscpu"], phase=RunPhase.CANCELLED)
    run.status.tasks[0].published = True
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert run_record(origin, tmp_path)["phase"] == "Cancelled"


def test_run_is_kept_until_its_record_is_written(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES, phase=RunPhase.DONE)
    for t in run.status.tasks:
        t.published = True
    old = datetime.now(UTC) - timedelta(hours=99)
    run.status.conditions = [{"type": "Published", "lastTransitionTime": old.isoformat().replace("+00:00", "Z")}]
    settings = replace(ENABLED, publish=PublishSettings(keep_run_hours=24))
    pub = make_publisher(tmp_path, origin, storage)
    assert pub.delete_expired_runs(settings) == 0  # the record is not in git yet
    pub.publish_once(settings)
    assert (clone(origin, tmp_path) / "data/hcloud/cax31/runs" / f"{RUN_ID}.json").exists()
    assert pub.delete_expired_runs(settings) == 1


def test_run_without_run_json_does_not_block_its_deletion(tmp_path, origin, cluster):
    run = cluster.add_run(["lscpu"], phase=RunPhase.DONE)
    run.status.tasks[0].published = True
    pub = make_publisher(tmp_path, origin, FakeStorage())
    pub.publish_once(ENABLED)
    assert run.name in pub._records_written


def backfill_storage(runs: dict[str, str | None]) -> FakeStorage:
    """`runs`: run id -> outcome of its only attempt."""
    storage = FakeStorage()
    day = datetime.now(UTC).date()
    for run_id, outcome in runs.items():
        run = RunKey("hcloud", "cax31", day, run_id)
        attempt = {"attempt": 1, "outcome": outcome, "lscpu_stdout": "x"}
        doc = {"vendor": "hcloud", "sku": "cax31", "run_id": run_id, "attempts": [attempt], "database": None}
        storage.objects[okey(run, "run.json")] = json.dumps(doc).encode()
    return storage


def test_backfill_writes_records_in_batches_and_skips_existing_ones_and_live_runs(
    tmp_path, origin, cluster, monkeypatch
):
    monkeypatch.setattr(publisher, "BACKFILL_BATCH", 2)
    storage = backfill_storage(
        {"r-done": "Completed", "r-failed": "ProvisioningFailed", "r-open": None, "r-live": "Completed"}
    )
    live = cluster.add_run(["lscpu"])
    live.spec.run_id = "r-live"
    pub = make_publisher(tmp_path, origin, storage)
    assert pub.backfill_run_records(30, ENABLED) == 3
    check = clone(origin, tmp_path) / "data/hcloud/cax31/runs"
    phases = {p.stem: json.loads(p.read_text())["phase"] for p in check.iterdir()}
    assert phases == {"r-done": "Done", "r-failed": "Failed", "r-open": "unknown"}
    assert "lscpu_stdout" not in (check / "r-done.json").read_text()
    log = git(clone(origin, tmp_path), "log", "--format=%s").splitlines()
    assert log == ["k8s-inspector: backfill 1 run records", "k8s-inspector: backfill 2 run records", "seed"]
    assert pub.backfill_run_records(30, ENABLED) == 0  # rerun: nothing new, no commit
    assert git(clone(origin, tmp_path), "log", "--format=%s").splitlines() == log


def test_backfill_does_not_overwrite_a_live_record(tmp_path, origin, cluster):
    seed = clone(origin, tmp_path, "seed4")
    (seed / "data/hcloud/cax31/runs").mkdir(parents=True)
    (seed / "data/hcloud/cax31/runs/r-done.json").write_text('{"phase": "Done", "kept": true}\n')
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "record")
    git(seed, "push", "origin", "HEAD:main")
    pub = make_publisher(tmp_path, origin, backfill_storage({"r-done": "Cancelled"}))
    assert pub.backfill_run_records(30, ENABLED) == 0
    assert json.loads((clone(origin, tmp_path) / "data/hcloud/cax31/runs/r-done.json").read_text())["kept"]


def test_backfill_needs_an_enabled_vendor(tmp_path, origin, cluster):
    pub = make_publisher(tmp_path, origin, backfill_storage({"r1": "Completed"}))
    assert pub.backfill_run_records(30, Settings()) == 0


def test_run_record_takes_the_variant_from_the_run_status(tmp_path, origin, storage, cluster):
    run = cluster.add_run(["lscpu"], phase=RunPhase.CANCELLED)
    run.status.tasks[0].published = True
    run.status.variant = 3  # run.json holds the runner's snapshot: no variant
    make_publisher(tmp_path, origin, storage).publish_once(ENABLED)
    assert run_record(origin, tmp_path)["variant"] == 3


def test_restarted_controller_holds_the_run_until_the_record_is_checked(tmp_path, origin, storage, cluster):
    run = cluster.add_run(NAMES, phase=RunPhase.DONE)
    for t in run.status.tasks:
        t.published = True
    old = datetime.now(UTC) - timedelta(hours=99)
    run.status.conditions = [{"type": "Published", "lastTransitionTime": old.isoformat().replace("+00:00", "Z")}]
    settings = replace(ENABLED, publish=PublishSettings(keep_run_hours=24))
    make_publisher(tmp_path, origin, storage).publish_once(settings)
    restarted = make_publisher(tmp_path, origin, storage)  # empty in-memory set
    assert restarted.delete_expired_runs(settings) == 0
    restarted.publish_once(settings)  # finds the record in the tree, writes nothing
    assert git(clone(origin, tmp_path), "log", "--format=%s").splitlines()[0].startswith("k8s-inspector: run records")
    assert restarted.delete_expired_runs(settings) == 1


@pytest.mark.parametrize("raw", [b"{not json", b'{"vendor": "..", "sku": "x", "run_id": "r"}', b'{"run_id": "r"}'])
def test_unusable_run_json_gives_up_and_does_not_hold_the_run(tmp_path, origin, cluster, raw):
    run = cluster.add_run(["lscpu"], phase=RunPhase.DONE)
    run.status.tasks[0].published = True
    storage = FakeStorage()
    storage.objects[okey(KEY, "run.json")] = raw
    pub = make_publisher(tmp_path, origin, storage)
    pub.publish_once(ENABLED)
    assert run.name in pub._records_written
    assert git(clone(origin, tmp_path), "log", "--format=%s").splitlines() == ["seed"]


def test_storage_error_or_failed_render_is_retried(tmp_path, origin, storage, cluster, monkeypatch):
    run = cluster.add_run(["lscpu"], phase=RunPhase.DONE)
    run.status.tasks[0].published = True
    pub = make_publisher(tmp_path, origin, storage)
    monkeypatch.setattr(pub, "_read_run_json", lambda key: (_ for _ in ()).throw(OSError("s3 down")))
    pub.publish_once(ENABLED)
    assert run.name not in pub._records_written
    monkeypatch.delattr(pub, "_read_run_json")
    monkeypatch.setattr(publisher.render, "render_run_record", lambda *args: (_ for _ in ()).throw(ValueError("x")))
    pub.publish_once(ENABLED)
    assert run.name not in pub._records_written  # rendering raised: kept, not deleted without a record


def test_backfill_takes_the_variant_from_git_and_skips_malformed_run_json(tmp_path, origin, cluster):
    seed = clone(origin, tmp_path, "seed5")
    (seed / "data/hcloud/cax31/2/timing/r-done").mkdir(parents=True)
    (seed / "data/hcloud/cax31/2/timing/r-done/inspector_start").write_text("2026-10-01T10:00:00Z\n")
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "variant 2")
    git(seed, "push", "origin", "HEAD:main")
    storage = backfill_storage({"r-done": "Completed", "r-novariant": "Completed", "r-bad": "Completed"})
    day = datetime.now(UTC).date()
    storage.objects[okey(RunKey("hcloud", "cax31", day, "r-bad"), "run.json")] = b"{not json"
    unsafe = {"vendor": "hcloud", "sku": "..", "run_id": "r-unsafe", "attempts": [{"attempt": 1}]}
    storage.objects[okey(RunKey("hcloud", "cax31", day, "r-unsafe"), "run.json")] = json.dumps(unsafe).encode()
    pub = make_publisher(tmp_path, origin, storage)
    assert pub.backfill_run_records(30, ENABLED) == 2
    check = clone(origin, tmp_path) / "data/hcloud/cax31/runs"
    assert json.loads((check / "r-done.json").read_text())["variant"] == 2
    assert json.loads((check / "r-novariant.json").read_text())["variant"] is None


def cost_lines(caplog) -> list[dict]:
    return [r.__dict__ for r in caplog.records if getattr(r, "event", "").endswith("cost")]


def test_cost_lines_are_logged_once_when_the_record_is_written(tmp_path, origin, storage, cluster, caplog):
    run = cluster.add_run(NAMES)
    start = datetime(2026, 9, 29, 12, 57, tzinfo=UTC)
    run.status.tasks[0].attempt, run.status.tasks[0].start = 1, start + timedelta(minutes=2)
    run.status.tasks[0].end, run.status.tasks[0].commit = start + timedelta(minutes=3), "abc"
    doc = json.loads(storage.objects[okey(KEY, "run.json")])
    doc["attempts"][0] |= {
        "attempt": 1,
        "api_start": "2026-09-29T12:57:00Z",
        "end": "2026-09-29T13:57:00+00:00",
        "outcome": "Completed",
        "capacity_type": "spot",
        "price": "0.5",
        "region": "fsn1",
        "ondemand_price": 1.0,
    }
    doc["family"] = "Cost Optimized"
    storage.objects[okey(KEY, "run.json")] = json.dumps(doc).encode()
    pub = make_publisher(tmp_path, origin, storage)
    with caplog.at_level("INFO", logger="k8s_inspector.cost"):
        pub.publish_once(ENABLED)
        pub.publish_once(ENABLED)
    lines = cost_lines(caplog)
    assert [line["event"] for line in lines] == ["run_attempt_cost", "run_cost"]
    setup = lines[0]["setup_s"]  # the first task start of the attempt, from the status tasks
    assert setup is not None and lines[1]["paid_usd"] == 0.5
    assert lines[1]["ondemand_counterfactual_usd"] == pytest.approx(1.0, abs=1e-5)  # one setup plus the task time
    assert lines[0]["backfill"] is False and lines[0]["ondemand_price_source"] == "launch"
    record = run_record(origin, tmp_path)
    assert record["attempts"][0]["cost_usd"] == 0.5 and record["attempts"][0]["setup_s"] == setup


def push_record(origin, tmp_path, run_id, attempts, files=None):
    seed = clone(origin, tmp_path, "seed-cost")
    runs = seed / "data/hcloud/cax31/runs"
    runs.mkdir(parents=True, exist_ok=True)
    record = {"run_id": run_id, "vendor": "hcloud", "sku": "cax31", "database": None, "attempts": attempts}
    (runs / f"{run_id}.json").write_text(json.dumps(record))
    for rel, text in (files or {}).items():
        (seed / rel).parent.mkdir(parents=True, exist_ok=True)
        (seed / rel).write_text(text)
    git(seed, "add", "-A")
    git(seed, "commit", "-m", "record")
    git(seed, "push", "origin", "HEAD:main")


def test_a_record_of_a_run_without_price_data_gets_keeper_current_price_or_a_reason(
    tmp_path, origin, storage, cluster, caplog, monkeypatch
):
    """A run launched by a controller that did not capture the on-demand price at launch."""
    cluster.add_run(NAMES)
    doc = json.loads(storage.objects[okey(KEY, "run.json")])
    doc["attempts"][0] |= {
        "attempt": 1, "api_start": "2026-09-29T12:57:00Z", "end": "2026-09-29T13:57:00+00:00",
        "outcome": "Completed", "capacity_type": "spot", "price": "0.5", "region": "fsn1",
    }  # fmt: skip
    storage.objects[okey(KEY, "run.json")] = json.dumps(doc).encode()
    monkeypatch.setattr(keeper, "ondemand_prices", lambda env, vendor, sku: {"fsn1": 0.8})
    pub = make_publisher(tmp_path, origin, storage)
    with caplog.at_level("INFO", logger="k8s_inspector.cost"):
        pub.publish_once(ENABLED)
    attempt = run_record(origin, tmp_path)["attempts"][0]
    assert (attempt["ondemand_price"], attempt["ondemand_price_source"]) == (0.8, "current")
    assert cost_lines(caplog)[0]["ondemand_price"] == 0.8


def test_a_keeper_outage_is_asked_once_per_pass_and_leaves_the_reason(tmp_path, origin, storage, cluster, monkeypatch):
    cluster.add_run(NAMES)
    doc = json.loads(storage.objects[okey(KEY, "run.json")])
    doc["attempts"][0] |= {
        "attempt": 1, "api_start": "2026-09-29T12:57:00Z", "end": "2026-09-29T13:57:00+00:00",
        "outcome": "Completed", "capacity_type": "spot", "price": "0.5", "region": "fsn1",
    }  # fmt: skip
    calls = []

    def down(env, vendor, sku):
        calls.append(sku)
        raise RuntimeError("keeper down")

    monkeypatch.setattr(keeper, "server_facts", down)
    monkeypatch.setattr(keeper, "ondemand_prices", down)
    pub = make_publisher(tmp_path, origin, storage)
    records = [{"vendor": "hcloud", "sku": s, "attempts": [dict(doc["attempts"][0])]} for s in ("a", "b", "c")]
    seen: set = set()
    for record in records:
        pub._fill_from_keeper(record, {}, {}, seen)
    assert len(calls) == 1  # the later records do not wait for Keeper again
    assert all(r["attempts"][0]["ondemand_price_reason"] == "keeper_unreachable" for r in records)
    assert all(r["attempts"][0]["ondemand_price"] is None for r in records)


def test_emit_cost_logs_logs_past_records_with_current_prices_and_changes_nothing(
    tmp_path, origin, caplog, monkeypatch
):
    today = datetime.now(UTC)
    run_id = f"r{today:%Y%m%d}t0100-aaaa"
    day = f"{today:%Y-%m-%d}"
    attempts = [
        {"attempt": 1, "api_start": f"{day}T01:00:00Z", "end": f"{day}T02:00:00+00:00", "outcome": "SpotInterrupted",
         "capacity_type": "spot", "price": "0.5", "region": "fsn1"},
        {"attempt": 2, "api_start": f"{day}T02:00:10Z", "end": f"{day}T03:00:10+00:00", "outcome": "Completed",
         "capacity_type": "on-demand", "price": "1.0", "region": "nbg1", "ondemand_price": 0.9},
    ]  # fmt: skip
    meta = {"run_id": run_id, "start": f"{day}T01:10:00", "end": f"{day}T01:30:00", "exit_code": 0}
    other = {"run_id": "r-other", "start": f"{day}T01:10:00", "end": f"{day}T01:50:00", "exit_code": 0}
    push_record(origin, tmp_path, run_id, attempts, {
        "data/hcloud/cax31/0/membench/meta.json": json.dumps(meta),
        "data/hcloud/cax31/0/lscpu/meta.json": json.dumps(other),
    })  # fmt: skip
    push_record(origin, tmp_path, "r20200101t0000-old", attempts)  # older than --since
    monkeypatch.setattr(keeper, "ondemand_prices", lambda env, vendor, sku: {"fsn1": 0.8, "nbg1": 0.7})
    monkeypatch.setattr(
        keeper, "server_facts", lambda env, vendor, sku: {"family": "F", "cpu_arch": "arm64", "gpu_count": 0}
    )
    pub = make_publisher(tmp_path, origin, FakeStorage())
    head = git(clone(origin, tmp_path), "rev-parse", "HEAD")
    with caplog.at_level("INFO", logger="k8s_inspector.cost"):
        assert pub.emit_cost_logs(30) == 1
    first, second, summary = cost_lines(caplog)
    assert first["backfill"] is True and first["family"] == "F"
    assert (first["ondemand_price"], first["ondemand_price_source"]) == (0.8, "current")
    assert (first["useful_s"], first["setup_s"]) == (1800.0, 600.0)  # the one result file of its run id
    assert (second["ondemand_price"], second["ondemand_price_source"]) == (0.9, "launch")  # stored: kept
    assert summary["event"] == "run_cost" and summary["backfill"] is True and summary["used_spot"] is True
    assert git(clone(origin, tmp_path), "rev-parse", "HEAD") == head


def test_emit_cost_logs_leaves_unknown_useful_time_null(tmp_path, origin, caplog):
    today = datetime.now(UTC)
    day = f"{today:%Y-%m-%d}"
    attempts = [{"attempt": 1, "api_start": f"{day}T01:00:00Z", "end": f"{day}T02:00:00+00:00",
                 "outcome": "SpotInterrupted", "capacity_type": "spot", "price": "0.5", "region": "fsn1"}]  # fmt: skip
    push_record(origin, tmp_path, f"r{today:%Y%m%d}t0100-bbbb", attempts)  # no result files of the run
    pub = make_publisher(tmp_path, origin, FakeStorage())
    with caplog.at_level("INFO", logger="k8s_inspector.cost"):
        pub.emit_cost_logs(30)
    attempt_line, summary = cost_lines(caplog)
    assert attempt_line["useful_s"] is None and attempt_line["wasted_usd"] is None
    assert summary["wasted_usd"] is None and summary["ondemand_counterfactual_usd"] is None


def test_tasks_written_off_by_the_controller_are_not_task_spans(cluster):
    """`-4` (lost with the node) and `-1` (never ran) are committed to `schedule/` with start and end of now."""
    run = cluster.add_run(["lscpu", "geekbench", "openssl"])
    now = datetime(2026, 9, 29, 13, 0, tzinfo=UTC)
    for t, code in zip(run.status.tasks, (0, -4, -1), strict=True):
        t.attempt, t.start, t.end, t.exit_code, t.published, t.commit = 1, now, now, code, True, "abc"
    spans = publisher._task_spans(run)
    assert [(s.attempt, s.kept) for s in spans] == [(1, True)]
