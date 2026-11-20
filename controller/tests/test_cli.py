import argparse
import signal
import socket
import threading
import urllib.request
from datetime import UTC, datetime, timedelta

import pytest

from k8s_inspector import cli, crd, planner
from k8s_inspector.kube import NotFound
from k8s_inspector.model import (
    BenchmarkRun,
    CapacityMode,
    CapacityType,
    EnvConfig,
    PublishSettings,
    RunPhase,
    RunSpec,
    RunStatus,
    Settings,
    TaskState,
    TaskStatus,
    VendorSettings,
)

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
ENV = EnvConfig(bucket="b", bucket_region="r", sc_run_image="ghcr.io/x/sc-run@sha256:1")
SETTINGS = Settings(vendors={"oci": VendorSettings(enabled=True)})


# --------------------------------------------------------------------------- parsing


@pytest.mark.parametrize(
    ("text", "expected"),
    [("0s", timedelta(0)), ("90s", timedelta(seconds=90)), ("90m", timedelta(minutes=90)), ("2h", timedelta(hours=2)),
     ("7d", timedelta(days=7))],
)  # fmt: skip
def test_parse_duration(text, expected):
    assert cli.parse_duration(text) == expected


@pytest.mark.parametrize("text", ["", "2", "h", "2x", "1h30m", "-1h"])
def test_parse_duration_rejects(text):
    with pytest.raises(argparse.ArgumentTypeError):
        cli.parse_duration(text)


def test_parser_run_options():
    args = cli.build_parser().parse_args(
        [
            "run",
            "oci",
            "VM.Standard.E5.Flex.2-16",
            "--tasks",
            "lscpu,stressngfull",
            "--force",
            "--on-demand",
            "--hold",
            "2h",
        ]
    )
    assert (args.vendor, args.sku) == ("oci", "VM.Standard.E5.Flex.2-16")
    assert args.tasks == ["lscpu", "stressngfull"]
    assert args.force and args.on_demand
    assert args.hold == timedelta(hours=2)


def test_parser_defaults_and_other_commands():
    parser = cli.build_parser()
    args = parser.parse_args(["run", "oci", "sku"])
    assert args.tasks == [] and not args.force and not args.on_demand and args.hold == timedelta(0)
    assert parser.parse_args(["plan", "--dry-run"]).dry_run
    assert parser.parse_args(["status", "--failing"]).failing
    assert parser.parse_args(["cancel", "oci-x-1234"]).run == "oci-x-1234"
    args = parser.parse_args(["publish", "--rescan", "--since", "3d", "--sku", "a", "--sku", "b"])
    assert (args.rescan, args.since, args.sku) == (True, timedelta(days=3), ["a", "b"])
    assert parser.parse_args(["serve"]).command == "serve"
    with pytest.raises(SystemExit) as e:
        parser.parse_args([])
    assert e.value.code == 2


# --------------------------------------------------------------------------- status


def make_run(sku, phase, tasks=(), vendor="oci", **status):
    return BenchmarkRun(
        f"{vendor}-{sku}",
        "inspector",
        "u",
        "1",
        NOW - timedelta(minutes=125),
        RunSpec(vendor, sku, "r1"),
        RunStatus(phase=phase, tasks=list(tasks), **status),
    )


def task(name, exit_code, error=None, state=TaskState.DONE):
    return TaskStatus(name, state, exit_code=exit_code, error_msg=error)


def test_render_status_table():
    runs = [
        make_run(
            "b",
            RunPhase.RUNNING,
            progress="3/20",
            current_step="07-geekbench",
            capacity_type=CapacityType.SPOT,
            attempt=2,
        ),
        make_run("a", RunPhase.PENDING),
    ]
    lines = cli.render_status(runs, NOW).splitlines()

    assert lines[0].split() == ["NAME", "VENDOR", "SKU", "PHASE", "PROGRESS", "STEP", "CAPACITY", "ATTEMPT", "AGE"]
    assert lines[1].split() == ["oci-a", "oci", "a", "Pending", "0/0", "-", "-", "0", "2h"]
    assert lines[2].split() == ["oci-b", "oci", "b", "Running", "3/20", "07-geekbench", "spot", "2", "2h"]
    assert (
        len({line.index("VENDOR") if i == 0 else line.index(" oci ") for i, line in enumerate(lines[:1] + lines[1:2])})
        >= 1
    )


def test_render_status_columns_align():
    lines = cli.render_status(
        [make_run("a-long-sku-name", RunPhase.DONE), make_run("b", RunPhase.RUNNING)], NOW
    ).splitlines()
    assert len({line.index("PHASE") if i == 0 else line.index(line.split()[3]) for i, line in enumerate(lines)}) == 1


def test_render_status_empty():
    assert cli.render_status([], NOW) == "no runs"
    assert (
        cli.render_status([make_run("a", RunPhase.RUNNING, [task("lscpu", 0)])], NOW, failing=True)
        == "no failing tasks"
    )


def test_render_status_failing_lists_failed_done_tasks_only():
    runs = [
        make_run(
            "a",
            RunPhase.PUBLISHING,
            [
                task("lscpu", 0),
                task("membench", -1, "SCARPENTER_ICE: no capacity"),
                task("stress", 137, "OOMKilled"),
                task("pending", None, state=TaskState.PENDING),
            ],
        )
    ]
    lines = cli.render_status(runs, NOW, failing=True).splitlines()

    assert lines[0].split() == ["RUN", "TASK", "EXIT", "ERROR"]
    assert len(lines) == 3
    assert lines[1].split(None, 3) == ["oci-a", "membench", "-1", "SCARPENTER_ICE: no capacity"]
    assert lines[2].split() == ["oci-a", "stress", "137", "OOMKilled"]


@pytest.mark.parametrize(
    ("delta", "text"),
    [
        (timedelta(seconds=5), "5s"),
        (timedelta(minutes=7), "7m"),
        (timedelta(hours=26), "1d"),
        (timedelta(hours=-1), "0s"),
    ],
)
def test_age(delta, text):
    assert cli._age(NOW - delta, NOW) == text


# --------------------------------------------------------------------------- one-shot commands


class FakeKube:
    namespace = "inspector"


@pytest.fixture
def env(monkeypatch):
    for name, value in {"K8SI_BUCKET": "b", "K8SI_BUCKET_REGION": "r", "K8SI_SC_RUN_IMAGE": "img@sha256:1"}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("K8SI_NAMESPACE", raising=False)
    monkeypatch.setattr(cli, "Kube", lambda namespace: FakeKube())
    monkeypatch.setattr(cli.config_loader, "load_settings", lambda kube, ns: SETTINGS)
    monkeypatch.setattr(cli, "setup_logging", lambda: None)


def test_main_sets_up_logging_for_every_command(env, monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "setup_logging", lambda: calls.append(1))
    monkeypatch.setattr(crd, "list_runs", lambda kube: [])
    assert cli.main(["status"]) == 0
    assert calls == [1]


def test_run_prints_run_name(env, monkeypatch, capsys):
    seen = {}

    def force_run(kube, cfg, vendor, sku, **kwargs):
        seen.update(vendor=vendor, sku=sku, **kwargs)
        return make_run("sku", RunPhase.PENDING)

    monkeypatch.setattr(planner, "force_run", force_run)
    code = cli.main(["run", "oci", "sku", "--tasks", "lscpu", "--force", "--on-demand", "--hold", "2h"])

    assert code == 0
    assert capsys.readouterr().out == "oci-sku\n"
    assert seen == {
        "vendor": "oci",
        "sku": "sku",
        "tasks": ["lscpu"],
        "force": True,
        "capacity": CapacityMode.ON_DEMAND,
        "hold_after": timedelta(hours=2),
    }


def test_run_without_on_demand_leaves_capacity_to_vendor_setting(env, monkeypatch):
    seen = {}
    monkeypatch.setattr(planner, "force_run", lambda *a, **kw: seen.update(kw) or make_run("s", RunPhase.PENDING))
    assert cli.main(["run", "oci", "sku"]) == 0
    assert seen["capacity"] is None and seen["force"] is False


def test_run_errors_exit_1_with_message(env, monkeypatch, capsys):
    def refuse(*a, **kw):
        raise ValueError("vendor oci is not enabled in the ConfigMap")

    monkeypatch.setattr(planner, "force_run", refuse)
    assert cli.main(["run", "oci", "sku"]) == 1
    assert "error: vendor oci is not enabled" in capsys.readouterr().err

    def exists(*a, **kw):
        raise crd.RunExists("x")

    monkeypatch.setattr(planner, "force_run", exists)
    assert cli.main(["run", "oci", "sku"]) == 1
    err = capsys.readouterr().err
    assert f"{crd.run_name('oci', 'sku')}" in err and "cancel" in err


def test_run_and_serve_fail_fast_without_run_image(env, monkeypatch, capsys):
    monkeypatch.setenv("K8SI_SC_RUN_IMAGE", "")
    monkeypatch.setattr(planner, "force_run", lambda *a, **kw: pytest.fail("must not run"))
    for argv in (["run", "oci", "sku"], ["serve"]):
        assert cli.main(argv) == 2
        assert "K8SI_SC_RUN_IMAGE" in capsys.readouterr().err


def test_missing_environment_is_a_usage_error(env, monkeypatch, capsys):
    monkeypatch.delenv("K8SI_BUCKET")
    assert cli.main(["status"]) == 2
    assert "K8SI_BUCKET" in capsys.readouterr().err


def test_invalid_configmap_is_a_usage_error(env, monkeypatch, capsys):
    def invalid(kube, ns):
        raise ValueError("invalid config value for planner.autoPlan")

    monkeypatch.setattr(cli.config_loader, "load_settings", invalid)
    assert cli.main(["status"]) == 2
    assert "planner.autoPlan" in capsys.readouterr().err


def test_status_command(env, monkeypatch, capsys):
    monkeypatch.setattr(crd, "list_runs", lambda kube: [make_run("a", RunPhase.RUNNING)])
    assert cli.main(["status"]) == 0
    assert "oci-a" in capsys.readouterr().out


def test_plan_command(env, monkeypatch, capsys):
    seen = {}

    def plan_cycle(kube, cfg, dry_run=False):
        seen["dry_run"] = dry_run
        return []

    monkeypatch.setattr(planner, "plan_cycle", plan_cycle)
    assert cli.main(["plan", "--dry-run"]) == 0
    assert seen == {"dry_run": True}
    assert "nothing to plan" in capsys.readouterr().out

    monkeypatch.setattr(
        planner, "plan_cycle", lambda *a, **kw: [planner.PlannedRun("oci", "s", ("lscpu", "membench"), "hash changed")]
    )
    assert cli.main(["plan"]) == 0
    assert capsys.readouterr().out == "oci s lscpu,membench (hash changed)\n"


def test_cancel_requests_the_cancel_with_an_annotation(env, monkeypatch, capsys):
    runs = {
        "oci-a": make_run("a", RunPhase.RUNNING),
        "oci-d": make_run("d", RunPhase.DONE),
        "oci-p": make_run("p", RunPhase.PUBLISHING),
    }
    patches = []

    def get_run(kube, name):
        if name not in runs:
            raise NotFound(name)
        return runs[name]

    class PatchingKube(FakeKube):
        def merge_patch(self, kind, name, body, subresource_status=False):
            patches.append((kind, name, body, subresource_status))

    monkeypatch.setattr(cli, "Kube", lambda namespace: PatchingKube())
    monkeypatch.setattr(crd, "get_run", get_run)
    monkeypatch.setattr(crd, "delete_run", lambda *a: pytest.fail("cancel must keep the BenchmarkRun"))

    assert cli.main(["cancel", "oci-a"]) == 0
    assert patches == [
        ("benchmarkrun", "oci-a", {"metadata": {"annotations": {"inspector.sparecores.com/cancel": "true"}}}, False)
    ]
    for name in ("oci-d", "oci-p"):  # not active: nothing to cancel, no annotation
        assert cli.main(["cancel", name]) == 0
        assert "not active" in capsys.readouterr().out
    assert len(patches) == 1
    assert cli.main(["cancel", "nope"]) == 1
    assert "no such run" in capsys.readouterr().err


def test_publish_command(env, monkeypatch, capsys):
    calls = []

    class FakePublisher:
        def __init__(self, kube, storage, env, workdir, *, settings_provider):
            assert settings_provider() is SETTINGS

        def publish_once(self):
            calls.append("once")
            return cli.PublishResult(2, "abc123", ("r1",))

        def rescan(self, since_days, skus=()):
            calls.append(("rescan", since_days, list(skus)))
            return cli.PublishResult(0, None, ())

    monkeypatch.setattr(cli, "Publisher", FakePublisher)
    monkeypatch.setattr(cli, "Storage", lambda env, uploads: object())

    assert cli.main(["publish"]) == 0
    assert "published 2 tasks in commit abc123" in capsys.readouterr().out
    assert cli.main(["publish", "--rescan", "--since", "3d", "--sku", "s1"]) == 0
    assert calls == ["once", ("rescan", 3, ["s1"])]
    assert "published 0 tasks" in capsys.readouterr().out


def test_publish_failure_exits_1(env, monkeypatch, capsys):
    class Failing:
        def __init__(self, *a, **kw):
            pass

        def publish_once(self):
            raise RuntimeError("push rejected")

    monkeypatch.setattr(cli, "Publisher", Failing)
    monkeypatch.setattr(cli, "Storage", lambda env, uploads: object())
    assert cli.main(["publish"]) == 1
    assert "push rejected" in capsys.readouterr().err


# --------------------------------------------------------------------------- ConfigProvider


def test_config_provider_caches_and_keeps_last_good_settings(monkeypatch):
    now = [0.0]
    loads = []
    good, newer = Settings(), Settings(publish=PublishSettings(interval_minutes=9))

    def load(kube, ns):
        loads.append(ns)
        if len(loads) == 2:
            return newer
        if len(loads) == 3:
            raise ValueError("bad yaml")
        return good

    provider = cli.ConfigProvider(FakeKube(), ENV, ttl=30, clock=lambda: now[0])
    monkeypatch.setattr(cli.config_loader, "load_settings", load)
    assert provider().settings is good
    now[0] = 10
    assert provider().settings is good and len(loads) == 1  # cached
    now[0] = 31
    assert provider().settings is newer
    now[0] = 62
    assert provider().settings is newer  # reload failed, previous kept
    assert provider().env is ENV


def test_config_provider_first_load_error_propagates(monkeypatch):
    def bad(kube, ns):
        raise ValueError("bad")

    monkeypatch.setattr(cli.config_loader, "load_settings", bad)
    with pytest.raises(ValueError):
        cli.ConfigProvider(FakeKube(), ENV)()


# --------------------------------------------------------------------------- serve


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def controller(monkeypatch):
    """Fake Runner, Publisher, planner loop and Storage; `started` is set once all three loops run."""
    started = {"runner": threading.Event(), "planner": threading.Event(), "publisher": threading.Event()}
    stopped = []

    class FakeRunner:
        def __init__(self, kube, storage, cfg_provider, namespace):
            assert namespace == "inspector"
            assert cfg_provider().env is ENV

        def run_forever(self, stop):
            started["runner"].set()
            stop.wait()
            stopped.append("runner")

    class FakePublisher:
        def __init__(self, kube, storage, env, workdir, *, settings_provider, metrics):
            assert settings_provider() is SETTINGS
            self.metrics = metrics

        def run_forever(self, settings_provider, stop):
            assert settings_provider() is SETTINGS
            started["publisher"].set()
            self.metrics.record_publish()
            stop.wait()
            stopped.append("publisher")

    def plan_loop(kube, cfg_provider, stop):
        assert cfg_provider().settings is SETTINGS
        started["planner"].set()
        stop.wait()
        stopped.append("planner")

    monkeypatch.setattr(cli, "Runner", FakeRunner)
    monkeypatch.setattr(cli, "_MeteredPublisher", FakePublisher)
    monkeypatch.setattr(cli, "Storage", lambda env, uploads: object())
    monkeypatch.setattr(planner, "run_forever", plan_loop)
    monkeypatch.setattr(crd, "list_runs", lambda kube: [make_run("a", RunPhase.RUNNING)])
    monkeypatch.setattr(cli.config_loader, "load_settings", lambda kube, ns: SETTINGS)
    return started, stopped


def test_serve_starts_everything_and_stops_on_event(controller):
    started, stopped = controller
    stop = threading.Event()
    port = free_port()
    provider = cli.ConfigProvider(FakeKube(), ENV)
    result = []
    thread = threading.Thread(target=lambda: result.append(cli.serve(FakeKube(), provider, stop, port)))
    thread.start()
    try:
        assert all(event.wait(5) for event in started.values())
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
            body = r.read().decode()
        assert 'k8s_inspector_runs{phase="Running",vendor="oci"} 1' in body
        assert "k8s_inspector_last_publish_timestamp_seconds" in body
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as r:
            assert r.status == 200
    finally:
        stop.set()
        thread.join(10)
    assert not thread.is_alive()
    assert result == [0]
    assert sorted(stopped) == ["planner", "publisher", "runner"]


def test_serve_exits_1_when_a_component_dies(controller, monkeypatch):
    def crashing(kube, cfg_provider, stop):
        raise RuntimeError("boom")

    monkeypatch.setattr(planner, "run_forever", crashing)
    stop = threading.Event()
    result = []
    thread = threading.Thread(
        target=lambda: result.append(cli.serve(FakeKube(), cli.ConfigProvider(FakeKube(), ENV), stop, free_port()))
    )
    thread.start()
    thread.join(10)
    assert not thread.is_alive()
    assert result == [1]
    assert stop.is_set()
    assert sorted(controller[1]) == ["publisher", "runner"]  # the others were stopped too


def test_cmd_serve_installs_and_restores_signal_handlers(env, controller, monkeypatch):
    before = signal.getsignal(signal.SIGTERM)
    monkeypatch.setattr(cli, "setup_logging", lambda: None)

    def sigterm_then_return(kube, provider, stop):
        signal.raise_signal(signal.SIGTERM)
        assert stop.is_set()
        return 0

    monkeypatch.setattr(cli, "serve", sigterm_then_return)

    assert cli.main(["serve"]) == 0
    assert signal.getsignal(signal.SIGTERM) is before
