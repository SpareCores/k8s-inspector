"""Rendering into the sc-inspector-data tree. Golden files are real sc-inspector-data files (hcloud/cax31 variant 0,
aws/t4g.medium stressnglongrun truncated to three samples, azure metas): the rendered files must equal them."""

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from k8s_inspector import render
from k8s_inspector.model import (
    AttemptStatus,
    BenchmarkRun,
    CapacityType,
    Manifest,
    ManifestObject,
    NodeFacts,
    RunKey,
    RunSpec,
    RunStatus,
    TaskDef,
    TaskOutcome,
    TaskState,
    TaskStatus,
    Verification,
)

FIXTURES = Path(__file__).parent / "fixtures" / "publish"
GOLDEN = FIXTURES / "hcloud_cax31" / "0"
RUN = RunKey("hcloud", "cax31", datetime(2026, 9, 29).date(), "r20260929t1257-abcd")


def task_def(name: str, **kwargs) -> TaskDef:
    return TaskDef(name=name, image="img:main", command=None, timeout=timedelta(minutes=30), **kwargs)


def golden_input(name: str, task: TaskDef, root: Path = GOLDEN, **status_extra) -> render.RenderInput:
    """RenderInput whose status.json is the golden legacy meta (as the runner would have written it)."""
    meta = json.loads((root / name / "meta.json").read_text())
    meta.pop("outputs")
    read = lambda n: (root / name / n).read_bytes() if (root / name / n).exists() else b""  # noqa: E731
    return render.RenderInput(
        run=RUN,
        task=task,
        key=name,
        status={**meta, "run_id": RUN.run_id, **status_extra},
        stdout=read("stdout"),
        stderr=read("stderr"),
    )


def tree_file(tree: Path, task: str, name: str, variant: int = 0) -> Path:
    return tree / "data" / "hcloud" / "cax31" / str(variant) / task / name


def assert_same_file(tree: Path, task: str, name: str, root: Path = GOLDEN) -> None:
    assert tree_file(tree, task, name).read_bytes() == (root / task / name).read_bytes()


# ---------------------------------------------------------------- meta.json


@pytest.mark.parametrize(
    "path",
    [
        FIXTURES / "metas" / "azure_precheck_minus3.json",  # has region
        FIXTURES / "metas" / "azure_no_node_minus1.json",  # quotes in error_msg, no region key
        GOLDEN / "geekbench" / "meta.json",
    ],
)
def test_dump_meta_equals_legacy_bytes(path):
    text = path.read_text()
    meta = json.loads(text)
    dumped = render.dump_meta(meta)
    if "region" in meta:
        assert dumped == text
    else:  # the legacy `Meta` model writes `spot` and `region` always, older files lack them
        assert dumped.startswith(text[: text.index('"outputs"')])
        assert {k: v for k, v in json.loads(dumped).items() if k in meta} == meta


def test_status_meta_roundtrip_has_legacy_fields_first_then_added():
    meta = render.meta_from_status({"start": "2026-01-01T00:00:00", "exit_code": 0, "pod": "ignored", "run_id": "r1"})
    assert list(meta) == list(render.LEGACY_META_FIELDS) + list(render.ADDED_META_FIELDS)
    assert meta["outputs"] == [] and meta["start_retries"] == 0 and meta["run_id"] == "r1" and "pod" not in meta
    text = render.dump_meta(meta)
    assert text.startswith('{"start":"2026-01-01T00:00:00","end":null,"exit_code":0,')
    assert not text.endswith("\n")


def test_iso_is_legacy_naive_utc_and_omits_zero_fraction():
    micro = datetime(2026, 11, 20, 12, 0, 44, 123456, tzinfo=UTC)
    assert render._iso(micro) == "2026-11-20T12:00:44.123456"
    assert render._iso(micro.replace(microsecond=0)) == "2026-11-20T12:00:44"  # as Python's isoformat() does
    assert render._iso(None) is None


def test_build_status():
    t0 = datetime(2026, 11, 20, 9, 30, 0, 123456, tzinfo=UTC)
    run = BenchmarkRun(
        name="n",
        namespace="inspector",
        uid="u",
        resource_version="1",
        created=t0,
        spec=RunSpec(vendor="oci", sku="VM.Standard.E5.Flex.2-16", run_id="r20261120t0930-3f9c"),
        status=RunStatus(
            region="eu-frankfurt-1",
            zone="AD-1",
            price_per_hour="0.0337",
            attempts=[
                AttemptStatus(n=1, capacity_type=CapacityType.SPOT),
                AttemptStatus(n=2, capacity_type=CapacityType.SPOT),
            ],
        ),
    )
    task = TaskStatus(
        name="geekbench", state=TaskState.DONE, attempt=1, start=t0, end=t0 + timedelta(hours=1), exit_code=-4,
        error_msg="SPOT_INTERRUPTED",
    )  # fmt: skip
    manifest = Manifest("geekbench", True, (ManifestObject("stdout", "put", 10), ManifestObject("stderr", "put", 0)))
    node = NodeFacts("node-1", "oci.VM", "amd64", t0, 1, node_os="Ubuntu", kubelet="v1.34", runtime="containerd://2.1")
    status = render.build_status(
        run,
        task,
        task_hash="h",
        outcome=TaskOutcome("geekbench", -4, version="6.7", kernel_version="6.8", nice=-20),
        verification=Verification("geekbench", True, manifest),
        node=node,
        attempt=run.status.attempts[0],
        image="ghcr.io/x@sha256:1",
        pod="p-a1-07-geekbench",
    )
    assert status["start"] == "2026-11-20T09:30:00.123456" and status["end"] == "2026-11-20T10:30:00.123456"
    assert (status["stdout_bytes"], status["stderr_bytes"], status["version"]) == (10, 0, "6.7")
    assert (status["spot"], status["capacity_type"], status["spot_attempts"]) == (True, "spot", 1)
    assert (status["region"], status["zone"], status["price"]) == ("eu-frankfurt-1", "AD-1", "0.0337")
    assert (status["node"], status["reason"], status["upload_ok"], status["pod"]) == (
        "node-1", "SPOT_INTERRUPTED", True, "p-a1-07-geekbench",
    )  # fmt: skip
    assert status["runtime"] == "containerd://2.1" and status["image"].endswith("sha256:1")
    assert status["nice"] == -20
    meta = render.meta_from_status(status)
    assert meta["run_id"] == "r20261120t0930-3f9c" and "pod" not in meta and meta["exit_code"] == -4
    assert meta["nice"] == -20
    json.dumps(status)  # JSON-serialisable


# ---------------------------------------------------------------- golden task renders


def test_raw_geekbench_and_meta(tmp_path):
    inp = golden_input("geekbench", task_def("geekbench", transforms=("raw", "fetch_geekbench_results")))
    written = render.render_task(tmp_path, "hcloud", "cax31", 0, inp)
    for name in ("stdout", "stderr", "results.json"):
        assert_same_file(tmp_path, "geekbench", name)
    golden = (GOLDEN / "geekbench" / "meta.json").read_text()
    ours = tree_file(tmp_path, "geekbench", "meta.json").read_text()
    # legacy fields identical (the golden predates `region`), added fields appended, outputs derived
    ours_meta, golden_meta = json.loads(ours), json.loads(golden)
    assert {k: ours_meta[k] for k in golden_meta} == golden_meta
    assert ours_meta["outputs"] == ["stdout", "stderr", "results.json"] and ours_meta["run_id"] == RUN.run_id
    assert ours.startswith(golden[:-1].rsplit(',"spot"', 1)[0])
    assert tree_file(tmp_path, "geekbench", "meta.json") in written


def test_openssl_and_stressnglongrun_parsers(tmp_path):
    render.render_task(tmp_path, "hcloud", "cax31", 0, golden_input("openssl", task_def("openssl", parse="openssl")))
    assert_same_file(tmp_path, "openssl", "parsed.json")
    stress_root = FIXTURES / "stressnglongrun_t4g" / "0"
    inp = golden_input("stressnglongrun", task_def("stressnglongrun", parse="stressnglongrun"), stress_root)
    render.render_task(tmp_path, "hcloud", "cax31", 0, inp)
    assert_same_file(tmp_path, "stressnglongrun", "parsed.csv", stress_root)  # CRLF rows like the csv module


def test_dmidecode_parser(tmp_path):
    for module in ("dmiparser", "dateparser", "fastnumbers"):
        pytest.importorskip(module)
    inp = golden_input("dmidecode", task_def("dmidecode", parse="dmidecode"))
    render.render_task(tmp_path, "hcloud", "cax31", 0, inp)
    assert_same_file(tmp_path, "dmidecode", "parsed.json")


def test_lstopo_style_matches_legacy():
    raw = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="10"><rect class="PU" x="0"/>'
        '<text class="L2">a &amp; b</text><line x1="0"/></svg>'
    )
    # output of sc-inspector's parse.style_lstopo_svg_file for the same input
    expected = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="10"><style>\n'
        "        text { pointer-events: none; font-family: ui-monospace, monospace; }\n"
        "        rect { transition: stroke-width 0.15s ease, filter 0.15s ease; }\n"
        "        rect:hover {\n"
        "            stroke: #a7f3d0;\n"
        "            stroke-width: 2;\n"
        "            filter: brightness(1.2);\n"
        "            cursor: pointer;\n"
        "        }\n"
        '    </style><rect class="PU" x="0" stroke="#34d399" fill="#ffffff" data-description="Processing unit" />'
        '<text class="L2" font-family="ui-monospace, monospace" fill="#ffffff">a &amp; b</text>'
        '<line x1="0" stroke="#34d399" /></svg>'
    )
    assert render._style_lstopo_svg(raw) == expected


@pytest.mark.skipif(shutil.which("lstopo") is None, reason="needs the hwloc lstopo binary")
def test_lstopo_renders_styled_svg(tmp_path):
    # The exact SVG depends on the hwloc version (the golden file is from the sc-inspector image).
    inp = golden_input("lstopo", task_def("lstopo", parse="lstopo"))
    render.render_task(tmp_path, "hcloud", "cax31", 0, inp)
    svg = tree_file(tmp_path, "lstopo", "lstopo.svg").read_text()
    assert svg.startswith("<svg") and "ui-monospace" in svg and "pointer-events: none" in svg


def test_geekbench_html_fallback(monkeypatch):
    pytest.importorskip("lxml")
    rows = "".join(
        f'<tr><td class="name">{n}</td><td class="score">{i}<span class="description">{i} MB/sec</span></td></tr>'
        for i, n in enumerate(render.GEEKBENCH_BENCHMARKS, start=1)
    )
    page = "".join(
        f'<div class="heading"><h3>{t}</h3></div><div class="table-wrapper"><table>'
        f'<tr><th class="score"> 1234 </th></tr>{rows}</table></div>'
        for t in render.GEEKBENCH_TABLES
    )
    monkeypatch.setattr(render, "_fetch_text", lambda url: f"<html><body>{page}</body></html>")
    out = render.geekbench_results(b'see "https://browser.geekbench.com/v6/cpu/123" ok', b"no document")
    results = json.loads(out)
    assert results["Single-Core Performance"]["Score"] == {"score": 1234}
    assert results["Multi-Core Performance"]["Clang"] == {"score": 6, "description": "6 MB/sec"}
    assert render.geekbench_results(b"no url", b"") is None


# ---------------------------------------------------------------- policy: failures, stale, schedule, variants


def plain_input(task="lscpu", exit_code=0, start="2026-10-01T10:00:00", run_id="r1", stdout=b"out", **extra):
    status = {"start": start, "end": "2026-10-01T10:05:00", "exit_code": exit_code, "task_hash": "h", "run_id": run_id}
    return render.RenderInput(
        run=RUN, task=task_def(task), key=task, status={**status, **extra}, stdout=stdout, stderr=b""
    )


def read_meta(tree: Path, task: str, variant: int | str = 0) -> dict:
    return json.loads((tree / "data" / "hcloud" / "cax31" / str(variant) / task / "meta.json").read_text())


def test_empty_streams_not_written_and_stale_streams_removed(tmp_path):
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input())
    assert tree_file(tmp_path, "lscpu", "stdout").read_bytes() == b"out"
    assert not tree_file(tmp_path, "lscpu", "stderr").exists()
    assert read_meta(tmp_path, "lscpu")["outputs"] == ["stdout"]
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-02T10:00:00", stdout=b""))
    assert not tree_file(tmp_path, "lscpu", "stdout").exists() and read_meta(tmp_path, "lscpu")["outputs"] == []


def test_harness_error_writes_meta_only(tmp_path):
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(exit_code=256, stdout=b"partial"))
    assert read_meta(tmp_path, "lscpu")["outputs"] == [] and not tree_file(tmp_path, "lscpu", "stdout").exists()


def test_stale_duplicate_is_skipped_and_same_start_rewrites(tmp_path):
    assert render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-02T10:00:00"))
    assert render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-01T10:00:00")) == []
    assert read_meta(tmp_path, "lscpu")["start"] == "2026-10-02T10:00:00"
    assert render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-02T10:00:00"))


@pytest.mark.parametrize("exit_code", [1, -4, 137])
def test_failure_does_not_replace_a_good_result(tmp_path, exit_code):
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-01T10:00:00", stdout=b"good"))
    failed = plain_input(start="2026-10-02T10:00:00", run_id="r2", exit_code=exit_code, stdout=b"partial")
    assert render.render_task(tmp_path, "hcloud", "cax31", 0, failed) == []
    assert tree_file(tmp_path, "lscpu", "stdout").read_bytes() == b"good"
    assert read_meta(tmp_path, "lscpu")["run_id"] == "r1"
    assert read_meta(tmp_path, "lscpu", "schedule")["run_id"] == "r1"


def test_failure_of_a_changed_task_goes_to_the_schedule_only(tmp_path):
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-01T10:00:00", stdout=b"good"))
    failed = plain_input(start="2026-10-02T10:00:00", run_id="r2", exit_code=137, stdout=b"partial")
    failed.status["task_hash"] = "h2"
    sched = tmp_path / "data/hcloud/cax31/schedule/lscpu/meta.json"
    assert render.render_task(tmp_path, "hcloud", "cax31", 0, failed) == [sched]
    assert (
        tree_file(tmp_path, "lscpu", "stdout").read_bytes() == b"good"
        and read_meta(tmp_path, "lscpu")["run_id"] == "r1"
    )
    assert read_meta(tmp_path, "lscpu", "schedule")["run_id"] == "r2"
    assert read_meta(tmp_path, "lscpu", "schedule")["exit_code"] == 137


def test_failure_replaces_a_failure_and_success_replaces_a_failure(tmp_path):
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-01T10:00:00", exit_code=1))
    render.render_task(
        tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-02T10:00:00", exit_code=2, run_id="r2")
    )
    assert read_meta(tmp_path, "lscpu")["exit_code"] == 2
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-03T10:00:00", run_id="r3"))
    assert read_meta(tmp_path, "lscpu")["exit_code"] == 0


def test_schedule_is_the_union_across_variants(tmp_path):
    render.render_task(tmp_path, "hcloud", "cax31", 0, plain_input(start="2026-10-01T10:00:00", run_id="r1"))
    failed = plain_input(start="2026-10-05T10:00:00", run_id="r2", exit_code=1)
    render.render_task(tmp_path, "hcloud", "cax31", 1, failed)
    # current-hash success beats the newer failure
    assert read_meta(tmp_path, "lscpu", "schedule")["run_id"] == "r1"
    assert read_meta(tmp_path, "lscpu", 1)["exit_code"] == 1
    render.render_task(tmp_path, "hcloud", "cax31", 1, plain_input(start="2026-10-06T10:00:00", run_id="r3"))
    assert read_meta(tmp_path, "lscpu", "schedule")["run_id"] == "r3"


def test_golden_schedule_meta_is_what_the_union_picks(tmp_path):
    inst = tmp_path / "data" / "hcloud" / "cax31"
    shutil.copytree(GOLDEN, inst / "0")
    golden = (FIXTURES / "hcloud_cax31" / "schedule" / "lscpu" / "meta.json").read_text()
    th = json.loads(golden)["task_hash"]
    path = render.refresh_schedule_meta(inst, "lscpu", th)
    # legacy rewrote the schedule meta through its model: the only difference is the then-missing `region`
    assert path.read_text() == golden[:-1] + ',"region":null}'
    assert render.refresh_schedule_meta(inst, "no_such_task", th) is None


def test_variantless_result_goes_to_schedule_only(tmp_path):
    inp = plain_input(exit_code=-1, error_msg="SCARPENTER_ICE: no capacity", stdout=b"")
    written = render.render_task(tmp_path, "hcloud", "cax31", None, inp)
    sched = tmp_path / "data/hcloud/cax31/schedule/lscpu/meta.json"
    assert written == [sched] and not (tmp_path / "data/hcloud/cax31/0").exists()
    assert json.loads(sched.read_text())["exit_code"] == -1


def test_start_retries_count_minus_four_and_become_minus_five(tmp_path):
    def attempt(n: int, **kw):
        inp = plain_input(exit_code=-4, start=f"2026-10-0{n}T10:00:00", run_id=f"r{n}", error_msg="NODE_LOST", **kw)
        render.render_task(tmp_path, "hcloud", "cax31", None, inp)
        return json.loads((tmp_path / "data/hcloud/cax31/schedule/lscpu/meta.json").read_text())

    assert attempt(1)["start_retries"] == 1
    assert attempt(1)["start_retries"] == 1  # re-render of the same run after a crash: not counted twice
    assert attempt(2)["start_retries"] == 2
    last = attempt(3)
    assert (last["start_retries"], last["exit_code"], last["error_msg"]) == (3, -5, "exceeded 3 start retries")
    ok = plain_input(exit_code=0, start="2026-10-04T10:00:00", run_id="r4")
    render.render_task(tmp_path, "hcloud", "cax31", None, ok)
    assert json.loads((tmp_path / "data/hcloud/cax31/schedule/lscpu/meta.json").read_text())["start_retries"] == 0


def test_start_retries_reset_on_a_changed_hash_and_on_other_failures(tmp_path):
    def attempt(n: int, exit_code: int = -4, task_hash: str = "h"):
        inp = plain_input(exit_code=exit_code, start=f"2026-10-0{n}T10:00:00", run_id=f"r{n}", task_hash=task_hash)
        render.render_task(tmp_path, "hcloud", "cax31", None, inp)
        return json.loads((tmp_path / "data/hcloud/cax31/schedule/lscpu/meta.json").read_text())["start_retries"]

    assert attempt(1) == 1 and attempt(2) == 2
    assert attempt(3, task_hash="new") == 1  # the task changed: a fresh count
    assert attempt(4, exit_code=1, task_hash="new") == 0  # a task failure is not a start failure
    assert attempt(5, task_hash="new") == 1  # not carried over the failure


def test_failing_parser_leaves_the_tree_untouched(tmp_path):
    inp = plain_input("stressnglongrun", stdout=b"x", stderr=b"not yaml")
    inp.stderr = b"not yaml"
    inp.task = task_def("stressnglongrun", parse="stressnglongrun")
    with pytest.raises(AttributeError):  # re.search found nothing
        render.render_task(tmp_path, "hcloud", "cax31", 0, inp)
    assert not any(tmp_path.rglob("*"))


def test_render_variant_and_timing(tmp_path):
    render.render_variant(
        tmp_path, "hcloud", "cax31", 0, (GOLDEN / "lscpu" / "stdout").read_bytes(),
        first_seen="2026-09-29T13:00:41.049769", lscpu_version="2.39.3",
    )  # fmt: skip
    ours = tmp_path / "data/hcloud/cax31/0/variant.json"
    golden = json.loads((GOLDEN / "variant.json").read_text())
    assert json.loads(ours.read_text()) == {**golden, "source": "observed"}
    before = ours.read_text()
    render.render_variant(tmp_path, "hcloud", "cax31", 0, b"[]")  # exists: untouched
    assert ours.read_text() == before

    t = datetime(2026, 9, 29, 12, 57, 13, 999, tzinfo=UTC)
    render.render_timing(tmp_path, "hcloud", "cax31", 0, RUN, {"api_start": render.format_timing_utc(t)})
    got = (tmp_path / "data/hcloud/cax31/0/timing" / RUN.run_id / "api_start").read_bytes()
    assert got == (GOLDEN / "timing/36570983419/api_start").read_bytes() == b"2026-09-29T12:57:13Z\n"


def test_dbaas_layout(tmp_path):
    db = {"vendor": "ovh", "id": "postgresql-discovery-b3-4", "engineVersion": "18"}
    inp = plain_input("pgbench_postgres_dbaas_ro_durable")
    written = render.render_dbaas_task(tmp_path, db, inp)
    base = tmp_path / "dbaas/ovh/postgresql-discovery-b3-4/postgres/18/standalone/pgbench_postgres_dbaas_ro_durable"
    assert sorted(p.name for p in written) == ["meta.json", "stdout"] and (base / "stdout").read_bytes() == b"out"
