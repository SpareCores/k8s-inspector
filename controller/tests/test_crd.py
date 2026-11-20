import copy
import re
import zlib
from datetime import UTC, date, datetime, timedelta, timezone

import httpx2 as httpx
import pytest
from lightkube import ApiError

from k8s_inspector import crd
from k8s_inspector import kube as kube_mod
from k8s_inspector.kube import AlreadyExists, Conflict, NotFound, TransientApiError
from k8s_inspector.model import (
    AttemptOutcome,
    AttemptStatus,
    CapacityMode,
    CapacityType,
    RunPhase,
    RunSpec,
    RunStatus,
    TaskState,
    TaskStatus,
)

DNS_1123 = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?")
T0 = datetime(2026, 11, 20, 9, 30, tzinfo=UTC)


class FakeKube:
    """In-memory BenchmarkRun store with resourceVersion semantics and injectable conflicts."""

    namespace = "inspector"

    def __init__(self) -> None:
        self.objects: dict[str, dict] = {}
        self.version = 0
        self.conflicts_to_inject = 0
        self.patches: list[dict] = []

    def _bump(self, obj: dict) -> None:
        self.version += 1
        obj["metadata"]["resourceVersion"] = str(self.version)

    def create(self, kind: str, manifest: dict) -> dict:
        assert kind == "benchmarkrun"
        name = manifest["metadata"]["name"]
        if name in self.objects:
            raise AlreadyExists(name)
        obj = copy.deepcopy(manifest)
        obj["metadata"].update(uid=f"uid-{name}", creationTimestamp="2026-11-20T23:30:00Z")
        self._bump(obj)
        self.objects[name] = obj
        return copy.deepcopy(obj)

    def get(self, kind: str, name: str) -> dict:
        if name not in self.objects:
            raise NotFound(name)
        return copy.deepcopy(self.objects[name])

    def list(self, kind: str, labels=None) -> list[dict]:
        return [copy.deepcopy(o) for o in self.objects.values()]

    def delete(self, kind: str, name: str, grace_seconds=None) -> None:
        self.objects.pop(name, None)

    def merge_patch(self, kind: str, name: str, body: dict, subresource_status: bool = False) -> dict:
        assert subresource_status
        self.patches.append(body)
        obj = self.objects[name]
        if self.conflicts_to_inject:
            self.conflicts_to_inject -= 1
            self._bump(obj)  # somebody else wrote in between
        if body["metadata"]["resourceVersion"] != obj["metadata"]["resourceVersion"]:
            raise Conflict(name)
        status = obj.setdefault("status", {})
        for key, value in body["status"].items():  # JSON merge patch: null removes, lists replace
            if value is None:
                status.pop(key, None)
            else:
                status[key] = value
        self._bump(obj)
        return copy.deepcopy(obj)


def spec(**overrides) -> RunSpec:
    fields = {"vendor": "oci", "sku": "VM.Standard.E5.Flex.2-16", "run_id": "r20261120t0930-3f9c"}
    return RunSpec(**(fields | overrides))


# --------------------------------------------------------------------------- names


def test_run_name_documented_shape():
    name = crd.run_name("oci", "VM.Standard.E5.Flex.2-16")
    assert re.fullmatch(r"oci-vm-standard-e5-flex-2-16-[0-9a-f]{8}", name)


@pytest.mark.parametrize(
    ("vendor", "sku"),
    [
        ("oci", "VM.Standard.E5.Flex.2-16"),
        ("azure", "Standard_D2s_v5"),
        ("aws", "p5.48xlarge"),
        ("gcp", "a3-megagpu-8g"),
        ("x", "A" * 200),
        ("v" * 100, "short"),
        ("v" * 100, "s" * 100),
        ("oci", "---"),
        ("oci", ""),
        ("", "sku"),
        ("", ""),
        ("oci", "ünïcode SKU/with spaces"),
        ("oci", "-leading.and.trailing-"),
        ("oci", "a" * 53 + "-" + "b" * 20),  # truncation lands right after a dash
    ],
)
def test_run_name_is_valid_dns_1123(vendor, sku):
    name = crd.run_name(vendor, sku)
    assert len(name) <= 63
    assert DNS_1123.fullmatch(name), name
    assert "--" not in name


def test_run_name_is_pure_and_distinguishes_similar_skus():
    assert crd.run_name("oci", "A.B") == crd.run_name("oci", "A.B")
    assert crd.run_name("oci", "A.B") != crd.run_name("oci", "A_B")  # same slug, different checksum
    assert crd.run_name("oci", "x" * 100) != crd.run_name("oci", "x" * 101)  # same truncated prefix


def test_run_name_keeps_checksum_when_truncated():
    sku = "very-long-sku-" * 10
    name = crd.run_name("oci", sku)
    assert len(name) == 63
    assert name.endswith(f"-{zlib.crc32(f'oci/{sku}'.encode()):08x}")


def test_new_run_id_format():
    assert re.fullmatch(r"r20261120t0930-[0-9a-f]{4}", crd.new_run_id(T0))
    local = datetime(2026, 11, 20, 11, 30, tzinfo=timezone(timedelta(hours=2)))
    assert crd.new_run_id(local).startswith("r20261120t0930-")


def test_run_key_uses_creation_day_utc():
    kube = FakeKube()
    crd.create_run(kube, spec())
    run = crd.list_runs(kube)[0]
    assert crd.run_key(run) == crd.RunKey("oci", "VM.Standard.E5.Flex.2-16", date(2026, 11, 20), "r20261120t0930-3f9c")


# --------------------------------------------------------------------------- manifest and parsing


def test_manifest_shape():
    manifest = crd.to_manifest(
        spec(tasks=["lscpu"], force=True, capacity=CapacityMode.ON_DEMAND, hold_after=timedelta(hours=2)), "inspector"
    )
    name = crd.run_name("oci", "VM.Standard.E5.Flex.2-16")
    assert manifest["apiVersion"] == "inspector.sparecores.com/v1alpha1" and manifest["kind"] == "BenchmarkRun"
    assert manifest["metadata"]["name"] == name and manifest["metadata"]["namespace"] == "inspector"
    assert manifest["metadata"]["labels"] == {
        "inspector.sparecores.com/vendor": "oci",
        "inspector.sparecores.com/run": name,
    }
    assert manifest["spec"] == {
        "vendor": "oci",
        "sku": "VM.Standard.E5.Flex.2-16",
        "runId": "r20261120t0930-3f9c",
        "tasks": ["lscpu"],
        "force": True,
        "capacity": "OnDemand",
        "holdAfter": "2h",
    }


@pytest.mark.parametrize(
    ("delta", "text"),
    [
        (timedelta(0), "0s"),
        (timedelta(seconds=45), "45s"),
        (timedelta(minutes=90), "90m"),
        (timedelta(hours=2), "2h"),
    ],
)
def test_hold_after_round_trip(delta, text):
    manifest = crd.to_manifest(spec(hold_after=delta), "inspector")
    assert manifest["spec"]["holdAfter"] == text
    manifest["metadata"]["creationTimestamp"] = "2026-11-20T09:30:00Z"
    assert crd.from_dict(manifest).spec.hold_after == delta


def test_from_dict_tolerates_missing_status():
    manifest = crd.to_manifest(spec(database={"vendor": "aws", "id": "rds", "engineVersion": "16"}), "inspector")
    manifest["metadata"].update(creationTimestamp="2026-11-20T09:30:00Z", uid="u", resourceVersion="3")
    run = crd.from_dict(manifest)
    assert run.status == RunStatus()
    assert (run.uid, run.resource_version, run.created) == ("u", "3", T0)
    assert run.spec.database == {"vendor": "aws", "id": "rds", "engineVersion": "16"}


def full_status() -> RunStatus:
    return RunStatus(
        phase=RunPhase.RUNNING,
        progress="1/2",
        current_step="geekbench",
        attempt=1,
        capacity_type=CapacityType.SPOT,
        node="node-1",
        region="eu-frankfurt-1",
        zone="z1",
        price_per_hour="0.0337",
        variant=2,
        tasks=[
            TaskStatus(
                "lscpu",
                TaskState.DONE,
                attempt=1,
                start=T0,
                end=T0 + timedelta(minutes=1),
                exit_code=0,
                published=True,
                commit="abc",
            ),
            TaskStatus("geekbench", TaskState.RUNNING, attempt=1, start=T0, error_msg="x", gpu_fallback=True),
        ],
        attempts=[
            AttemptStatus(
                1,
                CapacityType.SPOT,
                node_claim="nc-1",
                node="node-1",
                start=T0,
                end=T0,
                outcome=AttemptOutcome.SPOT_INTERRUPTED,
            ),
            AttemptStatus(2, CapacityType.ON_DEMAND),
        ],
        conditions=[{"type": "Ready", "status": "True"}],
    )


def test_status_round_trip():
    status = full_status()
    d = crd.status_to_dict(status)
    assert d["currentStep"] == "geekbench" and d["tasks"][0]["exitCode"] == 0
    assert d["tasks"][0]["start"] == "2026-11-20T09:30:00Z"
    assert d["attempts"][0]["outcome"] == "SpotInterrupted" and d["attempts"][0]["nodeClaim"] == "nc-1"
    obj = {
        "metadata": {"name": "n", "creationTimestamp": "2026-11-20T09:30:00Z"},
        "spec": crd.to_manifest(spec(), "i")["spec"],
        "status": d,
    }
    assert crd.from_dict(obj).status == status


def test_status_to_dict_unset_fields_are_null_for_merge_patch():
    d = crd.status_to_dict(RunStatus())
    assert d["node"] is None and d["currentStep"] is None and d["phase"] == "Pending"


# --------------------------------------------------------------------------- API helpers


def test_create_run_sets_pending_status_and_second_create_raises_run_exists():
    kube = FakeKube()
    run = crd.create_run(kube, spec())
    assert run.status.phase == RunPhase.PENDING and run.status.progress == "0/0"
    assert kube.objects[run.name]["status"]["phase"] == "Pending"
    with pytest.raises(crd.RunExists):
        crd.create_run(kube, spec(run_id="r20261120t1000-aaaa"))


def sleepless(monkeypatch):
    monkeypatch.setattr(kube_mod.time, "sleep", lambda seconds: None)


def transient_error():
    return ApiError(response=httpx.Response(503, json={"error": "x"}))


def test_create_run_survives_a_lost_create_response(monkeypatch):
    sleepless(monkeypatch)

    class LostResponse(FakeKube):
        lost = True

        def create(self, kind, manifest):
            created = super().create(kind, manifest)
            if self.lost:
                self.lost = False
                raise transient_error()
            return created

    kube = LostResponse()
    run = crd.create_run(kube, spec())
    assert run.name == crd.run_name("oci", "VM.Standard.E5.Flex.2-16")
    assert kube.objects[run.name]["status"]["phase"] == "Pending"


def test_create_run_retries_the_initial_status_update(monkeypatch):
    sleepless(monkeypatch)

    class FlakyPatch(FakeKube):
        failures = 1

        def merge_patch(self, *args, **kwargs):
            if self.failures:
                self.failures -= 1
                raise transient_error()
            return super().merge_patch(*args, **kwargs)

    kube = FlakyPatch()
    assert crd.create_run(kube, spec()).status.phase == RunPhase.PENDING


def test_create_run_gives_up_after_repeated_transient_errors(monkeypatch):
    sleepless(monkeypatch)

    class Down(FakeKube):
        def create(self, kind, manifest):
            raise transient_error()

    with pytest.raises(TransientApiError):
        crd.create_run(Down(), spec())


def test_get_list_delete():
    kube = FakeKube()
    run = crd.create_run(kube, spec())
    assert crd.get_run(kube, run.name).spec.sku == "VM.Standard.E5.Flex.2-16"
    assert [r.name for r in crd.list_runs(kube)] == [run.name]
    crd.delete_run(kube, run.name)
    crd.delete_run(kube, run.name)  # idempotent
    with pytest.raises(NotFound):
        crd.get_run(kube, run.name)


def test_update_status_mutates_and_refreshes_progress():
    kube = FakeKube()
    run = crd.create_run(kube, spec())

    def mutate(status: RunStatus) -> None:
        status.phase = RunPhase.RUNNING
        status.tasks = [TaskStatus("a", TaskState.DONE), TaskStatus("b", TaskState.RUNNING), TaskStatus("c")]

    updated = crd.update_status(kube, run.name, mutate)
    assert updated.status.phase == RunPhase.RUNNING and updated.status.progress == "1/3"
    assert crd.get_run(kube, run.name).status.progress == "1/3"


def test_update_status_clears_fields_set_to_none():
    kube = FakeKube()
    run = crd.create_run(kube, spec())
    crd.update_status(kube, run.name, lambda s: setattr(s, "node", "n1"))
    assert crd.get_run(kube, run.name).status.node == "n1"
    crd.update_status(kube, run.name, lambda s: setattr(s, "node", None))
    assert crd.get_run(kube, run.name).status.node is None


def test_update_status_retries_on_conflict_and_reapplies_mutation():
    kube = FakeKube()
    run = crd.create_run(kube, spec())
    kube.conflicts_to_inject = 2
    calls = []

    def mutate(status: RunStatus) -> None:
        calls.append(1)
        status.attempt += 1

    updated = crd.update_status(kube, run.name, mutate)
    assert len(calls) == 3
    assert updated.status.attempt == 1  # each retry starts from fresh state, not the failed attempt's


def test_update_status_gives_up_after_five_conflicts():
    kube = FakeKube()
    run = crd.create_run(kube, spec())
    kube.conflicts_to_inject = 100
    patches_before = len(kube.patches)
    with pytest.raises(Conflict):
        crd.update_status(kube, run.name, lambda s: None)
    assert len(kube.patches) - patches_before == 5


def test_update_status_missing_run_raises_not_found():
    with pytest.raises(NotFound):
        crd.update_status(FakeKube(), "nope", lambda s: None)
