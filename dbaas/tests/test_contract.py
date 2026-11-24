import json
import os
import signal
import subprocess
import sys
import types
from pathlib import Path

import pulumi
import pytest

from k8s_inspector_dbaas import __main__ as entry
from k8s_inspector_dbaas import stack, vendors
from k8s_inspector_dbaas.spec import DbOutputs, load_spec

SPEC_JSON = {
    "vendor": "fake", "databaseId": "db-8", "engineVersion": 18, "region": "eu-test-1", "zone": None,
    "run": "fake-x-1", "runId": "r1", "expiresAt": "2026-11-20T16:00:00Z", "labels": {"sc_inspector_run": "r1"},
    "createTimeoutMinutes": 50, "deleteTimeoutMinutes": 100, "network": {"vpc": "v-1"},
    "record": {"vcpus": 4, "memory_amount": 16384},
}  # fmt: skip


def test_the_spec_comes_from_the_environment():
    spec = load_spec({"DBAAS_SPEC": json.dumps(SPEC_JSON), "DBAAS_PASSWORD": "pw"})
    assert (spec.vendor, spec.engine_version, spec.zone, spec.network) == ("fake", "18", None, {"vpc": "v-1"})
    assert (spec.vcpus, spec.memory_gib, spec.password) == (4, 16.0, "pw")
    assert (spec.create_timeout, spec.delete_timeout) == ("50m", "100m")
    assert "pw" not in repr(spec)


def test_a_spec_without_a_required_key_is_refused():
    del SPEC_JSON["run"]
    try:
        with pytest.raises(KeyError):
            load_spec({"DBAAS_SPEC": json.dumps(SPEC_JSON)})
    finally:
        SPEC_JSON["run"] = "fake-x-1"


def test_a_vendor_module_is_found_by_its_id():
    assert vendors.load("fake").provision
    with pytest.raises(ValueError, match="no DBaaS module"):
        vendors.load("nosuchvendor")
    with pytest.raises(ValueError, match="invalid vendor"):
        vendors.load("../etc")


def test_the_fake_vendor_points_at_the_network_it_is_given(spec):
    out = vendors.load("fake").provision(spec)
    assert out.host == "fake-m5-large-1234abcd.fake.invalid" and out.port == 5432 and out.sslmode == "prefer"
    spec = type(spec)(**{**spec.__dict__, "network": {"host": "10.0.0.7", "admin_db": "defaultdb"}})
    out = vendors.load("fake").provision(spec)
    assert (out.host, out.admin_db) == ("10.0.0.7", "defaultdb")


def test_the_outputs_round_trip_through_the_stack_exports():
    out = DbOutputs(host="h", admin_user="u", admin_db="d", user="w", grant_role="r")
    assert DbOutputs.from_exports(json.loads(out.to_json())) == out
    assert "grant_role" not in DbOutputs(host="h", admin_user="u", admin_db="d").to_json()


def test_the_program_exports_the_outputs_of_the_vendor_module(spec, monkeypatch):
    exported = {}
    monkeypatch.setattr(pulumi, "export", exported.__setitem__)
    stack._program(spec)()
    assert exported["host"].endswith(".fake.invalid") and exported["port"] == 5432 and "user" not in exported


def test_the_stack_name_follows_from_the_run_name():
    assert stack.stack_name("aws-m5-large-1234abcd") == "k8s-dbaas-aws-m5-large-1234abcd"
    assert stack.STACK_PREFIX == "k8s-dbaas-"


def test_up_reports_the_connection_facts_in_the_termination_message(spec, tmp_path, monkeypatch):
    log = tmp_path / "termination"
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(log))
    monkeypatch.setenv("DBAAS_SPEC", json.dumps(SPEC_JSON))
    monkeypatch.setenv("DBAAS_PASSWORD", "pw")
    monkeypatch.setattr(stack, "up", lambda spec: DbOutputs(host="h", admin_user="u", admin_db="d"))
    assert entry.run("up") == 0
    assert json.loads(log.read_text()) == {
        "host": "h",
        "admin_user": "u",
        "admin_db": "d",
        "port": 5432,
        "sslmode": "require",
    }


def test_a_vendor_generated_password_is_reported_in_the_termination_message(spec, tmp_path, monkeypatch):
    log = tmp_path / "termination"
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(log))
    monkeypatch.setenv("DBAAS_SPEC", json.dumps(SPEC_JSON))
    monkeypatch.setenv("DBAAS_PASSWORD", "run-pw")
    out = DbOutputs(host="h", admin_user="u", admin_db="d", password="Generated-by-vendor")
    monkeypatch.setattr(stack, "up", lambda spec: out)
    assert entry.run("up") == 0
    assert json.loads(log.read_text())["password"] == "Generated-by-vendor"
    assert "password" not in repr(out)


def test_the_password_is_exported_as_a_secret(monkeypatch):
    exported = {}
    monkeypatch.setattr(pulumi, "export", exported.__setitem__)
    monkeypatch.setattr(pulumi.Output, "secret", staticmethod(lambda value: ("secret", value)))
    DbOutputs(host="h", admin_user="u", admin_db="d", password="x").export()
    assert exported["password"] == ("secret", "x") and exported["host"] == "h"


def test_the_spec_has_the_client_region_and_addresses():
    spec = load_spec({"DBAAS_SPEC": json.dumps(SPEC_JSON)})
    assert spec.client_region == "eu-test-1" and spec.client_addresses == []  # default: the database's region
    data = {**SPEC_JSON, "clientRegion": "eu-test-2", "clientAddresses": ["203.0.113.7/32"]}
    spec = load_spec({"DBAAS_SPEC": json.dumps(data)})
    assert (spec.region, spec.client_region, spec.client_addresses) == ("eu-test-1", "eu-test-2", ["203.0.113.7/32"])


def test_a_failed_up_reports_the_error_without_the_password(tmp_path, monkeypatch, capsys):
    log = tmp_path / "termination"
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(log))
    monkeypatch.setenv("DBAAS_SPEC", json.dumps(SPEC_JSON))
    monkeypatch.setenv("DBAAS_PASSWORD", "hunter2")

    def failing(spec):
        raise RuntimeError("provider said: password hunter2 is too weak")

    monkeypatch.setattr(stack, "up", failing)
    assert entry.run("up") == 1
    assert "hunter2" not in log.read_text() and "***" in json.loads(log.read_text())["error"]
    assert "hunter2" not in capsys.readouterr().err


def test_a_long_failure_keeps_the_error_lines_and_the_tail(tmp_path, monkeypatch):
    log = tmp_path / "termination"
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(log))
    monkeypatch.setenv("DBAAS_SPEC", json.dumps(SPEC_JSON))
    noise = "\n".join(f' +  step {i} "creating" (0s)\n@ updating....' for i in range(200))
    cause = " +  vultr:index:Database db error: plan 'x' is not available in ams"
    output = f"{noise}\n{cause}\n{noise}\nRuntimeError: the end"

    def failing(spec):
        raise RuntimeError(output)

    monkeypatch.setattr(stack, "up", failing)
    assert entry.run("up") == 1
    text = log.read_text()
    assert len(text) <= entry.MAX_MESSAGE
    error = json.loads(text)["error"]
    assert "plan 'x' is not available in ams" in error and error.endswith("RuntimeError: the end")


def test_down_needs_only_the_run_name_and_the_vendor(monkeypatch, tmp_path):
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(tmp_path / "termination"))
    destroyed = []
    monkeypatch.setenv("DBAAS_RUN", "fake-x-1")
    monkeypatch.setenv("DBAAS_VENDOR", "fake")
    monkeypatch.setattr(stack, "down", lambda run, vendor: destroyed.append((run, vendor)))
    assert entry.run("down") == 0 and destroyed == [("fake-x-1", "fake")]


def test_a_failed_down_reports_why_in_the_termination_message(monkeypatch, tmp_path):
    log = tmp_path / "termination"
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(log))
    monkeypatch.setenv("DBAAS_RUN", "fake-x-1")
    monkeypatch.setenv("DBAAS_VENDOR", "fake")
    monkeypatch.setattr(stack, "down", lambda run, vendor: 1 / 0)
    assert entry.run("down") == 1
    assert "division by zero" in json.loads(log.read_text())["error"]


def test_the_inventory_reports_the_tagged_databases_oldest_first(monkeypatch, tmp_path):
    log = tmp_path / "termination"
    listed = [
        {"id": f"db-{i}", "run": "r", "expires_at": "2026-11-20T16:00:00Z", "created_at": f"2026-11-20T0{i}:00:00Z"}
        for i in range(9, 0, -1)
    ] + [{"id": "db-0", "run": "r", "expires_at": "x", "created_at": "2026-11-19T00:00:00Z"}]
    monkeypatch.setenv(entry.TERMINATION_LOG_ENV, str(log))
    monkeypatch.setenv("DBAAS_VENDOR", "fake")
    monkeypatch.setenv("FAKE_DBAAS_INVENTORY", json.dumps(listed))
    assert entry.run("inventory") == 0
    message = json.loads(log.read_text())
    assert message["count"] == 10 and message["oldest_created_at"] == "2026-11-19T00:00:00Z"
    assert [d["id"] for d in message["databases"]][:2] == ["db-0", "db-1"] and len(message["databases"]) == 10
    monkeypatch.delenv("FAKE_DBAAS_INVENTORY")
    assert entry.run("inventory") == 0 and json.loads(log.read_text())["count"] == 0


def test_sigterm_goes_to_the_pulumi_cli_as_sigint(tmp_path):
    """A child that notes SIGINT stands for the CLI. The handler is the one `main` installs."""
    seen = tmp_path / "seen"
    code = (
        "import signal, sys, time\n"
        f"seen = {str(seen)!r}\n"
        "def noted(*args):\n"
        "    open(seen, 'w').write('int')\n"
        "    sys.exit(0)\n"
        "signal.signal(signal.SIGINT, noted)\n"
        "print('up', flush=True)\n"
        "time.sleep(30)\n"
    )
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE)
    try:
        child.stdout.readline()  # the child has installed its handler
        assert child.pid in entry._children()
        entry.interrupt_children(signal.SIGTERM, None)
        child.wait(timeout=10)
        assert seen.read_text() == "int"
    finally:
        child.kill()
        child.wait()


def test_sigterm_without_a_running_cli_exits():
    with pytest.raises(SystemExit):
        entry.interrupt_children(signal.SIGTERM, None)


def test_the_state_profile_is_written_to_a_file_that_the_aws_config_names(monkeypatch):
    monkeypatch.delenv("AWS_CONFIG_FILE", raising=False)
    entry.use_state_credentials()
    assert "AWS_CONFIG_FILE" not in os.environ
    monkeypatch.setenv("K8SI_STATE_AWS_CONFIG", "[profile k8si-state]\nrole_arn = r\n")
    entry.use_state_credentials()
    try:
        assert Path(os.environ["AWS_CONFIG_FILE"]).read_text() == "[profile k8si-state]\nrole_arn = r\n"
    finally:
        os.unlink(os.environ["AWS_CONFIG_FILE"])
        monkeypatch.delenv("AWS_CONFIG_FILE")


class FakeStack:
    def __init__(self, pending):
        self.pending, self.calls = pending, []
        self.workspace = types.SimpleNamespace(remove_stack=lambda name: self.calls.append(("remove", name)))

    def cancel(self):
        self.calls.append("cancel")

    def export_stack(self):
        return types.SimpleNamespace(deployment={"pending_operations": self.pending})

    def destroy(self, **kwargs):
        self.calls.append(("destroy", kwargs["refresh"]))


def test_down_refuses_a_state_with_pending_operations_and_keeps_it(monkeypatch):
    pending = [{"type": "creating", "resource": {"urn": "urn:x::db"}}]
    fake = FakeStack(pending)
    monkeypatch.setenv("PULUMI_BACKEND_URL", "file:///x")
    monkeypatch.setattr(stack.auto, "select_stack", lambda *a, **k: fake)
    with pytest.raises(stack.PendingOperations, match=r"creating urn:x::db.*Operator action.*state is kept"):
        stack.down("fake-x-1", "fake")
    assert fake.calls == ["cancel"]  # no destroy, no remove


def test_down_destroys_with_refresh_and_removes_the_state_when_nothing_is_pending(monkeypatch):
    fake = FakeStack([])
    monkeypatch.setenv("PULUMI_BACKEND_URL", "file:///x")
    monkeypatch.setattr(stack.auto, "select_stack", lambda *a, **k: fake)
    stack.down("fake-x-1", "fake")
    assert fake.calls == ["cancel", ("destroy", True), ("remove", "k8s-dbaas-fake-x-1")]


def test_down_runs_the_vendors_cleanup_with_the_run_name_and_keeps_the_state_when_it_fails(monkeypatch):
    fake = FakeStack([])
    cleaned = []

    def cleanup(run):
        fake.calls.append("cleanup")
        cleaned.append(run)
        if len(cleaned) == 1:
            raise RuntimeError("busy")

    monkeypatch.setenv("PULUMI_BACKEND_URL", "file:///x")
    monkeypatch.setattr(stack.auto, "select_stack", lambda *a, **k: fake)
    monkeypatch.setattr(stack.vendors, "load", lambda vendor: types.SimpleNamespace(cleanup=cleanup))
    with pytest.raises(RuntimeError, match="busy"):
        stack.down("gcp-x-1", "gcp")
    assert ("remove", "k8s-dbaas-gcp-x-1") not in fake.calls
    stack.down("gcp-x-1", "gcp")
    assert cleaned == ["gcp-x-1", "gcp-x-1"] and fake.calls[-3:] == [
        ("destroy", True),
        "cleanup",
        ("remove", "k8s-dbaas-gcp-x-1"),
    ]


def test_usage():
    assert entry.main(["x"]) == 2 and entry.main(["x", "sideways"]) == 2
