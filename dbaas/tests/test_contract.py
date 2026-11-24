import json

import pulumi
import pytest

from k8s_inspector_dbaas import __main__ as entry
from k8s_inspector_dbaas import stack, vendors
from k8s_inspector_dbaas.spec import DbOutputs, load_spec

SPEC_JSON = {
    "vendor": "fake", "databaseId": "db-8", "engineVersion": 18, "region": "eu-test-1", "zone": None,
    "run": "fake-x-1", "runId": "r1", "expiresAt": "2026-11-20T16:00:00Z", "labels": {"sc_inspector_run": "r1"},
    "network": {"vpc": "v-1"}, "record": {"vcpus": 4, "memory_amount": 16384},
}  # fmt: skip


def test_the_spec_comes_from_the_environment():
    spec = load_spec({"DBAAS_SPEC": json.dumps(SPEC_JSON), "DBAAS_PASSWORD": "pw"})
    assert (spec.vendor, spec.engine_version, spec.zone, spec.network) == ("fake", "18", None, {"vpc": "v-1"})
    assert (spec.vcpus, spec.memory_gib, spec.password) == (4, 16.0, "pw")
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


def test_down_needs_only_the_run_name(monkeypatch):
    destroyed = []
    monkeypatch.setenv("DBAAS_RUN", "fake-x-1")
    monkeypatch.setattr(stack, "down", destroyed.append)
    assert entry.run("down") == 0 and destroyed == ["fake-x-1"]
    monkeypatch.setattr(stack, "down", lambda run: 1 / 0)
    assert entry.run("down") == 1


def test_usage():
    assert entry.main(["x"]) == 2 and entry.main(["x", "sideways"]) == 2
