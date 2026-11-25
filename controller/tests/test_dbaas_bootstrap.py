"""The bootstrap script (it runs in the benchmark image with psycopg; here psycopg is a recording stand-in).

The script was also run against PostgreSQL 18 in the real benchmark image: same user, a distinct workload user that is
granted a role, `async` durability, an unreachable database."""

import json
import sys
import types

import pytest

from k8s_inspector import dbaas_bench
from k8s_inspector import dbaas_bootstrap as boot

ENV = {
    "SC_DB_HOST": "10.0.0.5", "SC_DB_PORT": "5432", "SC_DB_PASSWORD": "pw", "SC_DB_SSLMODE": "require",
    "SC_DB_USER": "scadmin", "SC_DB_BOOTSTRAP_USER": "scadmin", "SC_DB_BOOTSTRAP_DATABASE": "postgres",
    "SC_DB_NAME": "bench", "DB_WAIT_TIMEOUT_SEC": "60", "SC_DURABILITY": "durable",
}  # fmt: skip


class Cursor:
    def __init__(self, conn) -> None:
        self.conn, self.rows = conn, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        text = str(query)
        self.conn.log.append((self.conn.user, self.conn.dbname, text))
        if text.startswith("SET synchronous_commit") and not self.conn.world.get("settable", True):
            raise RuntimeError("permission denied")
        known = text.startswith("SELECT 1 FROM pg_roles") and params[0] in self.conn.world["roles"]
        known = known or (text.startswith("SELECT 1 FROM pg_database") and params[0] in self.conn.world["databases"])
        self.rows = [(1,)] if known else []
        if text.startswith("CREATE DATABASE"):
            self.conn.world["databases"].add(text.split('"')[1])
        if text.startswith("CREATE USER"):
            self.conn.world["roles"].add(text.split('"')[1])
        if text == "SELECT version()":
            self.rows = [("PostgreSQL 18.1",)]
        elif text.startswith("SHOW"):
            self.rows = [("18.1",)] if "num" not in text else [(180001,)]
        elif text.startswith("SELECT pg_is_in_recovery"):
            self.rows = [(False,)]
        elif "FROM pg_settings" in text:
            self.rows = [
                ("work_mem", "4096", "kB", "4MB", "Resource", "d", "user", "integer", "configuration file", False)
            ]
        elif "FROM pg_extension" in text:
            self.rows = [("plpgsql", "1.0")]
        elif "FROM pg_db_role_setting" in text:
            self.rows = [("scadmin", "All", ["synchronous_commit=off"])]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Conn:
    def __init__(self, world, log, user, dbname) -> None:
        self.world, self.log, self.user, self.dbname, self.closed = world, log, user, dbname, False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    class pgconn:  # noqa: N801
        @staticmethod
        def encrypt_password(password: bytes, user: bytes, algorithm: bytes) -> bytes:
            return b"SCRAM-SHA-256$4096:salt$" + algorithm + b":" + user  # never contains the password

    def cursor(self):
        assert not self.closed, "the connection is closed"
        return Cursor(self)

    def close(self):
        self.closed = True


class Psycopg:
    """Stand-in for `psycopg` and `psycopg.sql`: statements become plain text, identifiers and literals quoted."""

    def __init__(self) -> None:
        self.world = {"roles": {"scadmin", "postgres"}, "databases": {"postgres"}, "settable": True}
        self.log: list[tuple[str, str, str]] = []
        self.refuse: set[str] = set()  # users that cannot connect

    def connect_fn(self, user, dbname, timeout=10):
        if user in self.refuse:
            raise RuntimeError(f"no login for {user}")
        return Conn(self.world, self.log, user, dbname)

    def install(self, monkeypatch):
        class SQL(str):
            def format(self, *parts):
                return SQL(str.format(self, *parts))

        sql = types.SimpleNamespace(SQL=SQL, Identifier=lambda name: f'"{name}"', Literal=lambda value: f"'{value}'")
        monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(sql=sql))
        monkeypatch.setitem(sys.modules, "psycopg.sql", sql)

    def statements(self):
        return [
            (user, db, text) for user, db, text in self.log if text.split()[0] in ("CREATE", "ALTER", "GRANT", "SET")
        ]


@pytest.fixture
def pg(monkeypatch, tmp_path):
    fake = Psycopg()
    fake.install(monkeypatch)
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(boot, "OUTPUT", str(tmp_path / "dbaas" / "postgres.json"))
    monkeypatch.setattr(boot, "TERMINATION_LOG", str(tmp_path / "termination"))
    return fake


def run(pg):
    return boot.run(connect_fn=pg.connect_fn, sleep=lambda s: None)


def test_the_database_is_created_by_the_admin_user_and_the_settings_are_collected(pg, tmp_path):
    assert run(pg) == {"sync_commit_session_settable": True}
    assert ("scadmin", "postgres", 'CREATE DATABASE "bench"') in pg.statements()
    assert ("scadmin", "bench", 'ALTER ROLE "scadmin" RESET synchronous_commit') in pg.statements()  # durable
    document = json.loads((tmp_path / "dbaas" / "postgres.json").read_text())
    assert document["extra"] == {"sslmode": "require", "sync_commit_session_settable": True}
    postgres = document["postgres"]
    assert (postgres["version"], postgres["server_version_num"], postgres["in_recovery"]) == (
        "PostgreSQL 18.1",
        180001,
        False,
    )
    assert postgres["settings"] == {"work_mem": "4MB"} and postgres["nondefault_settings"]["work_mem"]["unit"] == "kB"
    assert postgres["extensions"] == [{"name": "plpgsql", "version": "1.0"}]
    assert postgres["role_settings"] == [{"role": "scadmin", "database": "All", "config": ["synchronous_commit=off"]}]


def test_an_existing_database_is_left_alone_so_a_rerun_works(pg):
    pg.world["databases"].add("bench")
    run(pg)
    assert not [s for s in pg.statements() if "CREATE DATABASE" in s[2]]


def test_a_distinct_workload_user_is_created_granted_and_creates_the_database_itself(pg, monkeypatch):
    for name, value in {
        "SC_DB_USER": "workload",
        "SC_DB_BOOTSTRAP_USER": "postgres",
        "SC_DB_GRANT_ROLE": "cloudsqlsuperuser",
    }.items():
        monkeypatch.setenv(name, value)
    run(pg)
    statements = pg.statements()
    assert statements[0] == (
        "postgres",
        "postgres",
        """CREATE USER "workload" WITH ENCRYPTED PASSWORD 'SCRAM-SHA-256$4096:salt$scram-sha-256:workload' """
        "CREATEDB CREATEROLE",
    )
    assert not [s for s in statements if "'pw'" in s[2]]  # the password itself is in no statement
    assert statements[1] == ("postgres", "postgres", 'GRANT "cloudsqlsuperuser" TO "workload"')
    assert statements[2] == ("workload", "postgres", 'CREATE DATABASE "bench"')  # the admin user cannot give it away


def test_an_existing_workload_user_is_made_capable_again(pg, monkeypatch):
    monkeypatch.setenv("SC_DB_USER", "workload")
    monkeypatch.setenv("SC_DB_BOOTSTRAP_USER", "postgres")
    pg.world["roles"].add("workload")
    run(pg)
    assert ("postgres", "postgres", 'ALTER USER "workload" CREATEDB CREATEROLE') in pg.statements()


def test_async_durability_relaxes_synchronous_commit_for_the_user(pg, monkeypatch):
    monkeypatch.setenv("SC_DURABILITY", "async")
    run(pg)
    assert ("scadmin", "bench", 'ALTER ROLE "scadmin" SET synchronous_commit TO off') in pg.statements()


def test_async_durability_is_refused_where_synchronous_commit_cannot_be_relaxed(pg, monkeypatch):
    monkeypatch.setenv("SC_DURABILITY", "async")
    pg.world["settable"] = False
    with pytest.raises(RuntimeError, match="does not allow"):
        run(pg)


def test_a_database_that_never_accepts_connections_times_out_with_the_last_error(pg, monkeypatch):
    pg.refuse.add("scadmin")
    ticks = iter(range(0, 1000, 20))
    monkeypatch.setattr(boot.time, "monotonic", lambda: next(ticks))
    with pytest.raises(TimeoutError, match="did not accept connections as scadmin within 60s: no login for scadmin"):
        run(pg)


def test_the_connect_error_is_logged_once_a_minute_while_waiting(pg, monkeypatch, capsys):
    pg.refuse.add("scadmin")
    clock = [0.0]
    monkeypatch.setattr(boot.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        clock[0] += seconds

    with pytest.raises(TimeoutError):
        boot.wait_until_connects("scadmin", "postgres", 150, connect_fn=pg.connect_fn, sleep=sleep)
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 3  # at 0 s, 60 s and 120 s, not at every 10 s retry
    assert all("no login for scadmin" in line for line in lines)


def test_the_result_goes_to_the_termination_message_without_the_password(pg, monkeypatch, tmp_path):
    real_run = boot.run
    monkeypatch.setattr(boot, "run", lambda: real_run(connect_fn=pg.connect_fn, sleep=lambda s: None))
    assert boot.main() == 0
    assert json.loads((tmp_path / "termination").read_text()) == {"sync_commit_session_settable": True}

    def failing(*args, **kwargs):
        raise RuntimeError("connection with password pw failed")

    monkeypatch.setattr(boot, "run", failing)
    assert boot.main() == 1
    assert json.loads((tmp_path / "termination").read_text()) == {"error": "connection with password *** failed"}


def test_the_postgres_settings_are_optional(pg, monkeypatch, tmp_path):
    monkeypatch.setattr(boot, "collect_postgres", lambda conn: 1 / 0)
    run(pg)
    assert list(json.loads((tmp_path / "dbaas" / "postgres.json").read_text())) == ["extra"]


# --------------------------------------------------------------------------- merged into the published output


def test_the_postgres_block_and_the_missing_fields_are_merged_into_stdout():
    stdout = json.dumps({"score": 12, "sslmode": None, "db_vcpus": 4, "sizes": []}).encode()
    document = {"postgres": {"version": "PostgreSQL 18"}, "extra": {"sslmode": "require", "db_vcpus": 99, "x": None}}
    merged = json.loads(dbaas_bench.merge_postgres(stdout, document))
    assert merged["postgres"] == {"version": "PostgreSQL 18"} and merged["score"] == 12
    assert (
        merged["sslmode"] == "require" and merged["db_vcpus"] == 4 and "x" not in merged
    )  # only fills what is missing


def test_output_that_is_not_a_json_object_is_kept_under_raw_stdout():
    merged = json.loads(dbaas_bench.merge_postgres(b"not json", {"postgres": {"a": 1}}))
    assert merged == {"raw_stdout": "not json", "postgres": {"a": 1}}
    assert json.loads(dbaas_bench.merge_postgres(b"[1]", {"extra": {"a": 1}})) == {"raw_stdout": [1], "a": 1}


def test_nothing_to_merge_keeps_stdout_byte_for_byte():
    assert dbaas_bench.merge_postgres(b'{"a":1}', {}) == b'{"a":1}'
    assert dbaas_bench.merge_postgres(b"", {"extra": {"a": 1}}) == b'{\n  "a": 1\n}\n'


def test_the_benchmark_environment_follows_the_database_size():
    database = {"vcpus": 16, "memoryGib": 64.0}
    env = dict(dbaas_bench.pgbench_env(database, 4))
    assert env["SC_PROFILE_VUS"] == "1,8,16,32" and env["SC_RUN_VUS"] == "32" and env["SC_CLIENT_VCPUS"] == "4"
    assert (env["SC_DB_VCPUS"], env["SC_DB_MEM_GIB"], env["SC_RUN_SECONDS"]) == ("16", "64.0", "300")
    assert "SC_CLIENT_VCPUS" not in dict(dbaas_bench.pgbench_env(database, None))
    assert dbaas_bench.concurrency_profile(1) == [1, 2] and dbaas_bench.concurrency_profile(2) == [1, 2, 4]
