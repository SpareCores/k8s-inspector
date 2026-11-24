"""The bootstrap of a managed database, run on the client node (port of sc-inspector `postgres_dbaas`).

NOT imported by the controller as code: the controller ships this file's text to the client node in a ConfigMap and runs
it with `python3 /dbaas/bootstrap.py` in the benchmark image (Python and psycopg 3 are in it, so no extra image is
pulled). It therefore uses only the standard library and psycopg, and reads everything from the environment: the
connection facts from the run's Secret (`SC_DB_*`), `DB_WAIT_TIMEOUT_SEC`, `SC_DURABILITY`.

Steps: wait until the database accepts connections; create the workload user when the vendor's admin user is not it;
create the database `bench`; probe whether `synchronous_commit` can be set per session; apply the durability mode to the
user; collect the PostgreSQL settings. The settings go to `/k8si/dbaas/postgres.json`, which the benchmark container
uploads next to its output; the controller merges them into the published `stdout`. The outcome goes to the termination
message: `{"sync_commit_session_settable": bool}`, or `{"error": "..."}` with exit code 1.
"""

import json
import os
import sys
import time

OUTPUT = "/k8si/dbaas/postgres.json"
TERMINATION_LOG = "/dev/termination-log"
RETRY_SECONDS = 10


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def connect(user: str, dbname: str, timeout: int = 10):
    """A connection with autocommit on, as the given user. The password is the run's, the same for every user."""
    import psycopg

    return psycopg.connect(
        host=env("SC_DB_HOST"),
        port=int(env("SC_DB_PORT", "5432")),
        user=user,
        password=env("SC_DB_PASSWORD"),
        dbname=dbname,
        connect_timeout=timeout,
        sslmode=env("SC_DB_SSLMODE", "prefer"),
        autocommit=True,
    )


def wait_until_connects(user: str, dbname: str, timeout: float, *, connect_fn=connect, sleep=time.sleep) -> None:
    """Retry every `RETRY_SECONDS` until a connection works. Raise `TimeoutError` with the last error."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            connect_fn(user, dbname).close()
            return
        except Exception as e:
            last = e
            sleep(RETRY_SECONDS)
    raise TimeoutError(f"{env('SC_DB_HOST')} did not accept connections as {user} within {timeout:.0f}s: {last}")


def create_workload_user(conn, user: str, grant_role: str | None) -> None:
    """The vendor's admin user is not the workload user (Cloud SQL's `postgres` cannot create databases it can use):
    create the user with the run's password, or make an existing one capable again. DDL takes no bind parameters, so
    the statement carries the SCRAM-SHA-256 verifier that libpq computes here, never the password: the server's
    statement log and error messages do not hold it."""
    from psycopg import sql

    verifier = conn.pgconn.encrypt_password(env("SC_DB_PASSWORD").encode(), user.encode(), b"scram-sha-256").decode()
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
        if cur.fetchone():
            cur.execute(sql.SQL("ALTER USER {} CREATEDB CREATEROLE").format(sql.Identifier(user)))
        else:
            cur.execute(
                sql.SQL("CREATE USER {} WITH ENCRYPTED PASSWORD {} CREATEDB CREATEROLE").format(
                    sql.Identifier(user), sql.Literal(verifier)
                )
            )
        if grant_role:
            cur.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(grant_role), sql.Identifier(user)))


def create_database(conn, name: str) -> None:
    from psycopg import sql

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def probe_sync_commit(conn) -> bool:
    """Can a session relax `synchronous_commit`? Managed services differ; the result goes to the published output."""
    try:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit TO off")
        return True
    except Exception:
        return False


def apply_durability(conn, durability: str, user: str, settable: bool) -> None:
    """`async` relaxes `synchronous_commit` for the user's sessions, anything else leaves the server default."""
    from psycopg import sql

    if durability == "async" and not settable:
        raise RuntimeError("async durability needs synchronous_commit=off, which this managed database does not allow")
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
        if not cur.fetchone():
            return
        role = sql.Identifier(user)
        if durability == "async":
            cur.execute(sql.SQL("ALTER ROLE {} SET synchronous_commit TO off").format(role))
        else:
            cur.execute(sql.SQL("ALTER ROLE {} RESET synchronous_commit").format(role))


def collect_postgres(conn) -> dict:
    """Server version, every setting (as `SHOW` prints it), the non-default ones with their source, extensions and
    per-role settings, so a run can be reproduced."""
    out: dict = {}
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        out["version"] = str(cur.fetchone()[0])
        cur.execute("SHOW server_version")
        out["server_version"] = str(cur.fetchone()[0])
        cur.execute("SHOW server_version_num")
        out["server_version_num"] = int(cur.fetchone()[0])
        cur.execute("SELECT pg_is_in_recovery()")
        out["in_recovery"] = bool(cur.fetchone()[0])
        cur.execute(
            "SELECT name, setting, unit, current_setting(name), category, short_desc, context, vartype, source,"
            " pending_restart FROM pg_settings ORDER BY name"
        )
        settings, nondefault = {}, {}
        for name, setting, unit, pretty, category, short_desc, context, vartype, source, pending in cur.fetchall():
            settings[name] = pretty
            if source and source != "default":
                entry = {"setting": pretty, "source": source, "context": context, "vartype": vartype}
                entry["category"] = category
                if unit:
                    entry["unit"], entry["setting_raw"] = unit, setting
                if short_desc:
                    entry["short_desc"] = short_desc
                if pending:
                    entry["pending_restart"] = True
                nondefault[name] = entry
        out["settings"], out["nondefault_settings"] = settings, nondefault
        cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname")
        out["extensions"] = [{"name": n, "version": v} for n, v in cur.fetchall()]
        cur.execute(
            "SELECT COALESCE(r.rolname, 'All'), COALESCE(d.datname, 'All'), s.setconfig FROM pg_db_role_setting s"
            " LEFT JOIN pg_roles r ON r.oid = s.setrole LEFT JOIN pg_database d ON d.oid = s.setdatabase ORDER BY 1, 2"
        )
        out["role_settings"] = [{"role": r, "database": d, "config": list(c or [])} for r, d, c in cur.fetchall()]
    return out


def run(*, connect_fn=connect, sleep=time.sleep) -> dict:
    """The whole bootstrap; returns the termination message."""
    admin, admin_db = env("SC_DB_BOOTSTRAP_USER"), env("SC_DB_BOOTSTRAP_DATABASE", "postgres")
    user, name = env("SC_DB_USER"), env("SC_DB_NAME", "bench")
    timeout = float(env("DB_WAIT_TIMEOUT_SEC", "3600"))
    wait_until_connects(admin, admin_db, timeout, connect_fn=connect_fn, sleep=sleep)
    with connect_fn(admin, admin_db) as conn:
        if user == admin:
            create_database(conn, name)
        else:
            create_workload_user(conn, user, env("SC_DB_GRANT_ROLE") or None)
    if user != admin:  # the admin user cannot hand a database to the workload user: the workload user creates it
        with connect_fn(user, admin_db) as conn:
            create_database(conn, name)
    wait_until_connects(user, name, timeout, connect_fn=connect_fn, sleep=sleep)
    with connect_fn(user, name) as conn:
        settable = probe_sync_commit(conn)
    with connect_fn(user, name) as conn:
        apply_durability(conn, env("SC_DURABILITY", "durable"), user, settable)
    extra = {"sslmode": env("SC_DB_SSLMODE", "prefer"), "sync_commit_session_settable": settable}
    document: dict = {"extra": extra}
    try:
        with connect_fn(user, name) as conn:
            document["postgres"] = collect_postgres(conn)
    except Exception as e:  # the repro data is a nicety: a benchmark without it is still a benchmark
        print(f"cannot collect the PostgreSQL settings: {e}", file=sys.stderr)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(document, f)
    return {"sync_commit_session_settable": settable}


def main() -> int:
    try:
        message = run()
        code = 0
    except Exception as e:
        print(f"bootstrap failed: {e}", file=sys.stderr)
        message, code = {"error": str(e)[:2000].replace(env("SC_DB_PASSWORD") or "\0", "***")}, 1
    with open(TERMINATION_LOG, "w") as f:
        json.dump(message, f)
    return code


if __name__ == "__main__":
    sys.exit(main())
