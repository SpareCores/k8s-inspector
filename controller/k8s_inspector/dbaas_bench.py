"""The pgbench side of a DBaaS run: the benchmark's environment and the PostgreSQL settings merged into its output.

Port of the constants of sc-inspector `benchmark_tiers` that the read-only workload uses, and of `pg_repro.
merge_postgres_into_stdout`. The connection variables (`SC_DB_HOST` ...) are not here: they come from the run's Secret.
"""

import json

RUN_SECONDS = 300  # 2 min warmup + 5 min measurement is enough to rank databases
WARMUP_SECONDS = 120
SETTLE_SECONDS = 60  # after the first warmup, later concurrency levels only settle
CPU_SCALE = 1


def concurrency_profile(vcpus: int) -> list[int]:
    """Client counts of the read-only workload: one (single-thread rank), half the vCPUs, all of them, and twice as
    many (hyperthreads oversubscribed). Deduplicated, ascending."""
    v = max(1, int(vcpus))
    return sorted({1, max(1, v // 2), v, 2 * v})


def pgbench_env(database: dict, client_vcpus: int | None) -> tuple[tuple[str, str], ...]:
    """The variables of the pgbench container beyond the task's own: what the database is (`database` is `RunSpec.
    database`, with `vcpus` and `memoryGib`) and how hard to drive it. `client_vcpus` is the client node's vCPUs."""
    vcpus = int(database["vcpus"])
    anchors = concurrency_profile(vcpus)
    env = {
        "SC_WORKLOAD": "pgbench_ro",
        "SC_PGBENCH_DB": "pgbench",
        "SC_TOPOLOGY": "dbaas",
        "SC_DB_VCPUS": str(vcpus),
        "SC_DB_MEM_GIB": str(database["memoryGib"]),
        "SC_PROFILE": "1",
        "SC_PROFILE_VUS": ",".join(str(v) for v in anchors),
        "SC_PROFILE_SEARCH": "0",
        "SC_PROFILE_MAX_CLIENTS": str(anchors[-1]),
        "SC_PROFILE_HARD_MAX_CLIENTS": str(anchors[-1]),
        "SC_WARMUP_ONCE": "1",
        "SC_WARMUP_SECONDS": str(WARMUP_SECONDS),
        "SC_SETTLE_SECONDS": str(SETTLE_SECONDS),
        "SC_RUN_SECONDS": str(RUN_SECONDS),
        "SC_RUN_VUS": str(anchors[-1]),
        "SC_CPU_SCALE": str(CPU_SCALE),
    }
    if client_vcpus:
        env["SC_CLIENT_VCPUS"] = str(client_vcpus)
    return tuple(env.items())


def merge_postgres(stdout: bytes, document: dict) -> bytes:
    """`stdout` (the benchmark's JSON) with the bootstrap's `postgres` block added and its `extra` fields filled in
    where the benchmark did not report them. Output that is not a JSON object is kept under `raw_stdout`. Without
    anything to merge, `stdout` is returned unchanged."""
    postgres, extra = document.get("postgres"), document.get("extra")
    if not postgres and not extra:
        return stdout
    text = stdout.decode("utf-8", errors="replace")
    if text.strip():
        try:
            parsed = json.loads(text)
            summary = parsed if isinstance(parsed, dict) else {"raw_stdout": parsed}
        except json.JSONDecodeError:
            summary = {"raw_stdout": text}
    else:
        summary = {}
    if postgres:
        summary["postgres"] = postgres
    for key, value in (extra or {}).items():
        if value is not None and summary.get(key) in (None, "", [], {}):
            summary[key] = value
    return (json.dumps(summary, indent=2) + "\n").encode()
