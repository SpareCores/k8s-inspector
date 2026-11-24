"""What Keeper knows about managed databases and the client servers next to them (decision K25: fail closed).

All of it is Spare Cores data: which databases exist, their versions, vCPUs and memory, the regions they are priced in
and what a client server costs there. Nothing here is a static table. Every error is a `RuntimeError`: the planner then
plans nothing and a forced run is refused.
"""

import time
import urllib.parse
from collections.abc import Sequence

from k8s_inspector import keeper
from k8s_inspector.model import EnvConfig

# The DBaaS price rows that stand for a database without high availability: Keeper's `none`, and for vendors that
# only list zonal levels (AWS RDS) `single-zone`. The first level a database has prices for is its standalone one.
STANDALONE_LEVELS = ("none", "single-zone")


def _base(env: EnvConfig) -> str:
    return (env.keeper_api_base or keeper.DEFAULT_API_BASE).rstrip("/")


def _list(env: EnvConfig, path: str, params: Sequence[tuple[str, str]] = ()) -> list[dict]:
    url = f"{_base(env)}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    data = keeper._fetch(url, keeper._read_key(env))
    if data is None:
        return []
    if not isinstance(data, list):
        raise RuntimeError(f"keeper returned unexpected JSON type for {url}")
    return data


def databases(env: EnvConfig, vendor: str) -> list[dict]:
    """Keeper's records of the vendor's ACTIVE PostgreSQL databases (`/databases`)."""
    rows = _list(env, "/databases", [("vendor", vendor), ("only_active", "true"), ("limit", "-1")])
    return [row for row in rows if str(row.get("engine", "")).lower() in ("postgresql", "postgres")]


def database(env: EnvConfig, vendor: str, database_id: str) -> dict | None:
    """One record, None when Keeper does not know the database."""
    url = f"{_base(env)}/database/{urllib.parse.quote(vendor, safe='')}/{urllib.parse.quote(database_id, safe='')}"
    return keeper._get(url, keeper._read_key(env))


def block_reasons(env: EnvConfig, task_names: Sequence[str]) -> dict[str, dict[str, dict[str, list[str]]]]:
    """`{vendor: {database: {task: [reason codes]}}}` of every ACTIVE database in one call; a task absent from a
    database's dict is eligible."""
    query = urllib.parse.urlencode([("tasks", t) for t in task_names])
    url = f"{_base(env)}/inspector/databases/task_block_reason_codes?{query}"
    data = keeper._get(url, keeper._read_key(env))
    if data is None:
        raise RuntimeError(f"keeper HTTP 404 for {url}")
    try:
        return {
            vendor: {
                db: {task: list(reasons) for task, reasons in found.items() if reasons} for db, found in rows.items()
            }
            for vendor, rows in data.items()
        }
    except (AttributeError, TypeError) as e:
        raise RuntimeError(f"keeper returned unexpected JSON for {url}") from e


def database_block_reasons(
    env: EnvConfig, vendor: str, database_id: str, task_names: Sequence[str]
) -> dict[str, list[str]]:
    """The block reasons of one database; a database Keeper does not know has every task blocked."""
    quoted = urllib.parse.quote
    query = urllib.parse.urlencode([("tasks", t) for t in task_names])
    path = f"/inspector/database/{quoted(vendor, safe='')}/{quoted(database_id, safe='')}/task_block_reason_codes"
    url = f"{_base(env)}{path}?{query}"
    data = keeper._get(url, keeper._read_key(env))
    if data is None:
        return {task: [keeper.UNKNOWN_SKU] for task in task_names}
    return {task: list(reasons) for task, reasons in data.items() if reasons}


def region_ids(env: EnvConfig, vendor: str) -> dict[str, str]:
    """`{region api_reference: Keeper region_id}` of the vendor (price rows and server filters use the id)."""
    rows = _list(env, "/regions", [("vendor", vendor)])
    return {row["api_reference"]: str(row["region_id"]) for row in rows if row.get("vendor_id", vendor) == vendor}


def region_cities(env: EnvConfig, vendor: str) -> dict[str, tuple[str, str]]:
    """`{region api_reference: (Keeper country_id, city)}` of the vendor's regions that have a city. A vendor whose
    database regions differ from its server regions (OVH) is matched by it."""
    rows = _list(env, "/regions", [("vendor", vendor)])
    return {
        row["api_reference"]: (row.get("country_id"), row["city"])
        for row in rows
        if row.get("vendor_id", vendor) == vendor and row.get("city")
    }


def standalone_prices(env: EnvConfig, vendor: str, database_id: str) -> dict[str, float]:
    """`{region api_reference: USD per hour}` of the standalone on-demand price of the database, from its ACTIVE
    price rows. Empty when it has none."""
    path = f"/database/{urllib.parse.quote(vendor, safe='')}/{urllib.parse.quote(database_id, safe='')}/prices"
    rows = [
        row
        for row in _list(env, path)
        if row.get("status") == "active" and row.get("allocation") == "ondemand" and row.get("price") is not None
    ]
    level = next((lv for lv in STANDALONE_LEVELS if any(row.get("ha") == lv for row in rows)), None)
    by_id: dict[str, float] = {}
    for row in rows:
        if row.get("ha") == level:
            region = str(row["region_id"])
            by_id[region] = min(float(row["price"]), by_id.get(region, float("inf")))
    names = {rid: name for name, rid in region_ids(env, vendor).items()}
    return {names[rid]: price for rid, price in by_id.items() if rid in names}


def client_servers(env: EnvConfig, vendor: str, region_id: str) -> list[dict]:
    """Candidate client servers of the vendor in the region (`/servers`): ACTIVE, x86_64, no accelerator, 2-4 vCPUs,
    with the on-demand price in that region as `min_price_ondemand`. Memory and CPU allocation are judged by
    `dbaas_client.rank`."""
    params = [
        ("vendor", vendor),
        ("regions", region_id),
        ("vcpus_min", "2"),
        ("vcpus_max", "4"),
        ("architecture", "x86_64"),
        ("gpu_max", "0"),
        ("only_active", "true"),
        ("best_price_allocation", "ONDEMAND_ONLY"),
        ("limit", "-1"),
    ]
    return _list(env, "/servers", params)


def engine_version(record: dict, preferred: str) -> str | None:
    """The PostgreSQL major version to benchmark: `preferred` when the database offers it, else its newest."""
    versions = [str(v) for v in record.get("engine_versions") or []]
    if not versions:
        return None
    if preferred in versions:
        return preferred

    def number(version: str) -> tuple[int, ...]:
        return tuple(int(part) if part.isdigit() else -1 for part in version.split("."))

    return max(versions, key=number)


class Cache:
    """The bulk Keeper answers (database records and block reasons), kept for a while: they change rarely and a
    planning cycle runs every few minutes."""

    TTL_SECONDS = 30 * 60

    def __init__(self) -> None:
        self._at: dict[object, float] = {}
        self._value: dict[object, object] = {}

    def _get(self, key: object, load):
        if key not in self._value or time.monotonic() - self._at[key] >= self.TTL_SECONDS:
            self._value[key] = load()
            self._at[key] = time.monotonic()
        return self._value[key]

    def databases(self, env: EnvConfig, vendor: str) -> list[dict]:
        return self._get(("databases", vendor), lambda: databases(env, vendor))

    def block_reasons(self, env: EnvConfig, task_names: Sequence[str]) -> dict[str, dict[str, dict[str, list[str]]]]:
        return self._get(("blocked", tuple(task_names)), lambda: block_reasons(env, task_names))
