"""Keeper API client: the ACTIVE servers and task eligibility (autoPlan; forced runs skip it).

Implements decision K25: keep the HTTP call, fail closed (any error means "blocked"). Port of sc-inspector
`keeper_client.py` (`/inspector/server/<vendor>/<sku>/task_block_reason_codes`), with `urllib.request`.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence

from k8s_inspector.model import EnvConfig

DEFAULT_API_BASE = "https://keeper.sparecores.net"  # the legacy default
TIMEOUT_SECONDS = 60
UNKNOWN_SKU = "KEEPER_UNKNOWN_SKU"  # reason recorded for every task of a SKU Keeper answers 404 for


def _read_key(env: EnvConfig) -> str | None:
    """The API key, or None when no key file is configured or the (optional) Secret mount is absent."""
    if not env.keeper_api_key_file:
        return None
    try:
        with open(env.keeper_api_key_file) as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def _fetch(url: str, key: str | None) -> object | None:
    """Parsed JSON of a GET, None on 404; `RuntimeError` for everything else."""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}"} if key else {})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 (configured base URL)
            return json.loads(response.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise RuntimeError(f"keeper HTTP {e.code} for {url}") from e
    except (OSError, ValueError) as e:  # URLError, timeouts, bad JSON
        raise RuntimeError(f"keeper request failed: {url}: {e}") from e


def _get(url: str, key: str | None) -> dict | None:
    """Parsed JSON object of a GET, None on 404; `RuntimeError` for everything else."""
    data = _fetch(url, key)
    if data is not None and not isinstance(data, dict):
        raise RuntimeError(f"keeper returned unexpected JSON type for {url}")
    return data


def block_reasons(
    env: EnvConfig, vendor: str, skus: list[str], task_names: Sequence[str] | None = None
) -> dict[str, dict[str, list[str]]]:
    """`{sku: {task_name: [reason codes]}}`; a task absent from a SKU's dict is eligible. Raises
    `RuntimeError` when Keeper cannot be reached or answers unexpectedly (the planner then plans nothing for
    that cycle). A SKU unknown to Keeper (404) has every task blocked with `UNKNOWN_SKU`. `task_names` defaults
    to the catalogue (`tasks.all_tasks()`)."""
    if task_names is None:
        from k8s_inspector import tasks

        task_names = [t.name for t in tasks.all_tasks()]
    base = (env.keeper_api_base or DEFAULT_API_BASE).rstrip("/")
    key = _read_key(env)
    query = urllib.parse.urlencode([("tasks", name) for name in task_names])
    result: dict[str, dict[str, list[str]]] = {}
    for sku in skus:
        path = f"/inspector/server/{urllib.parse.quote(vendor, safe='')}/{urllib.parse.quote(sku, safe='')}"
        data = _get(f"{base}{path}/task_block_reason_codes?{query}", key)
        if data is None:
            result[sku] = {name: [UNKNOWN_SKU] for name in task_names}
        else:
            result[sku] = {name: list(reasons) for name, reasons in data.items() if reasons}
    return result


def active_servers_block_reasons(
    env: EnvConfig, task_names: Sequence[str]
) -> dict[str, dict[str, dict[str, list[str]]]]:
    """`{vendor: {sku: {task_name: [reason codes]}}}` of every ACTIVE server in one call (legacy
    `servers_task_block_reasons`); a task absent from a SKU's dict is eligible. The SKU list is the planner's list
    of candidates. Raises `RuntimeError` when Keeper cannot be reached or answers unexpectedly."""
    base = (env.keeper_api_base or DEFAULT_API_BASE).rstrip("/")
    query = urllib.parse.urlencode([("tasks", name) for name in task_names])
    url = f"{base}/inspector/servers/task_block_reason_codes?{query}"
    data = _get(url, _read_key(env))
    if data is None:
        raise RuntimeError(f"keeper HTTP 404 for {url}")
    try:
        return {
            vendor: {
                sku: {task: list(reasons) for task, reasons in found.items() if reasons} for sku, found in skus.items()
            }
            for vendor, skus in data.items()
        }
    except (AttributeError, TypeError) as e:
        raise RuntimeError(f"keeper returned unexpected JSON for {url}") from e


def server_facts(env: EnvConfig, vendor: str, sku: str) -> dict | None:
    """`family`, `cpu_arch` and `gpu_count` of a server, from Keeper's `/v2/server/<vendor>/<sku>` fields `family`,
    `cpu_architecture` and `accelerator_count`. None when Keeper does not know the SKU; `RuntimeError` when it
    cannot be reached."""
    path = f"/v2/server/{urllib.parse.quote(vendor, safe='')}/{urllib.parse.quote(sku, safe='')}"
    data = _get(f"{(env.keeper_api_base or DEFAULT_API_BASE).rstrip('/')}{path}", _read_key(env))
    if data is None:
        return None
    count = data.get("accelerator_count")
    return {
        "family": data.get("family"),
        "cpu_arch": data.get("cpu_architecture"),
        "gpu_count": int(count) if count is not None else None,
    }


def ondemand_prices(env: EnvConfig, vendor: str, sku: str) -> dict[str, float]:
    """`{region api_reference: USD per hour}` of the on-demand price of a server, from the ACTIVE price rows of
    `/server/<vendor>/<sku>/prices` (the cheapest zone of the region). A region without a row is absent; a SKU
    Keeper does not know has none. `RuntimeError` when Keeper cannot be reached."""
    from k8s_inspector import dbaas_keeper

    path = f"/server/{urllib.parse.quote(vendor, safe='')}/{urllib.parse.quote(sku, safe='')}/prices"
    names = {region_id: name for name, region_id in dbaas_keeper.region_ids(env, vendor).items()}
    prices: dict[str, float] = {}
    for row in dbaas_keeper._list(env, path):
        if row.get("status") == "active" and row.get("allocation") == "ondemand" and row.get("price") is not None:
            region = names.get(str(row["region_id"]))
            if region:
                prices[region] = min(float(row["price"]), prices.get(region, float("inf")))
    return prices
