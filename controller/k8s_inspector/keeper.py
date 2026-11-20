"""Keeper API client: task eligibility for a SKU (autoPlan only; forced runs skip it).

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
    if not env.keeper_api_key_file:
        return None
    with open(env.keeper_api_key_file) as f:
        return f.read().strip()


def _get(url: str, key: str | None) -> dict | None:
    """Parsed JSON object of a GET, None on 404; `RuntimeError` for everything else."""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}"} if key else {})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 (configured base URL)
            data = json.loads(response.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise RuntimeError(f"keeper HTTP {e.code} for {url}") from e
    except (OSError, ValueError) as e:  # URLError, timeouts, bad JSON
        raise RuntimeError(f"keeper request failed: {url}: {e}") from e
    if not isinstance(data, dict):
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
