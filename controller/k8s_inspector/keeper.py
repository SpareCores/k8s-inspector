"""Keeper API client: task eligibility for a SKU (autoPlan only; forced runs skip it).

Owner: planner agent. Implements decision K25: keep the HTTP call, fail closed (any error means "blocked").
Port of sc-inspector `keeper_client.py`. Use `urllib.request` (no extra dependency).
"""

from k8s_inspector.model import EnvConfig


def block_reasons(env: EnvConfig, vendor: str, skus: list[str]) -> dict[str, dict[str, list[str]]]:
    """`{sku: {task_name: [reason codes]}}`; a task absent from a SKU's dict is eligible. Raises
    `RuntimeError` when Keeper cannot be reached (the planner then plans nothing for that cycle)."""
    raise NotImplementedError
