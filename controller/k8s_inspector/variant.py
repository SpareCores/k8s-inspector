"""Hardware variants: port of sc-inspector `inspector/variant_id.py` (`ID_VERSION = 1`).

Owner: publisher agent (may be done together with render.py). Pure functions. Keep ids identical to the legacy
so existing `data/<vendor>/<sku>/<N>/variant.json` files match.
"""

from collections.abc import Mapping
from typing import Any


def parse_lscpu_stdout(raw: Any) -> dict[str, str]:
    """`lscpu -JB` stdout (bytes/str JSON) -> fields keyed like `"Model name:"`."""
    raise NotImplementedError


def variant_id_from_fields(fields: Mapping[str, str]) -> str:
    raise NotImplementedError


def build_variant_json(
    fields: Mapping[str, str], *, source: str, first_seen: str | None = None, lscpu_version: str | None = None
) -> dict[str, Any]:
    """`source` is `observed` or `backfill`."""
    raise NotImplementedError


def match_variant(machine_fields: Mapping[str, str], stored_variant: Mapping[str, Any]) -> bool:
    raise NotImplementedError


def pick_variant(existing: Mapping[int, Mapping[str, Any]], machine_fields: Mapping[str, str]) -> int:
    """Index of the stored `variant.json` that matches the machine, else the next free integer. Called by the
    publisher with the variants found in its checkout (`data/<vendor>/<sku>/<N>/variant.json`)."""
    raise NotImplementedError
