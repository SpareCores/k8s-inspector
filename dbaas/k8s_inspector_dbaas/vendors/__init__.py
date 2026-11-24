"""One module per vendor: `vendors/<vendor>.py` exposes `provision(spec: DbSpec) -> DbOutputs`."""

import importlib
import re
from types import ModuleType


def load(vendor: str) -> ModuleType:
    """The vendor module. Raise `ValueError` for a vendor id that is not a module name or has no module."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", vendor):
        raise ValueError(f"invalid vendor id {vendor!r}")
    try:
        return importlib.import_module(f"{__name__}.{vendor}")
    except ModuleNotFoundError as e:
        if e.name != f"{__name__}.{vendor}":  # the vendor module exists but its provider package is missing
            raise
        raise ValueError(f"no DBaaS module for vendor {vendor!r}") from e
