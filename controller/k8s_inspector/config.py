"""Load configuration: environment variables and the ConfigMap `k8s-inspector` into `model.Config`.

Owner: config agent. Implements docs/reference/configuration.md. The YAML uses camelCase keys
(`maxInFlight`); the dataclasses use snake_case. Missing keys take the dataclass defaults, which equal the
documented example. Unknown keys log a warning (a newer ConfigMap must not stop an older controller); invalid
values raise `ValueError` naming the key.
"""

import logging
from collections.abc import Callable, Mapping
from typing import Any

import yaml

from k8s_inspector.kube import Kube, NotFound
from k8s_inspector.model import (
    Batching,
    CapacityMode,
    Config,
    Deadlines,
    Defaults,
    EnvConfig,
    NodePools,
    PublishSettings,
    Settings,
    SpotSettings,
    UploadSettings,
    VendorSettings,
)

log = logging.getLogger(__name__)

SETTINGS_CONFIGMAP = "k8s-inspector"
SETTINGS_KEY = "config.yaml"  # key inside the ConfigMap's data


# --------------------------------------------------------------------------- environment


def load_env(environ: Mapping[str, str]) -> EnvConfig:
    """Read the `K8SI_*`, `KEEPER_*`, `SENTRY_DSN` variables. Raise `ValueError` naming the missing
    required variable (K8SI_BUCKET, K8SI_BUCKET_REGION). Never log values."""

    def optional(name: str) -> str | None:
        return environ.get(name) or None  # an empty variable counts as unset

    def required(name: str) -> str:
        value = optional(name)
        if value is None:
            raise ValueError(f"missing required environment variable {name}")
        return value

    return EnvConfig(
        bucket=required("K8SI_BUCKET"),
        bucket_region=required("K8SI_BUCKET_REGION"),
        role_arn=optional("K8SI_ROLE_ARN"),
        git_url=optional("K8SI_GIT_URL") or "",
        git_branch=optional("K8SI_GIT_BRANCH") or "main",
        git_key_file=optional("K8SI_GIT_KEY_FILE"),
        keeper_api_base=optional("KEEPER_API_BASE"),
        keeper_api_key_file=optional("KEEPER_API_KEY_FILE"),
        sentry_dsn=optional("SENTRY_DSN"),
        namespace=optional("K8SI_NAMESPACE") or "inspector",
        sc_run_image=optional("K8SI_SC_RUN_IMAGE") or "",
        s3_endpoint_url=optional("K8SI_S3_ENDPOINT_URL"),
    )


# --------------------------------------------------------------------------- value checks
# Each takes (value, path) and returns the checked value or raises ValueError.


def _fail(path: str, expected: str, value: Any) -> ValueError:
    return ValueError(f"invalid config value for {path}: expected {expected}, got {value!r}")


def _bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise _fail(path, "true or false", value)
    return value


def _nonneg_int(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _fail(path, "a non-negative integer", value)
    return value


def _pos_int(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise _fail(path, "a positive integer", value)
    return value


def _nonneg_float(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise _fail(path, "a non-negative number", value)
    return float(value)


def _ratio(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not 0 <= value <= 1:
        raise _fail(path, "a number between 0 and 1", value)
    return float(value)


def _str(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise _fail(path, "a non-empty string", value)
    return value


def _capacity(value: Any, path: str) -> CapacityMode:
    try:
        return CapacityMode(value)
    except ValueError:
        raise _fail(path, " or ".join(m.value for m in CapacityMode), value) from None


def _optional(check: Callable[[Any, str], Any]) -> Callable[[Any, str], Any]:
    """The same check, but `null` is allowed."""
    return lambda value, path: None if value is None else check(value, path)


# --------------------------------------------------------------------------- settings

# camelCase key -> (dataclass field, check). One table per section.
_PLANNER = {"intervalMinutes": ("planner_interval_minutes", _pos_int), "autoPlan": ("auto_plan", _bool)}
_DEFAULTS = {"maxInFlight": ("max_in_flight", _nonneg_int), "maxInFlightGpu": ("max_in_flight_gpu", _nonneg_int)}
_BATCHING = {
    "hourlyMinFillRatio": ("hourly_min_fill_ratio", _ratio),
    "hourlyMaxWaitDays": ("hourly_max_wait_days", _nonneg_int),
    "perSecondMinWorkMinutes": ("per_second_min_work_minutes", _nonneg_int),
    "perSecondMaxWaitDays": ("per_second_max_wait_days", _nonneg_int),
}
_SPOT = {
    "maxSpotAttempts": ("max_spot_attempts", _pos_int),
    "maxLostMinutes": ("max_lost_minutes", _nonneg_int),
    "interruptionsToQuarantine": ("interruptions_to_quarantine", _pos_int),
    "quarantineDays": ("quarantine_days", _nonneg_int),
    "spotMaxTaskMinutes": ("spot_max_task_minutes", _pos_int),
}
_DEADLINES = {
    "provisionCpuMinutes": ("provision_cpu_minutes", _pos_int),
    "provisionGpuHours": ("provision_gpu_hours", _pos_int),
    "imagePullMinutes": ("image_pull_minutes", _pos_int),
    "holdIdleMinutes": ("hold_idle_minutes", _pos_int),
    "orphanNodeClaimMinutes": ("orphan_node_claim_minutes", _pos_int),
}
_PUBLISH = {"intervalMinutes": ("interval_minutes", _pos_int), "keepRunHours": ("keep_run_hours", _nonneg_int)}
_UPLOADS = {
    "partMiB": ("part_mib", _pos_int),
    "maxParts": ("max_parts", _pos_int),
    "singlePutMaxMiB": ("single_put_max_mib", _pos_int),
    "reputSeconds": ("reput_seconds", _pos_int),
    "urlTtlHours": ("url_ttl_hours", _pos_int),
    "resignEveryMinutes": ("resign_every_minutes", _pos_int),
}
_VENDOR = {
    "enabled": ("enabled", _bool),
    "autoPlan": ("auto_plan", _optional(_bool)),
    "maxInFlight": ("max_in_flight", _optional(_nonneg_int)),
    "maxUsdPerHour": ("max_usd_per_hour", _optional(_nonneg_float)),
    "capacity": ("capacity", _capacity),
    "publish": ("publish", _bool),
}


def _mapping(value: Any, path: str) -> Mapping:
    """A YAML section; an empty one (`planner:`) is `None` and counts as `{}`."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise _fail(path, "a mapping", value)
    return value


def _read(section: Mapping, path: str, table: dict) -> dict[str, Any]:
    """Dataclass keyword arguments for the keys present in `section`; warn about keys not in `table`."""
    kwargs = {}
    for key, value in section.items():
        if key not in table:
            log.warning("unknown config key %s (ignored)", f"{path}.{key}" if path else key)
            continue
        field_name, check = table[key]
        kwargs[field_name] = check(value, f"{path}.{key}" if path else key)
    return kwargs


def _node_pools(value: Any, path: str) -> NodePools | None:
    if value is None:
        return None
    section = _mapping(value, path)
    kwargs = _read(
        section,
        path,
        {"cpu": ("cpu", _str), "lowmem": ("lowmem", _optional(_str)), "gpu": ("gpu", _optional(_str))},
    )
    if "cpu" not in kwargs:
        raise ValueError(f"missing config value {path}.cpu")
    return NodePools(**kwargs)


def _vendor(value: Any, path: str) -> VendorSettings:
    section = dict(_mapping(value, path))
    pools = _node_pools(section.pop("nodePools", None), f"{path}.nodePools")
    return VendorSettings(node_pools=pools, **_read(section, path, _VENDOR))


def parse_settings(data: Mapping) -> Settings:
    """Convert the parsed ConfigMap YAML (a dict, possibly empty) into `Settings`. Unknown keys are logged
    as warnings; invalid values raise `ValueError`."""
    sections = dict(_mapping(data, "config"))
    sub = {
        name: _mapping(sections.pop(name, None), name)
        for name in ("planner", "defaults", "batching", "spot", "deadlines", "publish", "uploads")
    }
    vendors = _mapping(sections.pop("vendors", None), "vendors")
    for key in sections:
        log.warning("unknown config key %s (ignored)", key)

    return Settings(
        **_read(sub["planner"], "planner", _PLANNER),
        vendors={str(name): _vendor(value, f"vendors.{name}") for name, value in vendors.items()},
        defaults=Defaults(**_read(sub["defaults"], "defaults", _DEFAULTS)),
        batching=Batching(**_read(sub["batching"], "batching", _BATCHING)),
        spot=SpotSettings(**_read(sub["spot"], "spot", _SPOT)),
        deadlines=Deadlines(**_read(sub["deadlines"], "deadlines", _DEADLINES)),
        publish=PublishSettings(**_read(sub["publish"], "publish", _PUBLISH)),
        uploads=UploadSettings(**_read(sub["uploads"], "uploads", _UPLOADS)),
    )


def load_settings(kube: Kube, namespace: str) -> Settings:
    """GET ConfigMap `k8s-inspector`, `yaml.safe_load` its `config.yaml`, `parse_settings`. A missing
    ConfigMap yields the defaults (no vendor enabled)."""
    # `kube` is bound to its own namespace; the argument documents which one is meant.
    try:
        configmap = kube.get("configmap", SETTINGS_CONFIGMAP)
    except NotFound:
        log.info("ConfigMap %s/%s not found, using defaults", namespace, SETTINGS_CONFIGMAP)
        return Settings()
    text = (configmap.get("data") or {}).get(SETTINGS_KEY)
    if text is None:
        return Settings()
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"ConfigMap {SETTINGS_CONFIGMAP} key {SETTINGS_KEY} is not valid YAML: {e}") from e
    return parse_settings(data or {})


def load_config(environ: Mapping[str, str], kube: Kube) -> Config:
    """`load_env` plus `load_settings` for the namespace in the environment."""
    env = load_env(environ)
    return Config(env=env, settings=load_settings(kube, env.namespace))
