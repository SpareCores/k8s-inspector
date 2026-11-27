"""Load configuration: environment variables and the ConfigMap `k8s-inspector` into `model.Config`.

Owner: config agent. Implements docs/reference/configuration.md. The YAML uses camelCase keys
(`maxInFlight`); the dataclasses use snake_case. Missing keys take the dataclass defaults, which equal the
documented example. Unknown keys log a warning (a newer ConfigMap must not stop an older controller); invalid
values raise `ValueError` naming the key.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

import yaml

from k8s_inspector.kube import Kube, NotFound
from k8s_inspector.model import (
    UNLIMITED,
    CapacityMode,
    Config,
    DbaasSettings,
    DbaasVendor,
    Deadlines,
    Defaults,
    EnvConfig,
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
    """Read the `K8SI_*`, `KEEPER_*` variables. Raise `ValueError` naming the missing
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
        namespace=optional("K8SI_NAMESPACE") or "inspector",
        sc_run_image=optional("K8SI_SC_RUN_IMAGE") or "",
        s3_endpoint_url=optional("K8SI_S3_ENDPOINT_URL"),
        key_prefix=optional("K8SI_KEY_PREFIX") or "",
        dbaas_image=optional("K8SI_DBAAS_IMAGE") or "",
        dbaas_role_arn=optional("K8SI_DBAAS_ROLE_ARN"),
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


def _max_in_flight(value: Any, path: str) -> int | float:
    """A non-negative integer, or `unlimited`."""
    return UNLIMITED if value == "unlimited" else _nonneg_int(value, path)


def _nonneg_float(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise _fail(path, "a non-negative number", value)
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


def _version(value: Any, path: str) -> str:
    """A PostgreSQL major version; YAML reads `18` as a number."""
    if isinstance(value, bool) or not isinstance(value, int | float | str) or not str(value).strip():
        raise _fail(path, 'a PostgreSQL major version such as "18"', value)
    return str(value).strip()


def _publish(value: Any, path: str) -> bool | tuple[str, ...]:
    """`true`, `false` or a list of SKUs."""
    if isinstance(value, list):
        return tuple(_str(sku, f"{path}[]") for sku in value)
    return _bool(value, path)


def _optional(check: Callable[[Any, str], Any]) -> Callable[[Any, str], Any]:
    """The same check, but `null` is allowed."""
    return lambda value, path: None if value is None else check(value, path)


# --------------------------------------------------------------------------- settings

# camelCase key -> (dataclass field, check). One table per section.
_PLANNER = {"intervalMinutes": ("planner_interval_minutes", _pos_int), "autoPlan": ("auto_plan", _bool)}
_DEFAULTS = {"maxInFlight": ("max_in_flight", _max_in_flight)}
_SPOT = {
    "maxSpotAttempts": ("max_spot_attempts", _pos_int),
    "maxLostMinutes": ("max_lost_minutes", _nonneg_int),
    "interruptionsToQuarantine": ("interruptions_to_quarantine", _pos_int),
    "quarantineDays": ("quarantine_days", _nonneg_int),
    "spotMaxTaskMinutes": ("spot_max_task_minutes", _pos_int),
    "quotaReserveAfterMinutes": ("quota_reserve_after_minutes", _nonneg_int),
}
_DEADLINES = {
    "provisionCpuMinutes": ("provision_cpu_minutes", _pos_int),
    "provisionGpuHours": ("provision_gpu_hours", _pos_int),
    "registrationMinutes": ("registration_minutes", _pos_int),
    "registrationMinutesBareMetal": ("registration_minutes_bare_metal", _pos_int),
    "readyMinutes": ("ready_minutes", _pos_int),
    "readyMinutesBareMetal": ("ready_minutes_bare_metal", _pos_int),
    "readyMinutesGpu": ("ready_minutes_gpu", _pos_int),
    "anchorReadyMinutes": ("anchor_ready_minutes", _pos_int),
    "noOfferingMinutes": ("no_offering_minutes", _pos_int),
    "noOfferingFastMinutes": ("no_offering_fast_minutes", _pos_int),
    "imagePullMinutes": ("image_pull_minutes", _pos_int),
    "holdIdleMinutes": ("hold_idle_minutes", _pos_int),
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
    "maxInFlight": ("max_in_flight", _optional(_max_in_flight)),
    "maxUsdPerHour": ("max_usd_per_hour", _optional(_nonneg_float)),
    "maxNewRunsPerCycle": ("max_new_runs_per_cycle", _pos_int),
    "capacity": ("capacity", _capacity),
    "publish": ("publish", _publish),
    "dbaasAutoPlan": ("dbaas_auto_plan", _bool),
    "dbaasMaxInFlight": ("dbaas_max_in_flight", _nonneg_int),
    "dbaasMaxUsdPerHour": ("dbaas_max_usd_per_hour", _optional(_nonneg_float)),
}
_DBAAS = {
    "engineVersion": ("engine_version", _version),
    "createMinutes": ("create_minutes", _pos_int),
    "sweepAfterHours": ("sweep_after_hours", _pos_int),
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


def _vendor(value: Any, path: str) -> VendorSettings:
    return VendorSettings(**_read(_mapping(value, path), path, _VENDOR))


def _dbaas_vendor(value: Any, path: str) -> DbaasVendor:
    section = dict(_mapping(value, path))
    networks = {}
    for region, network in _mapping(section.pop("networks", None), f"{path}.networks").items():
        networks[str(region)] = dict(_mapping(network, f"{path}.networks.{region}"))
    service_account = section.pop("serviceAccount", None)
    by_city = section.pop("clientRegionByCity", False)
    for key in section:
        log.warning("unknown config key %s.%s (ignored)", path, key)
    return DbaasVendor(
        networks=networks,
        service_account=None if service_account is None else _str(service_account, f"{path}.serviceAccount"),
        client_region_by_city=_bool(by_city, f"{path}.clientRegionByCity"),
    )


def _dbaas(value: Any, path: str) -> DbaasSettings:
    section = dict(_mapping(value, path))
    vendors = _mapping(section.pop("vendors", None), f"{path}.vendors")
    return DbaasSettings(
        **_read(section, path, _DBAAS),
        vendors={str(v): _dbaas_vendor(c, f"{path}.vendors.{v}") for v, c in vendors.items()},
    )


def parse_settings(data: Mapping) -> Settings:
    """Convert the parsed ConfigMap YAML (a dict, possibly empty) into `Settings`. Unknown keys are logged
    as warnings; invalid values raise `ValueError`."""
    sections = dict(_mapping(data, "config"))
    sub = {
        name: _mapping(sections.pop(name, None), name)
        for name in ("planner", "defaults", "spot", "deadlines", "publish", "uploads")
    }
    vendors = _mapping(sections.pop("vendors", None), "vendors")
    bench_pool = _str(sections.pop("benchPool", "bench"), "benchPool")
    bench_class = _str(sections.pop("benchClass", "bench"), "benchClass")
    dbaas = _dbaas(sections.pop("dbaas", None), "dbaas")
    for key in sections:
        log.warning("unknown config key %s (ignored)", key)

    return Settings(
        **_read(sub["planner"], "planner", _PLANNER),
        bench_pool=bench_pool,
        bench_class=bench_class,
        vendors={str(name): _vendor(value, f"vendors.{name}") for name, value in vendors.items()},
        defaults=Defaults(**_read(sub["defaults"], "defaults", _DEFAULTS)),
        spot=SpotSettings(**_read(sub["spot"], "spot", _SPOT)),
        deadlines=Deadlines(**_read(sub["deadlines"], "deadlines", _DEADLINES)),
        publish=PublishSettings(**_read(sub["publish"], "publish", _PUBLISH)),
        uploads=UploadSettings(**_read(sub["uploads"], "uploads", _UPLOADS)),
        dbaas=dbaas,
    )


def load_settings(kube: Kube, namespace: str) -> Settings:
    """GET ConfigMap `k8s-inspector`, `yaml.safe_load` its `config.yaml`, `parse_settings`. A missing
    ConfigMap or an empty `config.yaml` yields the defaults (no vendor enabled, `from_configmap` false: the
    caller must not treat that as a decision, e.g. to delete runs)."""
    # `kube` is bound to its own namespace; the argument documents which one is meant.
    try:
        configmap = kube.get("configmap", SETTINGS_CONFIGMAP)
    except NotFound:
        log.info("ConfigMap %s/%s not found, using defaults", namespace, SETTINGS_CONFIGMAP)
        return Settings()
    text = (configmap.get("data") or {}).get(SETTINGS_KEY)
    if not text:
        return Settings()
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"ConfigMap {SETTINGS_CONFIGMAP} key {SETTINGS_KEY} is not valid YAML: {e}") from e
    if not data:
        return Settings()
    return replace(parse_settings(data), from_configmap=True)


def load_config(environ: Mapping[str, str], kube: Kube) -> Config:
    """`load_env` plus `load_settings` for the namespace in the environment."""
    env = load_env(environ)
    return Config(env=env, settings=load_settings(kube, env.namespace))
