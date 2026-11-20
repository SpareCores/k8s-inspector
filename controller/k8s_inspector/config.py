"""Load configuration: environment variables and the ConfigMap `k8s-inspector` into `model.Config`.

Owner: config agent. Implements docs/reference/configuration.md. The YAML uses camelCase keys
(`maxInFlight`); the dataclasses use snake_case. Missing keys take the dataclass defaults.
"""

from collections.abc import Mapping

from k8s_inspector.kube import Kube
from k8s_inspector.model import Config, EnvConfig, Settings

SETTINGS_CONFIGMAP = "k8s-inspector"
SETTINGS_KEY = "config.yaml"  # key inside the ConfigMap's data


def load_env(environ: Mapping[str, str]) -> EnvConfig:
    """Read the `K8SI_*`, `KEEPER_*`, `SENTRY_DSN` variables. Raise `ValueError` naming the missing
    required variable (K8SI_BUCKET, K8SI_BUCKET_REGION). Never log values."""
    raise NotImplementedError


def parse_settings(data: Mapping) -> Settings:
    """Convert the parsed ConfigMap YAML (a dict, possibly empty) into `Settings`. Unknown keys raise
    `ValueError` (typos must not pass silently)."""
    raise NotImplementedError


def load_settings(kube: Kube, namespace: str) -> Settings:
    """GET ConfigMap `k8s-inspector`, `yaml.safe_load` its `config.yaml`, `parse_settings`. A missing
    ConfigMap yields the defaults (no vendor enabled)."""
    raise NotImplementedError


def load_config(environ: Mapping[str, str], kube: Kube) -> Config:
    """`load_env` plus `load_settings` for the namespace in the environment."""
    raise NotImplementedError
