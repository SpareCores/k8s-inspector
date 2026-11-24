import logging
from pathlib import Path

import pytest
import yaml

from k8s_inspector.config import SETTINGS_KEY, load_config, load_env, load_settings, parse_settings
from k8s_inspector.kube import NotFound
from k8s_inspector.model import CapacityMode, Settings, VendorSettings

DOCS_CONFIGURATION = Path(__file__).parents[2] / "docs" / "reference" / "configuration.md"

MINIMAL_ENV = {"K8SI_BUCKET": "b", "K8SI_BUCKET_REGION": "eu-test-1"}


class FakeKube:
    def __init__(self, configmap: dict | None) -> None:
        self.configmap = configmap

    def get(self, kind: str, name: str) -> dict:
        assert (kind, name) == ("configmap", "k8s-inspector")
        if self.configmap is None:
            raise NotFound(name)
        return self.configmap


def documented_example() -> dict:
    """The YAML block of docs/reference/configuration.md, so docs and defaults cannot drift apart."""
    text = DOCS_CONFIGURATION.read_text()
    block = text.split("```yaml\n", 1)[1].split("```", 1)[0]
    return yaml.safe_load(block)


# --------------------------------------------------------------------------- env


def test_env_defaults_and_optionals():
    env = load_env(MINIMAL_ENV)
    assert (env.bucket, env.bucket_region) == ("b", "eu-test-1")
    assert (env.namespace, env.git_branch, env.git_url) == ("inspector", "main", "")
    assert env.role_arn is None and env.s3_endpoint_url is None and env.sc_run_image == ""
    assert env.key_prefix == ""


def test_env_all_variables():
    env = load_env(
        MINIMAL_ENV
        | {
            "K8SI_ROLE_ARN": "arn:aws:iam::1:role/x",
            "K8SI_GIT_URL": "git@example.com:o/r.git",
            "K8SI_GIT_BRANCH": "dev",
            "K8SI_GIT_KEY_FILE": "/keys/git",
            "KEEPER_API_BASE": "https://keeper.test",
            "KEEPER_API_KEY_FILE": "/keys/keeper",
            "K8SI_NAMESPACE": "other",
            "K8SI_SC_RUN_IMAGE": "ghcr.io/sparecores/sc-run@sha256:abc",
            "K8SI_S3_ENDPOINT_URL": "https://s3.test",
            "K8SI_KEY_PREFIX": "k8s-inspector/spikes/",
        }
    )
    assert env.git_branch == "dev" and env.namespace == "other"
    assert env.keeper_api_key_file == "/keys/keeper"
    assert env.sc_run_image.endswith("@sha256:abc") and env.s3_endpoint_url == "https://s3.test"
    assert env.key_prefix == "k8s-inspector/spikes/"


@pytest.mark.parametrize("missing", ["K8SI_BUCKET", "K8SI_BUCKET_REGION"])
def test_env_missing_required_names_the_variable(missing):
    environ = dict(MINIMAL_ENV)
    del environ[missing]
    with pytest.raises(ValueError, match=missing):
        load_env(environ)


def test_env_empty_required_counts_as_missing():
    with pytest.raises(ValueError, match="K8SI_BUCKET"):
        load_env(MINIMAL_ENV | {"K8SI_BUCKET": ""})


# --------------------------------------------------------------------------- settings


def test_empty_settings_are_the_defaults():
    assert parse_settings({}) == Settings()
    assert parse_settings({"planner": None, "vendors": None}) == Settings()


def test_defaults_equal_documented_example():
    parsed = parse_settings(documented_example())
    assert parsed.vendors == {
        "oci": VendorSettings(
            enabled=True,
            auto_plan=False,
            max_in_flight=2,
            max_usd_per_hour=2.0,
            capacity=CapacityMode.SPOT_FIRST,
        )
    }
    # Everything except the example vendor equals the dataclass defaults.
    assert parsed == Settings(vendors=parsed.vendors)


def test_values_override_defaults():
    settings = parse_settings(
        {
            "planner": {"intervalMinutes": 10, "autoPlan": True},
            "defaults": {"maxInFlight": 3},
            "uploads": {"partMiB": 8},
            "vendors": {"aws": {"enabled": True, "maxInFlight": 0}},
        }
    )
    assert settings.planner_interval_minutes == 10 and settings.auto_plan is True
    assert settings.defaults.max_in_flight == 3
    assert settings.uploads.part_mib == 8 and settings.uploads.max_parts == 64
    aws = settings.vendors["aws"]
    assert aws.max_in_flight == 0
    assert aws.capacity == CapacityMode.SPOT_FIRST and not aws.publish and aws.auto_plan is None


def test_bench_class_setting():
    settings = parse_settings({})
    assert settings.bench_class == "bench"
    assert parse_settings({"benchClass": "b"}).bench_class == "b"


def test_bench_pool_defaults_and_overrides(caplog):
    assert parse_settings({}).bench_pool == "bench"
    assert parse_settings({"benchPool": "other"}).bench_pool == "other"
    with caplog.at_level(logging.WARNING):
        parse_settings({"vendors": {"oci": {"nodePools": {"cpu": "bench-oci"}}}})
    assert "vendors.oci.nodePools" in caplog.text


def test_unknown_keys_warn_but_load(caplog):
    with caplog.at_level(logging.WARNING):
        settings = parse_settings(
            {"bogus": 1, "spot": {"maxSpotAttempt": 3, "maxSpotAttempts": 4}, "vendors": {"oci": {"enabld": True}}}
        )
    assert settings.spot.max_spot_attempts == 4
    assert "vendors.oci.enabld" in caplog.text and "spot.maxSpotAttempt " in caplog.text
    assert "bogus" in caplog.text


@pytest.mark.parametrize(
    ("data", "path"),
    [
        ({"planner": {"intervalMinutes": "5"}}, "planner.intervalMinutes"),
        ({"planner": {"autoPlan": "yes"}}, "planner.autoPlan"),
        ({"defaults": {"maxInFlight": -1}}, "defaults.maxInFlight"),
        ({"defaults": {"maxInFlight": True}}, "defaults.maxInFlight"),
        ({"uploads": {"partMiB": 0}}, "uploads.partMiB"),
        ({"vendors": {"oci": {"capacity": "Spot"}}}, "vendors.oci.capacity"),
        ({"vendors": {"oci": {"maxUsdPerHour": "2"}}}, "vendors.oci.maxUsdPerHour"),
        ({"benchPool": 3}, "benchPool"),
        ({"vendors": {"oci": "enabled"}}, "vendors.oci"),
        ({"spot": ["maxSpotAttempts"]}, "spot"),
    ],
)
def test_invalid_values_raise_naming_the_key(data, path):
    with pytest.raises(ValueError, match=path.replace(".", r"\.")):
        parse_settings(data)


# --------------------------------------------------------------------------- ConfigMap


def test_load_settings_reads_config_yaml_key():
    kube = FakeKube({"data": {SETTINGS_KEY: "planner:\n  intervalMinutes: 7\nvendors:\n  oci: {enabled: true}\n"}})
    settings = load_settings(kube, "inspector")
    assert settings.planner_interval_minutes == 7 and settings.vendors["oci"].enabled


@pytest.mark.parametrize("configmap", [None, {}, {"data": {}}, {"data": {SETTINGS_KEY: ""}}])
def test_load_settings_missing_or_empty_gives_defaults(configmap):
    assert load_settings(FakeKube(configmap), "inspector") == Settings()


def test_load_settings_tells_whether_they_come_from_the_configmap():
    assert not load_settings(FakeKube(None), "inspector").from_configmap
    assert load_settings(FakeKube({"data": {SETTINGS_KEY: "planner: {autoPlan: true}"}}), "inspector").from_configmap


def test_load_settings_invalid_yaml_is_a_clear_error():
    with pytest.raises(ValueError, match="not valid YAML"):
        load_settings(FakeKube({"data": {SETTINGS_KEY: "a: [unclosed"}}), "inspector")


def test_load_config_combines_env_and_settings():
    config = load_config(MINIMAL_ENV | {"K8SI_NAMESPACE": "ns"}, FakeKube(None))
    assert config.env.namespace == "ns" and config.settings == Settings()


def test_vendor_publish_is_a_bool_or_a_list_of_skus():
    parse = lambda value: parse_settings({"vendors": {"aws": {"publish": value}}}).vendors["aws"].publish  # noqa: E731
    assert parse(True) is True and parse(["a", "b"]) == ("a", "b")
    with pytest.raises(ValueError, match="vendors.aws.publish"):
        parse("yes")


def test_max_in_flight_global_default_and_vendor_override():
    settings = parse_settings(
        {
            "defaults": {"maxInFlight": 4},
            "vendors": {"aws": {"enabled": True}, "oci": {"enabled": True, "maxInFlight": 0}},
        }
    )
    assert settings.defaults.max_in_flight == 4
    assert settings.vendors["aws"].max_in_flight is None  # falls back to defaults.maxInFlight
    assert settings.vendors["oci"].max_in_flight == 0  # pauses the vendor


# --------------------------------------------------------------------------- DBaaS


def test_dbaas_settings_and_the_vendor_flag():
    parsed = parse_settings(
        {
            "vendors": {"gcp": {"enabled": True, "dbaasAutoPlan": True}, "aws": {"enabled": True}},
            "dbaas": {
                "engineVersion": 17,  # YAML reads a bare 17 as a number
                "createMinutes": 45,
                "sweepAfterHours": 4,
                "vendors": {
                    "gcp": {"serviceAccount": "gcp-dbaas", "networks": {"europe-west4": {"network": "projects/x/y"}}},
                    "aws": None,
                },
            },
        }
    )
    assert parsed.vendors["gcp"].dbaas_auto_plan is True and parsed.vendors["aws"].dbaas_auto_plan is False
    assert (parsed.dbaas.engine_version, parsed.dbaas.create_minutes, parsed.dbaas.sweep_after_hours) == ("17", 45, 4)
    gcp = parsed.dbaas.vendors["gcp"]
    assert gcp.service_account == "gcp-dbaas" and gcp.networks == {"europe-west4": {"network": "projects/x/y"}}
    assert parsed.dbaas.vendors["aws"].networks == {} and parsed.dbaas.vendors["aws"].service_account is None


def test_dbaas_limits_have_their_own_keys_next_to_the_server_limits():
    parsed = parse_settings(
        {
            "vendors": {
                "gcp": {"maxInFlight": 3, "maxUsdPerHour": 2.0, "dbaasMaxInFlight": 2, "dbaasMaxUsdPerHour": 1.5},
                "aws": {"dbaasMaxInFlight": 0},
            }
        }
    )
    gcp, aws = parsed.vendors["gcp"], parsed.vendors["aws"]
    assert (gcp.max_in_flight, gcp.max_usd_per_hour, gcp.dbaas_max_in_flight, gcp.dbaas_max_usd_per_hour) == (
        3,
        2.0,
        2,
        1.5,
    )
    assert (aws.dbaas_max_in_flight, aws.dbaas_max_usd_per_hour) == (0, None)  # 0 pauses, no cap
    assert VendorSettings().dbaas_max_in_flight == 1 and VendorSettings().dbaas_max_usd_per_hour is None


def test_dbaas_defaults_are_off():
    parsed = parse_settings({})
    assert (parsed.dbaas.engine_version, parsed.dbaas.create_minutes, parsed.dbaas.sweep_after_hours) == ("18", 60, 6)
    assert parsed.dbaas.vendors == {} and VendorSettings().dbaas_auto_plan is False


@pytest.mark.parametrize(
    "data",
    [
        {"dbaas": {"createMinutes": 0}},
        {"dbaas": {"engineVersion": ""}},
        {"dbaas": {"vendors": {"gcp": {"networks": {"r": "not a mapping"}}}}},
        {"dbaas": {"vendors": {"gcp": {"serviceAccount": ""}}}},
        {"vendors": {"gcp": {"dbaasAutoPlan": "yes"}}},
        {"vendors": {"gcp": {"dbaasMaxInFlight": -1}}},
        {"vendors": {"gcp": {"dbaasMaxUsdPerHour": "cheap"}}},
    ],
)
def test_invalid_dbaas_values_name_the_key(data):
    with pytest.raises(ValueError, match="invalid config value for"):
        parse_settings(data)


def test_unknown_dbaas_keys_warn_and_are_ignored(caplog):
    with caplog.at_level(logging.WARNING):
        parse_settings({"dbaas": {"bogus": 1, "vendors": {"gcp": {"zone": "x"}}}})
    assert "unknown config key dbaas.bogus" in caplog.text and "dbaas.vendors.gcp.zone" in caplog.text


def test_the_dbaas_image_is_an_optional_environment_variable():
    assert load_env(MINIMAL_ENV).dbaas_image == ""
    assert load_env({**MINIMAL_ENV, "K8SI_DBAAS_IMAGE": "img@sha256:1"}).dbaas_image == "img@sha256:1"
    assert load_env(MINIMAL_ENV).dbaas_role_arn is None
    assert load_env({**MINIMAL_ENV, "K8SI_DBAAS_ROLE_ARN": "arn:r"}).dbaas_role_arn == "arn:r"


def test_client_region_by_city_is_a_per_vendor_flag_that_is_off_by_default():
    parsed = parse_settings({"dbaas": {"vendors": {"ovh": {"clientRegionByCity": True}, "oci": {}}}})
    assert parsed.dbaas.vendors["ovh"].client_region_by_city is True
    assert parsed.dbaas.vendors["oci"].client_region_by_city is False
