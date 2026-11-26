"""Shared fixtures. Tests never touch a cluster or a cloud: pass fakes into `Kube`/`Storage` consumers."""

import pytest

from k8s_inspector.model import Config, EnvConfig, Settings


@pytest.fixture
def config() -> Config:
    return Config(env=EnvConfig(bucket="test-bucket", bucket_region="eu-test-1"), settings=Settings())


@pytest.fixture(autouse=True)
def no_keeper_cost_data(monkeypatch):
    """The cost data in `run.json` comes from Keeper: tests never ask it (override to give an answer)."""
    from k8s_inspector import keeper

    monkeypatch.setattr(keeper, "ondemand_prices", lambda env, vendor, sku: {})
    monkeypatch.setattr(keeper, "server_facts", lambda env, vendor, sku: None)
