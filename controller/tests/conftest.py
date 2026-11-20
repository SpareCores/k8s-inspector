"""Shared fixtures. Tests never touch a cluster or a cloud: pass fakes into `Kube`/`Storage` consumers."""

import pytest

from k8s_inspector.model import Config, EnvConfig, Settings


@pytest.fixture
def config() -> Config:
    return Config(env=EnvConfig(bucket="test-bucket", bucket_region="eu-test-1"), settings=Settings())
