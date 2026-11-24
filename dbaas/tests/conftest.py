import pytest

from k8s_inspector_dbaas.spec import DbSpec


@pytest.fixture
def spec() -> DbSpec:
    return DbSpec(
        vendor="fake",
        database_id="db-8",
        engine_version="18",
        region="eu-test-1",
        client_region="eu-test-1",
        zone="eu-test-1a",
        run="fake-m5-large-1234abcd",
        run_id="r20261120t1000-aaaa",
        expires_at="2026-11-20T16:00:00Z",
        labels={"sc_inspector_run": "r20261120t1000-aaaa", "sc_inspector_expires": "20261120t1600z"},
        create_timeout_minutes=50,
        delete_timeout_minutes=100,
        network={},
        record={"vcpus": 8, "memory_amount": 65536},
        password="s3cret-pass",
    )
