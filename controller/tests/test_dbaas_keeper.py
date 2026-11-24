"""Keeper's database and server data for DBaaS, against a local HTTP server (no network)."""

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from k8s_inspector import dbaas_keeper
from k8s_inspector.model import EnvConfig

PRICES = [
    {"region_id": "1000", "ha": "none", "allocation": "ondemand", "status": "active", "price": 1.0},
    {"region_id": "1000", "ha": "none", "allocation": "ondemand", "status": "active", "price": 0.9},
    {"region_id": "1100", "ha": "none", "allocation": "ondemand", "status": "active", "price": 1.2},
    {"region_id": "1200", "ha": "none", "allocation": "ondemand", "status": "inactive", "price": 0.1},
    {"region_id": "1300", "ha": "none", "allocation": "spot", "status": "active", "price": 0.1},
    {"region_id": "1000", "ha": "multi-zone", "allocation": "ondemand", "status": "active", "price": 2.0},
    {"region_id": "9999", "ha": "none", "allocation": "ondemand", "status": "active", "price": 0.1},  # unknown region
]
AWS_PRICES = [  # AWS RDS lists only zonal levels: single-zone is the standalone one
    {"region_id": "eu-west-1", "ha": "single-zone", "allocation": "ondemand", "status": "active", "price": 0.2},
    {"region_id": "eu-west-1", "ha": "multi-zone", "allocation": "ondemand", "status": "active", "price": 0.4},
]


@pytest.fixture
def keeper():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append((self.path, self.headers.get("Authorization")))
            parts = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parts.query)
            path = parts.path
            if path == "/databases":
                body = [
                    {"api_reference": "db-a", "engine": "postgresql", "vcpus": 4},
                    {"api_reference": "my-sql", "engine": "mysql", "vcpus": 2},
                ]
            elif path == "/regions":
                body = [
                    {"vendor_id": "gcp", "region_id": "1000", "api_reference": "us-central1"},
                    {"vendor_id": "gcp", "region_id": "1100", "api_reference": "europe-west4", "city": "Eemshaven"},
                    {"vendor_id": "gcp", "region_id": "1200", "api_reference": "asia-east1"},
                    {"vendor_id": "gcp", "region_id": "1300", "api_reference": "asia-east2"},
                ]
                if query.get("vendor") == ["aws"]:
                    body = [{"vendor_id": "aws", "region_id": "eu-west-1", "api_reference": "eu-west-1"}]
            elif path.endswith("/prices"):
                body = AWS_PRICES if "/aws/" in path else PRICES
            elif path == "/servers":
                body = [{"api_reference": "e2-medium", "min_price_ondemand": 0.03}]
            elif path == "/inspector/databases/task_block_reason_codes":
                body = {"gcp": {"db-a": {"t": []}, "db-b": {"t": ["No budget."]}}}
            elif path == "/inspector/database/gcp/db-a/task_block_reason_codes":
                body = {"t": ["No budget."]}
            elif path == "/database/gcp/db-a":
                body = {"api_reference": "db-a", "vcpus": 4}
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"{}")
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield EnvConfig(bucket="b", bucket_region="r", keeper_api_base=f"http://127.0.0.1:{httpd.server_port}"), seen
    httpd.shutdown()


def test_the_vendors_active_postgres_databases_are_listed(keeper):
    env, seen = keeper
    assert [d["api_reference"] for d in dbaas_keeper.databases(env, "gcp")] == ["db-a"]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(seen[0][0]).query)
    assert query == {"vendor": ["gcp"], "only_active": ["true"], "limit": ["-1"]}


def test_a_database_is_read_by_vendor_and_id_and_an_unknown_one_is_none(keeper):
    env, _ = keeper
    assert dbaas_keeper.database(env, "gcp", "db-a")["vcpus"] == 4
    assert dbaas_keeper.database(env, "gcp", "nope") is None


def test_block_reasons_of_every_database_and_of_one(keeper):
    env, _ = keeper
    assert dbaas_keeper.block_reasons(env, ["t"]) == {"gcp": {"db-a": {}, "db-b": {"t": ["No budget."]}}}
    assert dbaas_keeper.database_block_reasons(env, "gcp", "db-a", ["t"]) == {"t": ["No budget."]}
    assert dbaas_keeper.database_block_reasons(env, "gcp", "gone", ["t"]) == {"t": ["KEEPER_UNKNOWN_SKU"]}


def test_the_standalone_price_per_region_uses_active_on_demand_rows_of_the_none_level(keeper):
    env, _ = keeper
    prices = dbaas_keeper.standalone_prices(env, "gcp", "db-a")
    assert prices == {
        "us-central1": 0.9,
        "europe-west4": 1.2,
    }  # cheapest row per region; no HA, spot, inactive, unknown


def test_regions_with_a_city_are_listed_by_it(keeper):
    env, _ = keeper
    assert dbaas_keeper.region_cities(env, "gcp") == {"europe-west4": "Eemshaven"}  # the others have no city


def test_a_vendor_that_lists_only_zonal_levels_uses_single_zone(keeper):
    env, _ = keeper
    assert dbaas_keeper.standalone_prices(env, "aws", "db.m5.large") == {"eu-west-1": 0.2}


def test_client_servers_are_asked_for_in_the_region_by_its_id(keeper):
    env, seen = keeper
    assert dbaas_keeper.client_servers(env, "gcp", "1000")[0]["api_reference"] == "e2-medium"
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(seen[-1][0]).query)
    assert query["regions"] == ["1000"] and query["vcpus_min"] == ["2"] and query["vcpus_max"] == ["4"]
    assert (
        query["architecture"] == ["x86_64"]
        and query["gpu_max"] == ["0"]
        and query["best_price_allocation"] == ["ONDEMAND_ONLY"]
    )


def test_every_keeper_failure_is_a_runtime_error_so_the_callers_fail_closed():
    env = EnvConfig(bucket="b", bucket_region="r", keeper_api_base="http://127.0.0.1:1")
    for call in (
        lambda: dbaas_keeper.databases(env, "gcp"),
        lambda: dbaas_keeper.block_reasons(env, ["t"]),
        lambda: dbaas_keeper.standalone_prices(env, "gcp", "db-a"),
    ):
        with pytest.raises(RuntimeError):
            call()


def test_the_cache_asks_keeper_once_per_period(monkeypatch):
    calls = []
    monkeypatch.setattr(dbaas_keeper, "databases", lambda env, vendor: calls.append(vendor) or [vendor])
    cache = dbaas_keeper.Cache()
    env = EnvConfig(bucket="b", bucket_region="r")
    assert cache.databases(env, "gcp") == cache.databases(env, "gcp") == ["gcp"]
    cache.databases(env, "aws")
    assert calls == ["gcp", "aws"]
    monkeypatch.setattr(dbaas_keeper.time, "monotonic", lambda: 10**9)
    cache.databases(env, "gcp")
    assert calls == ["gcp", "aws", "gcp"]
