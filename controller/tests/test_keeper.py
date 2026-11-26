"""Keeper client against a local HTTP server (no network)."""

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from k8s_inspector import keeper
from k8s_inspector.model import EnvConfig

server_facts, ondemand_prices = keeper.server_facts, keeper.ondemand_prices  # conftest stubs the module's names


@pytest.fixture
def server():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append((self.path, self.headers.get("Authorization")))
            path = urllib.parse.urlsplit(self.path).path
            if path == "/inspector/servers/task_block_reason_codes":
                self.send_response(200)
                body = json.dumps(
                    {"oci": {"s1": {"membench": [], "nvidia_smi": ["No GPUs available."]}, "s2": {}}}
                ).encode()
            elif path.endswith("/broken/task_block_reason_codes"):
                self.send_response(500)
                body = b"oops"
            elif path.endswith("/gone/task_block_reason_codes"):
                self.send_response(404)
                body = b"{}"
            elif path.endswith("/garbled/task_block_reason_codes"):
                self.send_response(200)
                body = b"[not json"
            else:
                self.send_response(200)
                body = json.dumps({"membench": [], "nvidia_smi": ["No GPUs available."]}).encode()
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_port}", seen
    httpd.shutdown()


def env_for(base: str, key_file: str | None = None) -> EnvConfig:
    return EnvConfig(bucket="b", bucket_region="r", keeper_api_base=base, keeper_api_key_file=key_file)


def test_only_blocked_tasks_are_listed(server, tmp_path):
    base, seen = server
    key = tmp_path / "key"
    key.write_text("secret-token\n")
    out = keeper.block_reasons(env_for(base + "/", str(key)), "oci", ["VM.A.1-2"], ["membench", "nvidia_smi"])
    assert out == {"VM.A.1-2": {"nvidia_smi": ["No GPUs available."]}}
    path, auth = seen[0]
    assert path.startswith("/inspector/server/oci/VM.A.1-2/task_block_reason_codes?")
    assert path.endswith("tasks=membench&tasks=nvidia_smi") and auth == "Bearer secret-token"


def test_missing_key_file_means_no_key(server, tmp_path):
    base, seen = server
    keeper.block_reasons(env_for(base, str(tmp_path / "absent")), "oci", ["VM.A.1-2"], ["membench"])
    assert seen[0][1] is None


def test_unknown_sku_is_blocked_for_every_task(server):
    out = keeper.block_reasons(env_for(server[0]), "oci", ["gone"], ["membench", "stressng"])
    assert out == {"gone": {"membench": [keeper.UNKNOWN_SKU], "stressng": [keeper.UNKNOWN_SKU]}}


@pytest.mark.parametrize("sku", ["broken", "garbled"])
def test_errors_fail_closed(server, sku):
    with pytest.raises(RuntimeError):
        keeper.block_reasons(env_for(server[0]), "oci", ["ok", sku], ["membench"])


def test_unreachable_keeper_raises():
    with pytest.raises(RuntimeError):
        keeper.block_reasons(env_for("http://127.0.0.1:1"), "oci", ["x"], ["membench"])


def test_active_servers_come_in_one_call_with_only_blocked_tasks(server):
    base, seen = server
    result = keeper.active_servers_block_reasons(env_for(base), ["membench", "nvidia_smi"])
    assert result == {"oci": {"s1": {"nvidia_smi": ["No GPUs available."]}, "s2": {}}}
    assert [path for path, _ in seen] == ["/inspector/servers/task_block_reason_codes?tasks=membench&tasks=nvidia_smi"]


def test_active_servers_fail_closed_when_unreachable():
    with pytest.raises(RuntimeError):
        keeper.active_servers_block_reasons(env_for("http://127.0.0.1:1"), ["membench"])


REAL_OCI_SHAPE = {  # trimmed from the live bulk response (reasons are free text)
    "oci": {
        "BM.DenseIO.A4.Ax.72": {
            "lscpu": [],
            "geekbench": ["Benchmark doesn't scale beyond 32 cores."],
            "stressngfull": [],
            "llm": [],
            "stressnglongrun": ["Benchmark limited to a few SKUs."],
            "nvidia_smi": ["No GPUs available."],
        },
        "VM.Optimized3.Flex.1-4": {name: [] for name in ("lscpu", "geekbench", "stressngfull", "llm")}
        | {"stressnglongrun": ["Benchmark limited to a few SKUs."], "nvidia_smi": ["No GPUs available."]},
    }
}


def test_real_bulk_response_keeps_oci_skus_with_their_blocked_tasks(monkeypatch):
    monkeypatch.setattr(keeper, "_get", lambda url, key: REAL_OCI_SHAPE)
    result = keeper.active_servers_block_reasons(
        EnvConfig(bucket="b", bucket_region="r"), list(REAL_OCI_SHAPE["oci"]["BM.DenseIO.A4.Ax.72"])
    )
    assert set(result["oci"]) == {"BM.DenseIO.A4.Ax.72", "VM.Optimized3.Flex.1-4"}
    assert set(result["oci"]["VM.Optimized3.Flex.1-4"]) == {"stressnglongrun", "nvidia_smi"}  # lscpu, llm... eligible
    assert set(result["oci"]["BM.DenseIO.A4.Ax.72"]) == {"geekbench", "stressnglongrun", "nvidia_smi"}


def test_server_facts_come_from_the_v2_server_record(monkeypatch):
    seen = []
    record = {"family": "Compute Optimized", "cpu_architecture": "x86_64", "accelerator_count": 4.0, "series": "c6a"}
    monkeypatch.setattr(keeper, "_get", lambda url, key: seen.append(url) or record)
    env = EnvConfig(bucket="b", bucket_region="r", keeper_api_base="http://k")
    facts = server_facts(env, "aws", "c6a.12xlarge")
    assert facts == {"family": "Compute Optimized", "series": "c6a", "cpu_arch": "x86_64", "gpu_count": 4}
    assert seen == ["http://k/v2/server/aws/c6a.12xlarge"]


def test_unknown_server_has_no_facts(monkeypatch):
    monkeypatch.setattr(keeper, "_get", lambda url, key: None)
    assert server_facts(EnvConfig(bucket="b", bucket_region="r"), "aws", "nope") is None


def fake_prices(monkeypatch, rows):
    regions = [{"api_reference": f"eu-{i}", "region_id": rid, "vendor_id": "gcp"} for i, rid in enumerate("123")]
    monkeypatch.setattr(keeper, "_fetch", lambda url, key: rows if "/prices" in url else regions)


def test_ondemand_prices_are_the_cheapest_active_zone_per_region_by_api_reference(monkeypatch):
    def row(region, allocation, price, status="active"):
        return {"region_id": region, "allocation": allocation, "price": price, "status": status}

    fake_prices(
        monkeypatch,
        [
            row("1", "ondemand", 1.2),
            row("1", "ondemand", 1.1),
            row("1", "spot", 0.4),
            row("2", "ondemand", 2.0, status="inactive"),
            row("9", "ondemand", 3.0),  # not a region of the vendor
        ],
    )
    prices = ondemand_prices(EnvConfig(bucket="b", bucket_region="r"), "gcp", "n2")
    assert prices == {"eu-0": 1.1, "eu-1": None, "eu-2": None}  # None: no on-demand offer
    assert keeper.ondemand_price_fields(prices, "eu-0") == {"ondemand_price": 1.1}
    assert keeper.ondemand_price_fields(prices, "eu-1") == {
        "ondemand_price": None,
        "ondemand_price_reason": "no_ondemand_offer",
    }
    assert keeper.ondemand_price_fields(prices, "elsewhere")["ondemand_price_reason"] == "region_not_in_keeper"


def test_an_sku_unknown_to_keeper_has_no_prices(monkeypatch):
    monkeypatch.setattr(keeper, "_fetch", lambda url, key: None)
    prices = ondemand_prices(EnvConfig(bucket="b", bucket_region="r"), "aws", "nope")
    assert prices is None
    assert keeper.ondemand_price_fields(prices, "eu-0")["ondemand_price_reason"] == "keeper_unknown_sku"
