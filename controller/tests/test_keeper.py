"""Keeper client against a local HTTP server (no network)."""

import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from k8s_inspector import keeper
from k8s_inspector.model import EnvConfig


@pytest.fixture
def server():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append((self.path, self.headers.get("Authorization")))
            path = urllib.parse.urlsplit(self.path).path
            if path.endswith("/broken/task_block_reason_codes"):
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
