"""Kube against a fake API server (httpx2 MockTransport); no cluster."""

import json

import httpx2 as httpx
import lightkube
import pytest
from lightkube import ApiError
from lightkube.config.kubeconfig import KubeConfig
from lightkube.config.models import Cluster, User

from k8s_inspector.kube import AlreadyExists, Conflict, Forbidden, Kube, NotFound, is_transient

CRD_BASE = "/apis/inspector.sparecores.com/v1alpha1/namespaces/inspector/benchmarkruns"


def status_body(code: int, reason: str, message: str = "boom") -> dict:
    return {
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "code": code,
        "reason": reason,
        "message": message,
    }


def make_kube(handler) -> Kube:
    config = KubeConfig.from_one(
        cluster=Cluster(server="https://fake.test"), user=User(token="test"), namespace="inspector"
    )
    client = lightkube.Client(config=config, transport=httpx.MockTransport(handler))
    return Kube("inspector", client=client)


def json_response(code: int, body: dict) -> httpx.Response:
    return httpx.Response(code, json=body)


def pod_list(rv: str, *names: str) -> dict:
    return {
        "kind": "PodList",
        "apiVersion": "v1",
        "metadata": {"resourceVersion": rv},
        "items": [{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": n, "resourceVersion": rv}} for n in names],
    }


def watch_line(event_type: str, name: str, rv: str) -> str:
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "resourceVersion": rv}}
    return json.dumps({"type": event_type, "object": pod})


def test_crd_group_version_plural_match_manifest():
    seen = []

    def handler(request):
        seen.append(request.url.path)
        return json_response(
            200, {"apiVersion": "inspector.sparecores.com/v1alpha1", "kind": "BenchmarkRun", "metadata": {"name": "r"}}
        )

    make_kube(handler).get("benchmarkrun", "r")
    assert seen == [f"{CRD_BASE}/r"]


def test_get_returns_dict_and_node_is_cluster_scoped():
    seen = []

    def handler(request):
        seen.append(request.url.path)
        return json_response(200, {"apiVersion": "v1", "kind": "Node", "metadata": {"name": "n1"}})

    assert make_kube(handler).get("node", "n1")["metadata"]["name"] == "n1"
    assert seen == ["/api/v1/nodes/n1"]


def test_nodeclaim_path():
    seen = []

    def handler(request):
        seen.append(request.url.path)
        return json_response(200, {"apiVersion": "karpenter.sh/v1", "kind": "NodeClaim", "metadata": {"name": "c"}})

    make_kube(handler).get("nodeclaim", "c")
    assert seen == ["/apis/karpenter.sh/v1/nodeclaims/c"]


def test_not_found_is_translated():
    kube = make_kube(lambda request: json_response(404, status_body(404, "NotFound")))
    with pytest.raises(NotFound):
        kube.get("pod", "x")


def test_create_already_exists_vs_conflict():
    kube = make_kube(lambda request: json_response(409, status_body(409, "AlreadyExists")))
    with pytest.raises(AlreadyExists):
        kube.create(
            "benchmarkrun",
            {
                "apiVersion": "inspector.sparecores.com/v1alpha1",
                "kind": "BenchmarkRun",
                "metadata": {"name": "r"},
                "spec": {},
            },
        )

    kube = make_kube(lambda request: json_response(409, status_body(409, "Conflict")))
    with pytest.raises(Conflict):
        kube.merge_patch("benchmarkrun", "r", {"status": {}}, subresource_status=True)


def test_list_passes_label_selector():
    seen = []

    def handler(request):
        seen.append(dict(request.url.params))
        return json_response(200, pod_list("5", "a", "b"))

    pods = make_kube(handler).list("pod", labels={"inspector.sparecores.com/run": "r1"})
    assert [p["metadata"]["name"] for p in pods] == ["a", "b"]
    assert seen[0]["labelSelector"] == "inspector.sparecores.com/run=r1"


def test_delete_ignores_not_found_and_sends_grace():
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, dict(request.url.params)))
        return json_response(404, status_body(404, "NotFound"))

    make_kube(handler).delete("pod", "gone", grace_seconds=0)
    assert seen[0][0:2] == ("DELETE", "/api/v1/namespaces/inspector/pods/gone")
    assert seen[0][2]["gracePeriodSeconds"] == "0"


def test_delete_of_a_missing_nodeclaim_is_ignored_and_other_errors_raise():
    make_kube(lambda request: json_response(404, status_body(404, "NotFound"))).delete("nodeclaim", "gone")
    with pytest.raises(Forbidden):
        make_kube(lambda request: json_response(403, status_body(403, "Forbidden"))).delete("nodeclaim", "x")


def test_merge_patch_status_subresource_with_resource_version():
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, request.headers["content-type"], json.loads(request.content)))
        return json_response(
            200, {"apiVersion": "inspector.sparecores.com/v1alpha1", "kind": "BenchmarkRun", "metadata": {"name": "r"}}
        )

    body = {"metadata": {"resourceVersion": "7"}, "status": {"phase": "Running"}}
    make_kube(handler).merge_patch("benchmarkrun", "r", body, subresource_status=True)
    method, path, content_type, sent = seen[0]
    assert (method, path) == ("PATCH", f"{CRD_BASE}/r/status")
    assert content_type == "application/merge-patch+json"
    assert sent == body


def test_json_patch_sends_the_operations_as_rfc_6902():
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, request.headers["content-type"], json.loads(request.content)))
        return json_response(
            200, {"apiVersion": "inspector.sparecores.com/v1alpha1", "kind": "BenchmarkRun", "metadata": {"name": "r"}}
        )

    operations = [
        {"op": "test", "path": "/metadata/finalizers/0", "value": "f"},
        {"op": "remove", "path": "/metadata/finalizers/0"},
    ]
    make_kube(handler).json_patch("benchmarkrun", "r", operations)
    assert seen == [("PATCH", f"{CRD_BASE}/r", "application/json-patch+json", operations)]


def test_apply_secret_replaces_when_it_exists():
    seen = []
    manifest = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "run-urls"}, "stringData": {"a": "b"}}

    def handler(request):
        seen.append(request.method)
        if request.method == "POST":
            return json_response(409, status_body(409, "AlreadyExists"))
        return json_response(200, manifest)

    make_kube(handler).apply_secret(manifest)
    assert seen == ["POST", "PUT"]


def test_watch_starts_with_list_and_streams_events():
    def handler(request):
        if request.url.params.get("watch") == "true":
            assert request.url.params["resourceVersion"] == "10"
            return httpx.Response(200, text=watch_line("MODIFIED", "a", "11") + "\n")
        return json_response(200, pod_list("10", "a"))

    events = make_kube(handler).watch("pod")
    first, second = next(events), next(events)
    assert (first.type, first.kind, first.obj["metadata"]["name"]) == ("ADDED", "pod", "a")
    assert (second.type, second.obj["metadata"]["resourceVersion"]) == ("MODIFIED", "11")


def test_watch_relists_on_410_gone():
    calls = {"list": 0, "watch": 0}

    def handler(request):
        if request.url.params.get("watch") == "true":
            calls["watch"] += 1
            if calls["watch"] == 1:
                return json_response(410, status_body(410, "Expired", "too old resource version"))
            assert request.url.params["resourceVersion"] == "20"
            return httpx.Response(200, text=watch_line("ADDED", "c", "21") + "\n")
        calls["list"] += 1
        return json_response(200, pod_list("20", "a", "b"))

    events = make_kube(handler).watch("pod", resource_version="5")
    seen = [next(events) for _ in range(3)]
    assert [(e.type, e.obj["metadata"]["name"]) for e in seen] == [("ADDED", "a"), ("ADDED", "b"), ("ADDED", "c")]
    assert calls == {"list": 1, "watch": 2}


def test_watch_relists_on_410_error_event_in_stream():
    calls = {"watch": 0}

    def handler(request):
        if request.url.params.get("watch") == "true":
            calls["watch"] += 1
            if calls["watch"] == 1:
                return httpx.Response(200, text=json.dumps({"type": "ERROR", "object": status_body(410, "Expired")}))
            return httpx.Response(200, text=watch_line("MODIFIED", "a", "31") + "\n")
        return json_response(200, pod_list("30", "a"))

    events = make_kube(handler).watch("pod", resource_version="5")
    assert [next(events).type for _ in range(2)] == ["ADDED", "MODIFIED"]


@pytest.mark.parametrize(
    ("response", "transient"),
    [
        (httpx.Response(429, json={"kind": "Status", "code": 429}), True),
        (httpx.Response(503, json={"error": "x"}), True),  # a body without a Status code
        (httpx.Response(503, json={"kind": "Status", "message": "no code"}), True),
        (httpx.Response(500, json={"kind": "Status", "code": 500, "message": "storage is (re)initializing"}), True),
        (httpx.Response(403, json={"kind": "Status", "code": 403}), False),
        (httpx.Response(404, json={"error": "x"}), False),
    ],
)
def test_is_transient_api_errors(response, transient):
    assert is_transient(ApiError(response=response)) is transient


def test_is_transient_transport_errors():
    assert is_transient(httpx.ConnectError("refused"))
    assert not is_transient(httpx.UnsupportedProtocol("no scheme"))
    assert not is_transient(ValueError("x"))


def test_list_node_pods_selects_by_node_across_namespaces():
    seen = []

    def handler(request):
        seen.append((request.url.path, request.url.params.get("fieldSelector")))
        return json_response(200, pod_list("1", "flannel", "anchor"))

    found = make_kube(handler).list_node_pods("node-1")
    assert [p["metadata"]["name"] for p in found] == ["flannel", "anchor"]
    assert seen == [("/api/v1/pods", "spec.nodeName=node-1")]


def test_list_node_pods_forbidden_is_translated():
    kube = make_kube(lambda request: json_response(403, status_body(403, "Forbidden")))
    with pytest.raises(Forbidden):
        kube.list_node_pods("node-1")


def test_a_job_is_deleted_in_the_background_so_its_pods_go_too():
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, dict(request.url.params)))
        return json_response(200, {"kind": "Status", "status": "Success"})

    kube = make_kube(handler)
    kube.delete("job", "dbaas-down-aws-x-1")
    assert seen[0][:2] == ("DELETE", "/apis/batch/v1/namespaces/inspector/jobs/dbaas-down-aws-x-1")
    assert seen[0][2]["propagationPolicy"] == "Background"
    kube.delete("pod", "p")
    assert "propagationPolicy" not in seen[1][2]  # a Pod needs no cascade


def test_jobs_are_listed_by_label_in_the_namespace():
    seen = []

    def handler(request):
        seen.append((request.url.path, dict(request.url.params)))
        body = {"apiVersion": "batch/v1", "kind": "JobList", "metadata": {"resourceVersion": "1"}, "items": [
            {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "dbaas-up-x"}}]}  # fmt: skip
        return json_response(200, body)

    jobs = make_kube(handler).list("job", labels={"inspector.sparecores.com/run": "x"})
    assert [j["metadata"]["name"] for j in jobs] == ["dbaas-up-x"]
    assert seen[0][0] == "/apis/batch/v1/namespaces/inspector/jobs"
