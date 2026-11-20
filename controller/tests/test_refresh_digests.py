"""The offline parts of scripts/refresh_digests.py (registry access is not tested)."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts" / "refresh_digests.py"
spec = importlib.util.spec_from_file_location("refresh_digests", SCRIPT)
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)

CATALOGUE = """\
# comment kept
tasks:
  - name: a
    image: ghcr.io/x/a:main
    digest: sha256:old
    command: run
  - name: v
    images:
      - ghcr.io/x/g:main
      - ghcr.io/x/c:main
    digests:
      ghcr.io/x/g:main: sha256:old
    command: null
  - name: b
    image: nvidia/cuda:11.4
    command: run
"""
DIGESTS = {
    "ghcr.io/x/a:main": "sha256:a",
    "ghcr.io/x/g:main": "sha256:g",
    "ghcr.io/x/c:main": "sha256:c",
    "nvidia/cuda:11.4": "sha256:n",
}


def test_split_reference():
    assert refresh.split_reference("ghcr.io/sparecores/dmidecode:main") == ("ghcr.io", "sparecores/dmidecode", "main")
    assert refresh.split_reference("nvidia/cuda:11.4.3-base") == ("registry-1.docker.io", "nvidia/cuda", "11.4.3-base")
    assert refresh.split_reference("python:3.12") == ("registry-1.docker.io", "library/python", "3.12")
    assert refresh.split_reference("alpine") == ("registry-1.docker.io", "library/alpine", "latest")


def test_catalogue_images_are_distinct_and_in_order():
    assert refresh.catalogue_images(CATALOGUE) == list(DIGESTS)


def test_rewrite_sets_digests_keeps_the_rest_and_is_idempotent():
    out = refresh.rewrite(CATALOGUE, DIGESTS)
    assert out.startswith("# comment kept\n")
    assert "    images:\n      - ghcr.io/x/g:main\n" in out
    assert "    image: ghcr.io/x/a:main\n    digest: sha256:a\n    command: run\n" in out
    assert (
        "      - ghcr.io/x/c:main\n    digests:\n      ghcr.io/x/g:main: sha256:g\n      ghcr.io/x/c:main: sha256:c\n"
        in out
    )
    assert "    image: nvidia/cuda:11.4\n    digest: sha256:n\n" in out
    assert "sha256:old" not in out
    assert refresh.rewrite(out, DIGESTS) == out
