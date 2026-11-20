"""Pin every catalogue image to its current multi-arch index digest.

Usage: python scripts/refresh_digests.py [path/to/tasks.yaml]

Reads the image references of `k8s_inspector/catalogue/tasks.yaml`, resolves each tag to the digest of its
manifest list (the multi-arch index) through the OCI distribution API of its public registry (anonymous bearer
token, no credentials) and rewrites the file in place: a task's `digest: sha256:...` line goes right after its
`image:` line, the vLLM ladder's `digests:` map right after its `images:` list. Everything else is left as it is.
Standard library only, so CI can run it without installing the package.
"""

import hashlib
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CATALOGUE = Path(__file__).parents[1] / "k8s_inspector" / "catalogue" / "tasks.yaml"
INDEX_TYPES = ("application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json")
ACCEPT = ", ".join(INDEX_TYPES + ("application/vnd.oci.image.manifest.v1+json",))
REGISTRY_HOSTS = {"docker.io": "registry-1.docker.io"}

IMAGE_LINE = re.compile(r"^    image: (\S+)$")
DIGEST_LINE = re.compile(r"^    digest: ")
LADDER_ITEM = re.compile(r"^      - (\S+)$")


def split_reference(ref: str) -> tuple[str, str, str]:
    """`ghcr.io/sparecores/x:main` -> (registry host, repository, tag). Docker Hub is the default registry."""
    first, _, rest = ref.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        host, name = first, rest
    else:
        host, name = "docker.io", ref
    name, _, tag = name.rpartition(":") if ":" in name else (name, "", "latest")
    if host == "docker.io" and "/" not in name:
        name = f"library/{name}"
    return REGISTRY_HOSTS.get(host, host), name, tag


def anonymous_token(challenge: str, repository: str) -> str:
    """Bearer token for a `WWW-Authenticate: Bearer realm=...,service=...` challenge."""
    params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
    query = {"service": params.get("service", ""), "scope": f"repository:{repository}:pull"}
    with urllib.request.urlopen(f"{params['realm']}?{urllib.parse.urlencode(query)}", timeout=30) as response:
        body = json.load(response)
    return body.get("token") or body["access_token"]


def fetch_manifest(host: str, repository: str, tag: str) -> urllib.request.addinfourl:
    url = f"https://{host}/v2/{repository}/manifests/{tag}"
    request = urllib.request.Request(url, headers={"Accept": ACCEPT})
    try:
        return urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as error:
        challenge = error.headers.get("WWW-Authenticate", "")
        if error.code != 401 or not challenge.startswith("Bearer "):
            raise
    request.add_header("Authorization", f"Bearer {anonymous_token(challenge, repository)}")
    return urllib.request.urlopen(request, timeout=30)


def resolve_digest(ref: str) -> str:
    """The `sha256:...` digest of the manifest list `ref` points to."""
    host, repository, tag = split_reference(ref)
    with fetch_manifest(host, repository, tag) as response:
        media_type = response.headers.get_content_type()
        body = response.read()
    if media_type not in INDEX_TYPES:
        raise ValueError(f"{ref}: not a multi-arch index (media type {media_type})")
    return "sha256:" + hashlib.sha256(body).hexdigest()


def rewrite(text: str, digests: dict[str, str]) -> str:
    """The catalogue text with every digest line set from `digests` (image reference -> digest)."""
    lines = text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if match := IMAGE_LINE.match(line):
            out.append(line)
            out.append(f"    digest: {digests[match[1]]}")
            if i < len(lines) and DIGEST_LINE.match(lines[i]):
                i += 1
        elif line == "    images:":
            out.append(line)
            refs = []
            while i < len(lines) and (item := LADDER_ITEM.match(lines[i])):
                out.append(lines[i])
                refs.append(item[1])
                i += 1
            out.append("    digests:")
            out += [f"      {ref}: {digests[ref]}" for ref in dict.fromkeys(refs)]  # one key per image
            if i < len(lines) and lines[i] == "    digests:":
                i += 1
                while i < len(lines) and lines[i].startswith("      "):
                    i += 1
        else:
            out.append(line)
    return "\n".join(out) + "\n"


def catalogue_images(text: str) -> list[str]:
    """Every distinct image reference of the catalogue, in file order."""
    refs = [m[1] for line in text.splitlines() if (m := IMAGE_LINE.match(line) or LADDER_ITEM.match(line))]
    return list(dict.fromkeys(refs))


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else CATALOGUE
    text = path.read_text()
    digests, failed = {}, []
    for ref in catalogue_images(text):
        try:
            digests[ref] = resolve_digest(ref)
        except (OSError, ValueError, KeyError) as error:
            failed.append(ref)
            print(f"FAILED {ref}: {error}", file=sys.stderr)
    if failed:
        return 1
    path.write_text(rewrite(text, digests))
    print(f"pinned {len(digests)} images in {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
