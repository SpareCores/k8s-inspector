# Controller image: ghcr.io/sparecores/k8s-inspector. Build context is this directory.
# Base: Debian trixie like the legacy sc-inspector image (python:3.12 there), so `lstopo` is the same hwloc (2.12.0).
FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928 AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.7@sha256:95f2aa1fe59274951cfe9b0cbc7972e879ff1004bc8945d130a32eb0dbd85945 /uv /bin/uv
# fastnumbers has no wheel for the newest Python yet: it compiles.
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY k8s_inspector k8s_inspector
RUN uv sync --locked --no-dev --no-editable

FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
# git and ssh push the results to sc-inspector-data; lstopo renders the lstopo.svg of the website.
# hwloc-nox is pinned to the Debian trixie build of hwloc 2.12.0: a different lstopo changes the rendered SVGs.
# When the base image moves to a release without this version, the build fails: bump the pin deliberately.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssh-client hwloc-nox=2.12.0-4 \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --uid 10001 --user-group --no-create-home --home-dir /work --shell /usr/sbin/nologin inspector
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["k8s-inspector"]
