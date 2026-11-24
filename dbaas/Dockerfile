# Provisioner image: ghcr.io/sparecores/k8s-inspector-dbaas. Build context is this directory.
# Python, the Pulumi CLI, the Pulumi SDK and the provider packages of the `vendors` dependency group, with their
# Pulumi plugins already installed, so a Job needs no download.
ARG TARGETARCH

# The CLI release tarballs, verified by checksum (https://github.com/pulumi/pulumi/releases, the checksums file).
FROM scratch AS pulumi-amd64
ADD --checksum=sha256:1e38f0fb69a14a326cf3acfaf975770e8de5d0771042f26292f0013d237b8393 \
    https://github.com/pulumi/pulumi/releases/download/v3.277.0/pulumi-v3.277.0-linux-x64.tar.gz /pulumi.tar.gz
FROM scratch AS pulumi-arm64
ADD --checksum=sha256:b9c2708bd13ffdcba79f555a0532893a9b5a44efca8e225d171b8f7b605b16ce \
    https://github.com/pulumi/pulumi/releases/download/v3.277.0/pulumi-v3.277.0-linux-arm64.tar.gz /pulumi.tar.gz
FROM pulumi-${TARGETARCH} AS pulumi

FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928 AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.7@sha256:95f2aa1fe59274951cfe9b0cbc7972e879ff1004bc8945d130a32eb0dbd85945 /uv /bin/uv
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 UV_PYTHON_DOWNLOADS=never PULUMI_HOME=/opt/pulumi PULUMI_SKIP_UPDATE_CHECK=true
RUN --mount=from=pulumi,source=/pulumi.tar.gz,target=/pulumi.tar.gz \
    mkdir -p /opt/pulumi-cli && tar -xzf /pulumi.tar.gz -C /opt/pulumi-cli --strip-components=1
# A C compiler for provider SDKs that have no wheel for this Python yet (oci: crc32c); the final stage does not get it.
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --group vendors --no-install-project
COPY k8s_inspector_dbaas k8s_inspector_dbaas
RUN uv sync --locked --no-dev --group vendors --no-editable
COPY install_plugins.py ./
RUN mkdir -p /opt/pulumi && PATH=/opt/pulumi-cli:/app/.venv/bin:$PATH /app/.venv/bin/python install_plugins.py

FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
RUN useradd --uid 10001 --user-group --no-create-home --home-dir /work --shell /usr/sbin/nologin inspector \
    && mkdir -p /work && chown 10001:10001 /work
COPY --from=build /opt/pulumi-cli /opt/pulumi-cli
COPY --from=build --chown=10001:10001 /opt/pulumi /opt/pulumi
COPY --from=build /app/.venv /app/.venv
ENV PATH=/opt/pulumi-cli:/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 PULUMI_HOME=/opt/pulumi PULUMI_SKIP_UPDATE_CHECK=true \
    PULUMI_AUTOMATION_API_SKIP_VERSION_CHECK=true HOME=/work
USER 10001:10001
WORKDIR /work
ENTRYPOINT ["python", "-m", "k8s_inspector_dbaas"]
