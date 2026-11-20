# The sc-run image: the anchor runs it, and it copies the binary to the node for the steps.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w" -o /sc-run ./cmd/sc-run

FROM scratch
COPY --from=build /sc-run /sc-run
ENTRYPOINT ["/sc-run"]
