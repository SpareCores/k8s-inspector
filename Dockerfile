# The sc-run image: the anchor runs it, and it copies the binary to the node for the steps.
FROM --platform=$BUILDPLATFORM golang:1 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w" -o /sc-run ./cmd/sc-run

FROM scratch
COPY --from=build /sc-run /sc-run
ENTRYPOINT ["/sc-run"]
