# Static sc-run binaries for the benchmark nodes.
.PHONY: test build clean
LDFLAGS := -s -w

test:
	go vet ./...
	go test -race ./...

build:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/sc-run-linux-amd64 ./cmd/sc-run
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS)" -o dist/sc-run-linux-arm64 ./cmd/sc-run

clean:
	rm -rf dist
