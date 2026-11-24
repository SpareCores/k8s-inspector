# Static sc-run binaries for the benchmark nodes.
.PHONY: test build clean dbaas-test dbaas-image
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

dbaas-test:
	cd dbaas && uv run ruff check . && uv run pytest

dbaas-image:
	docker build -t ghcr.io/sparecores/k8s-inspector-dbaas:dev dbaas
