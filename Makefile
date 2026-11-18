# These targets are intended to run inside operator-managed development/build images.
# No target starts a daemon, container or external service.
.PHONY: assets build test-go test-ai fmt
assets:
	go run ./cmd/assets
build:
	go build -mod=mod -o bin/tensorforge ./cmd/server
test-go:
	go test -mod=mod -race -count=1 ./...
test-ai:
	pytest -q -p no:cacheprovider
fmt:
	gofmt -w cmd internal
