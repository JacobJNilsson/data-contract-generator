.PHONY: build test lint vet tidy check clean setup import-tools import-test integration-test

# Pinned Python toolchain for the declared-schema importer (declimport).
# Kept out of `make check` on purpose: the declimport unit tests run against
# a fake command runner and need no Python. Create the venv and install the
# pinned Data Contract CLI with `make import-tools`, then drive the real tool
# end to end with `make import-test`.
IMPORT_VENV := tools/import/.venv

build:
	go build ./...

test:
	go test -race -coverprofile=coverage.out ./profile/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL profile %.1f%% < 100%%\n", $$1; exit 1} else {printf "profile: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./csvcontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL csvcontract %.1f%% < 100%%\n", $$1; exit 1} else {printf "csvcontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./fingerprint/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL fingerprint %.1f%% < 100%%\n", $$1; exit 1} else {printf "fingerprint: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./odcs/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL odcs %.1f%% < 100%%\n", $$1; exit 1} else {printf "odcs: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./odcsemit/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL odcsemit %.1f%% < 100%%\n", $$1; exit 1} else {printf "odcsemit: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./odcsdest/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL odcsdest %.1f%% < 100%%\n", $$1; exit 1} else {printf "odcsdest: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./pgcheck/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL pgcheck %.1f%% < 100%%\n", $$1; exit 1} else {printf "pgcheck: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./pgintrospect/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL pgintrospect %.1f%% < 100%%\n", $$1; exit 1} else {printf "pgintrospect: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./jsoncontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL jsoncontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "jsoncontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./excelcontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL excelcontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "excelcontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./declimport/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL declimport %.1f%% < 100%%\n", $$1; exit 1} else {printf "declimport: %.1f%%\n", $$1}}'

lint:
	golangci-lint run ./...

vet:
	go vet ./...

tidy:
	go mod tidy

check: tidy vet lint test build

# Two-tier gate. `check` above is the fast, pure-Go default: no Docker, no
# network. `integration-test` is the second tier for destination code that
# needs a live Postgres. It is deliberately kept out of `check` so the default
# developer path stays Docker-free. The internal/pgtest harness starts a
# throwaway Postgres via testcontainers (needs a Docker daemon), or connects to
# an existing database when TEST_PG_CONN is set. Tests skip — not fail — when
# neither is available. The declimport CLI integration tests share the same tag
# and skip here unless their pinned binary is on PATH (use `make import-test`
# for those specifically, which also puts the venv on PATH).
integration-test:
	go test -tags=integration -race ./...

setup:
	git config core.hooksPath .githooks

# Create the importer's venv and install the pinned Data Contract CLI. This
# is a network install and is deliberately not part of `make check`.
import-tools:
	python3 -m venv $(IMPORT_VENV)
	$(IMPORT_VENV)/bin/pip install --upgrade pip
	$(IMPORT_VENV)/bin/pip install -r tools/import/requirements.txt
	@echo "Installed:" && $(IMPORT_VENV)/bin/datacontract --version

# Run the build-tagged integration tests against the real datacontract CLI,
# putting the venv's bin on PATH so the importer finds the pinned binary. The
# test skips (does not fail) if the binary is absent.
import-test:
	PATH="$(CURDIR)/$(IMPORT_VENV)/bin:$$PATH" go test -tags integration -race ./declimport/...

clean:
	rm -rf coverage.out
