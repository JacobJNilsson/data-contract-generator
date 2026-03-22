PG_CONTAINER := contract-gen-pg-test
PG_PORT      := 5439
PG_CONN      := postgres://postgres:postgres@localhost:$(PG_PORT)/postgres

.PHONY: build test lint vet tidy check clean setup db-start db-stop

build:
	go build ./...

test: db-start
	go test -race -coverprofile=coverage.out ./profile/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL profile %.1f%% < 100%%\n", $$1; exit 1} else {printf "profile: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./csvcontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL csvcontract %.1f%% < 100%%\n", $$1; exit 1} else {printf "csvcontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./jsoncontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL jsoncontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "jsoncontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./apicontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL apicontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "apicontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./verify/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL verify %.1f%% < 100%%\n", $$1; exit 1} else {printf "verify: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./transform/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 100) {printf "FAIL transform %.1f%% < 100%%\n", $$1; exit 1} else {printf "transform: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./supacontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL supacontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "supacontract: %.1f%%\n", $$1}}'
	TEST_PG_CONN=$(PG_CONN) go test -race -coverprofile=coverage.out ./pgcontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 85) {printf "FAIL pgcontract %.1f%% < 85%%\n", $$1; exit 1} else {printf "pgcontract: %.1f%%\n", $$1}}'

lint:
	golangci-lint run ./...

vet:
	go vet ./...

tidy:
	go mod tidy

check: tidy vet lint test build

# Start Postgres if not already running. Reuse across test runs.
db-start:
	@docker inspect $(PG_CONTAINER) >/dev/null 2>&1 && \
		docker start $(PG_CONTAINER) >/dev/null 2>&1 || \
		docker run --name $(PG_CONTAINER) \
			-e POSTGRES_PASSWORD=postgres \
			-p $(PG_PORT):5432 \
			-d postgres:17-alpine >/dev/null 2>&1
	@until docker exec $(PG_CONTAINER) pg_isready -U postgres >/dev/null 2>&1; do sleep 0.2; done

db-stop:
	@docker stop $(PG_CONTAINER) >/dev/null 2>&1 || true

setup:
	git config core.hooksPath .githooks

clean: db-stop
	rm -rf coverage.out
