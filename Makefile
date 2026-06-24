.PHONY: build test lint vet tidy check clean setup

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
	go test -race -coverprofile=coverage.out ./jsoncontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL jsoncontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "jsoncontract: %.1f%%\n", $$1}}'
	go test -race -coverprofile=coverage.out ./excelcontract/...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk '{if ($$1+0 < 95) {printf "FAIL excelcontract %.1f%% < 95%%\n", $$1; exit 1} else {printf "excelcontract: %.1f%%\n", $$1}}'

lint:
	golangci-lint run ./...

vet:
	go vet ./...

tidy:
	go mod tidy

check: tidy vet lint test build

setup:
	git config core.hooksPath .githooks

clean:
	rm -rf coverage.out
