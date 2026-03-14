MIN_COVERAGE := 100

.PHONY: build test lint vet tidy check clean

build:
	go build ./...

test:
	go test -race -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//' | \
		awk -v min=$(MIN_COVERAGE) '{if ($$1+0 < min) {printf "Coverage %.1f%% < %d%% minimum\n", $$1, min; exit 1} else {printf "Coverage: %.1f%%\n", $$1}}'

lint:
	golangci-lint run ./...

vet:
	go vet ./...

tidy:
	go mod tidy

check: tidy vet lint test build

setup:
	git config core.hooksPath go-rewrite/.githooks

clean:
	rm -rf coverage.out
