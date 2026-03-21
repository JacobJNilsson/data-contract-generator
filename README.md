# data-contract-generator

Go libraries for generating data contracts from CSV files, JSON/NDJSON, REST APIs, PostgreSQL databases, and Supabase projects. Used by the AI ingestion agent to understand source and destination schemas before generating code.

## Install

```bash
go get github.com/JacobJNilsson/data-contract-generator@latest
```

## Packages

| Package | Purpose |
|---|---|
| [`contract`](contract/) | Shared types: `DataContract`, `SchemaContract`, `FieldDefinition` |
| [`csvcontract`](csvcontract/) | Analyze CSV files: encoding, delimiter, header detection, type inference, data profiling |
| [`jsoncontract`](jsoncontract/) | Analyze JSON arrays and NDJSON: streaming, type inference, data profiling |
| [`apicontract`](apicontract/) | Analyze REST APIs via OpenAPI 3.x / Swagger 2.0 specs |
| [`pgcontract`](pgcontract/) | Analyze PostgreSQL databases: schema introspection, constraints, data profiling |
| [`supacontract`](supacontract/) | Analyze Supabase projects via PostgREST OpenAPI (no direct DB connection needed) |
| [`verify`](verify/) | Validate contracts for structural correctness and semantic coherence |
| [`transform`](transform/) | Generate transformation contracts with field mapping suggestions |

## Quick start

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    "github.com/JacobJNilsson/data-contract-generator/csvcontract"
)

func main() {
    contract, err := csvcontract.AnalyzeFile(context.Background(), "data.csv", nil)
    if err != nil {
        log.Fatal(err)
    }
    b, _ := json.MarshalIndent(contract, "", "  ")
    fmt.Println(string(b))
}
```

## Development

Requires Go 1.25+, Docker (for pgcontract integration tests), and `golangci-lint`.

```bash
make setup    # configure git hooks
make check    # lint, test (starts Postgres via Docker), build
make db-stop  # stop the test database
```

The pre-commit hook runs `make check` which enforces:

| Package | Coverage gate |
|---|---|
| csvcontract | 100% |
| jsoncontract | 95% |
| apicontract | 95% |
| verify | 100% |
| transform | 100% |
| supacontract | 95% |
| pgcontract | 85% (tested against real Postgres) |

## Architecture

```
contract/        ← shared types (single source of truth)
csvcontract/     ← CSV source analysis
jsoncontract/    ← JSON/NDJSON source analysis
apicontract/     ← REST API analysis (OpenAPI/Swagger)
pgcontract/      ← PostgreSQL analysis
supacontract/    ← Supabase analysis
verify/          ← contract validation
transform/       ← field mapping suggestions
```

No circular dependencies. No type aliases. Each analyzer imports `contract/` for shared types and stdlib for everything else (except `pgcontract` which uses `pgx`).
