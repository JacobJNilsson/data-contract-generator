# data-contract-generator

Go libraries for generating data contracts from ingestible files (CSV, JSON/NDJSON, Excel) and computing structural fingerprints. Used by the data-ingestion platform to understand a landed file's shape and identity before authoring pipeline code for it.

## Install

```bash
go get github.com/JacobJNilsson/data-contract-generator@latest
```

## Packages

| Package | Purpose |
|---|---|
| [`contract`](contract/) | Shared types: `DataContract`, `SchemaContract`, `FieldDefinition`, `FieldProfile` |
| [`profile`](profile/) | Shared profiling core: type classification, column profiling, value-shape signatures |
| [`csvcontract`](csvcontract/) | Analyze CSV files: encoding, delimiter, header detection, type inference, data profiling |
| [`excelcontract`](excelcontract/) | Analyze Excel (.xlsx) files: multi-sheet, table and header detection, type inference, data profiling |
| [`jsoncontract`](jsoncontract/) | Analyze JSON arrays and NDJSON: streaming, type inference, data profiling |
| [`fingerprint`](fingerprint/) | Structural fingerprints: canonical schema identity, hashing, and near-neighbour ranking for the pipeline cache |

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

Requires Go 1.25+ and `golangci-lint`. No Docker needed (the test suite is pure Go).

```bash
make setup    # configure git hooks
make check    # tidy, vet, lint, test, build
```

The pre-commit hook runs `make check` which enforces:

| Package | Coverage gate |
|---|---|
| profile | 100% |
| csvcontract | 100% |
| fingerprint | 100% |
| jsoncontract | 95% |
| excelcontract | 95% |

## Architecture

```
contract/        ← shared types (DataContract, SchemaContract, FieldProfile)
profile/         ← shared profiling core: type classification, column profiling, value-shape signatures
csvcontract/     ← CSV file analysis
excelcontract/   ← Excel (.xlsx) file analysis
jsoncontract/    ← JSON/NDJSON file analysis
fingerprint/     ← structural identity for the pipeline cache
```

`excelcontract` emits `contract.DataContract`, while `csvcontract` and `jsoncontract` emit their own `SourceContract` shapes, so `contract/` is the shared type set for the schema-style path, not a single source of truth across every package. `csvcontract` and `excelcontract` share the `profile/` core; `jsoncontract` carries its own profiling and is a known divergence. No circular dependencies.

The standalone source-analysis packages (REST/OpenAPI, PostgreSQL, Supabase) and the contract verification and transform-mapping packages were removed when the standalone analysis product they served was wound down; they live in git history if ever needed again.
