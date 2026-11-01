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

Requires Go 1.25+ and `golangci-lint`.

The gate is two-tier:

- **Fast tier (default)** — pure Go, no Docker, no network. This is what `make check` and the pre-commit hook run, and what a contributor needs day to day.
- **Integration tier** — for destination code that must run against a live Postgres. Gated behind the `integration` build tag so it never touches the fast path, and run separately (in CI, and locally on demand). It needs a Docker daemon (the `internal/pgtest` harness starts a throwaway Postgres via testcontainers) or an existing database via `TEST_PG_CONN`; it skips rather than fails when neither is present.

```bash
make setup             # configure git hooks
make check             # fast tier: tidy, vet, lint, test, build (no Docker)
make integration-test  # integration tier: go test -tags=integration ./... (needs Docker or TEST_PG_CONN)
```

The pre-commit hook runs `make check` which enforces per-package coverage:

| Package | Coverage gate |
|---|---|
| profile | 100% |
| csvcontract | 100% |
| fingerprint | 100% |
| odcs | 100% |
| odcsemit | 100% |
| odcsdest | 100% |
| pgcheck | 100% |
| pgintrospect | 100% |
| declimport | 100% |
| jsoncontract | 95% |
| excelcontract | 95% |

`pgcontract` is gated at 100% in the INTEGRATION tier instead of the fast tier: its faithful generator (`Generate`) needs a live Postgres, so its full behaviour — the pure assembly/validation core and the live-DB reads alike — is exercised and measured under `-tags=integration` rather than faked to hit a fast-tier number.

CI runs the fast tier and the integration tier as separate jobs, so the pure-Go promise holds for the default path while destination code is still covered against a real Postgres. Both jobs enforce the per-package coverage gates above (the fast tier for the pure packages, the integration tier for `pgcontract`).

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

## The decimal mark

A CSV holds numbers as text, so a reader can only read a numeric column if it
knows the column's decimal mark. The generator states the mark only when the
file proves it, and states nothing it cannot prove.

**The grammar.** `profile.ParseDecimal(s, mark)` reads a number with a
declared mark, `,` or `.`. A number is an optional leading `-`, one or more
digits, and optionally the mark followed by one or more digits. Nothing else
is a number: no group separator, no exponent, no currency sign, no percent
sign, and no surrounding space. The parser never guesses.

**The evidence.** A value proves its mark when it holds exactly one
separator, `,` or `.`, with digits on both sides and a number of digits after
the separator other than three. `3,1416` proves `,`. `12,345` proves
nothing, because it reads as 12.345 or as 12345. A leading `-` does not
change the rule.

**The file's mark.** `csvcontract` and `excelcontract` collect the evidence
from every column the profiler types numeric. When every proving value agrees,
the contract states the mark: `decimal_char` in the native contract, and the
schema-level custom property `dcgDecimalChar` in the ODCS document, beside
`dcgDelimiter`. When no value proves a mark, or values prove both marks, the
contract states none.

**Ranges.** An unproven value is a value the profiler types numeric that
holds a `,` or a `.` and proves no mark. `12,345`, `1,234.56`, `.5` and `5.`
are unproven. A numeric column that holds an unproven value gets no
`min_value` and no `max_value`, even when other values prove the file's mark.
The column's type stays numeric. Every other column keeps its range.

**What does not change.** The mark is not part of the structural
fingerprint, so it changes no cache key. Type classification does not
change. `profile.ParseNumeric` keeps its behaviour for its existing callers.
`jsoncontract` reads JSON numbers, which have one spelling, so the rule does
not apply to it.

The standalone source-analysis packages (REST/OpenAPI, PostgreSQL, Supabase) and the contract verification and transform-mapping packages were removed when the standalone analysis product they served was wound down; they live in git history if ever needed again.
