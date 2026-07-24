# data-contract-generator

Go libraries for generating data contracts from CSV, JSON/NDJSON, Excel, REST APIs, PostgreSQL databases, and Supabase projects.

## Session Start

```bash
ml prime        # Load project expertise into context
sd prime        # Load issue tracking context
sd ready        # Find unblocked work
```

## Project Rules

- **Go conventions**: Prefer stdlib, remove dead code, no type aliases, no circular dependencies. External deps: golang.org/x/text, excelize/v2.
- **Testing**: Per-package coverage gates (profile/csv/fingerprint/odcs/odcsemit/odcsdest/pgcheck/declimport 100%, json/excel 95%). Compare complete objects in tests. Race detector on all runs.
- **Pre-commit**: Runs `make check` (tidy, vet, lint, per-package coverage, build). All must pass.
- **Commits**: Conventional Commits format (`<type>[optional scope]: <description>`). Imperative mood, start capitalized, no trailing period.
- **All changes go through PRs.** Never push directly to main.
- **All code, comments, test data, and documentation must be in English.**

## Architecture

```
contract/        <- shared types for the schema-style path
profile/         <- shared type classification, column profiling, value-shape signatures
csvcontract/     <- CSV file analysis
excelcontract/   <- Excel (.xlsx) file analysis
jsoncontract/    <- JSON/NDJSON file analysis
fingerprint/     <- structural identity for the pipeline cache
```

The source-analysis (REST/OpenAPI, PostgreSQL, Supabase), verification, and transform-mapping packages were removed when the standalone analysis product was wound down.

## Cross-Repo Testing

When a feature spans multiple repos (library → API → frontend), unit tests in each repo are not sufficient. Write a Playwright e2e test in `data-ingestion-web/e2e/` that tests the real user workflow against a local API server. See `data-ingestion-web/AGENTS.md` for details.

## Before You Finish

```bash
ml learn        # See what changed, get suggestions for what to record
ml record <domain> --type <type> --description "..."
sd close <id>   # Close completed issues
ml sync && sd sync
```

Domains: `go`, `testing`, `architecture`
