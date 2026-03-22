# data-contract-generator

Go libraries for generating data contracts from CSV, JSON/NDJSON, REST APIs, PostgreSQL databases, and Supabase projects.

## Session Start

```bash
ml prime        # Load project expertise into context
sd prime        # Load issue tracking context
sd ready        # Find unblocked work
```

## Project Rules

- **Go conventions**: Prefer stdlib, remove dead code, no type aliases, no circular dependencies. Only external deps: pgx/v5 and golang.org/x/text.
- **Testing**: Per-package coverage gates (csv/verify/transform 100%, json/api/supa 95%, pg 85%). Compare complete objects in tests. Race detector on all runs.
- **Pre-commit**: Runs `make check` (tidy, vet, lint, per-package coverage, build). All must pass.
- **Commits**: Conventional Commits format (`<type>[optional scope]: <description>`). Imperative mood, start capitalized, no trailing period.
- **All changes go through PRs.** Never push directly to main.
- **All code, comments, test data, and documentation must be in English.**

## Architecture

```
contract/        <- shared types (single source of truth)
csvcontract/     <- CSV source analysis
jsoncontract/    <- JSON/NDJSON source analysis
apicontract/     <- REST API analysis (OpenAPI/Swagger)
pgcontract/      <- PostgreSQL analysis (real Postgres in tests)
supacontract/    <- Supabase analysis (via PostgREST OpenAPI)
verify/          <- contract validation
transform/       <- field mapping suggestions
```

## Before You Finish

```bash
ml learn        # See what changed, get suggestions for what to record
ml record <domain> --type <type> --description "..."
sd close <id>   # Close completed issues
ml sync && sd sync
```

Domains: `go`, `testing`, `architecture`
