# contract-gen (Go)

Go libraries for generating data contracts from CSV files, PostgreSQL databases, and Supabase projects. Used by the AI ingestion agent to understand source and destination schemas before generating code.

## Packages

| Package | Purpose |
|---|---|
| `csvcontract` | Analyze CSV files: encoding, delimiter, header detection, type inference, data profiling |
| `pgcontract` | Analyze PostgreSQL databases: schema introspection, constraints, data profiling |
| `supacontract` | Analyze Supabase projects via PostgREST OpenAPI (no direct DB connection needed) |

## Quick start

```bash
make setup    # configure git hooks
make check    # lint, test (starts Postgres via Docker), build
make db-stop  # stop the test database
```

## Development

Requires Go 1.25+, Docker (for pgcontract integration tests), and `golangci-lint`.

The pre-commit hook runs `make check` which enforces:
- `go vet` and `golangci-lint` with zero issues
- csvcontract: 100% test coverage
- supacontract: 95% test coverage
- pgcontract: 85% test coverage (tested against real Postgres)
- Successful build

### Testing

```bash
make check       # full quality gate (starts Postgres automatically)
make test        # tests only (starts Postgres automatically)
make lint        # linter only
make db-start    # start Postgres without running tests
make db-stop     # stop Postgres
```

## Package details

- **[csvcontract](csvcontract/)** — Streaming two-phase analysis. First pass sniffs encoding and delimiter, second pass streams rows through per-column profilers. Bounded memory via capped frequency maps and streaming min/max.

- **[pgcontract](pgcontract/)** — Connects to PostgreSQL via pgx, introspects `information_schema` for schema metadata, then samples up to 10,000 rows (in batches of 1,000) for data profiling. Uses `pgx.Identifier` for safe SQL identifier quoting.

- **[supacontract](supacontract/)** — Pure HTTP. Fetches the PostgREST OpenAPI spec from `{project_url}/rest/v1/` and extracts table definitions, column types, and descriptions. No database driver needed.
