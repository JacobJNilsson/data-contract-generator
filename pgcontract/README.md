# pgcontract

Analyzes PostgreSQL databases and produces contracts describing table structures, types, constraints, and data profiles. The AI agent uses these contracts to decide which table to ingest data into or extract data from.

## Usage

```go
// Analyze an entire database (all tables in a schema).
contract, err := pgcontract.AnalyzeDatabase(ctx, connString, &pgcontract.Options{
    Schema:          "public",
    IncludeComments: true,
})

// Analyze a single table.
table, err := pgcontract.AnalyzeTable(ctx, connString, "users", &pgcontract.Options{
    IncludeComments: true,
})
```

## What it produces

`AnalyzeDatabase` returns a `DatabaseContract` containing every table in the schema:

```json
{
  "contract_type": "destination",
  "database_id": "mydb",
  "tables": [
    {
      "table_name": "users",
      "schema": "public",
      "row_count": 1523,
      "fields": [
        {
          "name": "id",
          "data_type": "integer",
          "nullable": false,
          "constraints": [
            {"type": "primary_key"},
            {"type": "not_null"}
          ],
          "profile": {
            "null_count": 0,
            "null_percentage": 0,
            "distinct_count": 1523,
            "min_value": "1",
            "max_value": "1523",
            "top_values": [{"value": "1", "count": 1}],
            "sample_size": 1523
          }
        },
        {
          "name": "email",
          "data_type": "varchar",
          "nullable": false,
          "description": "User email address",
          "constraints": [
            {"type": "unique"},
            {"type": "not_null"}
          ]
        }
      ],
      "sample_data": [
        ["1", "alice@example.com"],
        ["2", "bob@example.com"]
      ],
      "validation_rules": {
        "required_fields": ["id", "email"],
        "unique_constraints": ["id", "email"]
      }
    }
  ],
  "metadata": {
    "source": "postgresql",
    "schema": "public",
    "table_count": 1,
    "connection": "user@localhost:5432/mydb"
  }
}
```

## Options

```go
type Options struct {
    Schema          string // PostgreSQL schema (default: "public")
    IncludeComments bool   // Include column comments as descriptions
    SampleSize      int    // Max rows to sample for profiling (default: 10000)
    BatchSize       int    // Rows per batch during profiling (default: 1000)
    MaxSampleRows   int    // Rows included in SampleData (default: 5)
    TopN            int    // Top frequent values per column (default: 5)
}
```

## Features

- Schema introspection via `information_schema`
- Constraint detection: PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL
- Type normalization (20+ PostgreSQL types mapped to standard names)
- Column comments from `pg_description`
- Data profiling: null counts, distinct counts, min/max, top values
- Row count (exact for small tables, estimated via `pg_stat` for large)
- Sample data preview
- Connection string sanitization (password stripped from metadata)

## Testing

Tests run against a real PostgreSQL instance via Docker:

```bash
make check    # starts Postgres, runs all tests, enforces coverage
make db-stop  # stops the container
```

## Supported types

| PostgreSQL | Contract type |
|---|---|
| `integer`, `int`, `int4` | `integer` |
| `bigint`, `int8` | `bigint` |
| `smallint`, `int2` | `smallint` |
| `text` | `text` |
| `varchar(n)`, `character varying` | `varchar` |
| `char(n)`, `character` | `char` |
| `boolean`, `bool` | `boolean` |
| `date` | `date` |
| `timestamp`, `timestamp without time zone` | `timestamp` |
| `timestamp with time zone`, `timestamptz` | `timestamptz` |
| `numeric(p,s)`, `decimal(p,s)` | `numeric` |
| `real` | `real` |
| `double precision` | `double` |
| `json` | `json` |
| `jsonb` | `jsonb` |
| `uuid` | `uuid` |
| `bytea` | `bytea` |
