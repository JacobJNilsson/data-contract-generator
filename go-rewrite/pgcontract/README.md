# pgcontract - PostgreSQL Destination Contract Generator

Analyzes PostgreSQL database schemas and generates destination contracts describing table structures, types, and constraints.

## Usage

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    
    "github.com/jacobjnilsson/contract-gen/pgcontract"
)

func main() {
    ctx := context.Background()
    
    contract, err := pgcontract.Analyze(
        ctx,
        "postgres://user:password@localhost:5432/mydb",
        "users",
        &pgcontract.Options{
            Schema:          "public",
            IncludeComments: true,
        },
    )
    if err != nil {
        log.Fatal(err)
    }
    
    b, _ := json.MarshalIndent(contract, "", "  ")
    fmt.Println(string(b))
}
```

## Features

- **Schema Introspection**: Analyzes table structure from `information_schema`
- **Type Normalization**: Maps PostgreSQL types to standard contract types
- **Constraint Detection**: Identifies PRIMARY KEY, FOREIGN KEY, UNIQUE, and NOT NULL constraints
- **Comment Support**: Optionally includes PostgreSQL column comments as field descriptions
- **Validation Rules**: Auto-generates required fields and unique constraints

## Options

```go
type Options struct {
    // Schema is the PostgreSQL schema to analyze (default: "public")
    Schema string

    // IncludeComments includes PostgreSQL column comments as descriptions
    IncludeComments bool
}
```

## Destination Contract Structure

```json
{
  "contract_type": "destination",
  "destination_id": "public.users",
  "schema": {
    "fields": [
      {
        "name": "id",
        "data_type": "integer",
        "nullable": false,
        "constraints": [
          {"type": "primary_key"},
          {"type": "not_null"}
        ]
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
    ]
  },
  "validation_rules": {
    "required_fields": ["id", "email", "name"],
    "unique_constraints": ["id", "email"]
  },
  "metadata": {
    "source": "postgresql",
    "schema": "public",
    "table": "users",
    "connection": "user@localhost:5432/mydb"
  }
}
```

## Testing

### Unit Tests

```bash
go test ./pgcontract/...
```

### Integration Tests

Integration tests require a running PostgreSQL instance:

```bash
# Start PostgreSQL (Docker example)
docker run --name pg-test -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:17-alpine

# Run integration tests
export TEST_PG_CONN="postgres://postgres:postgres@localhost:5432/postgres"
go test -tags=integration ./pgcontract/... -v

# Cleanup
docker rm -f pg-test
```

## Supported PostgreSQL Types

| PostgreSQL Type | Normalized Type |
|----------------|-----------------|
| `integer`, `int`, `int4` | `integer` |
| `bigint`, `int8` | `bigint` |
| `smallint`, `int2` | `smallint` |
| `text` | `text` |
| `character varying`, `varchar(n)` | `varchar` |
| `character`, `char(n)` | `char` |
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

## Error Handling

- Returns error if table does not exist
- Returns error if connection fails
- Returns error if schema introspection fails
- Sanitizes connection string in metadata (removes password)

## Thread Safety

The `Analyze` function is safe for concurrent use across goroutines.
