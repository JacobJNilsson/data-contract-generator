//go:build integration

// Package declimport's integration test drives the real Data Contract CLI
// end to end, proving the importer's argv contract and ODCS parsing hold
// against the genuine tool rather than the fake Runner. It is gated behind
// the `integration` build tag so it never runs in the default `make check`
// (which is pure Go and needs no Python toolchain); run it with
//
//	make import-test
//
// which puts dcg's pinned venv on PATH first. If the datacontract binary is
// not found, the test skips with an explanatory message rather than failing,
// so a contributor without the toolchain is not blocked.
package declimport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
)

const sampleDDL = `CREATE TABLE customers (
    id integer PRIMARY KEY,
    email text NOT NULL,
    created_at timestamp,
    active boolean
);
`

// sampleAvro is a tiny Avro record schema with a couple of typed fields. The
// importer's [avro] extra (see tools/import/requirements.txt) must be present
// for the import step to load; the test skips when the binary is absent and
// fails loudly if the extra is missing, so a half-installed toolchain is not
// mistaken for a passing run.
const sampleAvro = `{
  "type": "record",
  "name": "Customer",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "email", "type": "string"},
    {"name": "active", "type": "boolean"}
  ]
}
`

func TestIntegration_FromSQLDDL_Postgres(t *testing.T) {
	if _, err := exec.LookPath(DefaultBinary); err != nil {
		t.Skipf("real %q not on PATH; install with `make import-tools` and run `make import-test`", DefaultBinary)
	}

	ddl := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(ddl, []byte(sampleDDL), 0o600); err != nil {
		t.Fatalf("write sample DDL: %v", err)
	}

	got, err := FromSQLDDL(context.Background(), ddl, "postgres")
	if err != nil {
		t.Fatalf("FromSQLDDL against real CLI: %v", err)
	}

	// A faithful ODCS contract: v3.1.0 header, one table, the typed columns.
	if got.APIVersion != "v3.1.0" {
		t.Fatalf("apiVersion = %q, want v3.1.0", got.APIVersion)
	}
	if got.Kind != "DataContract" {
		t.Fatalf("kind = %q, want DataContract", got.Kind)
	}
	if len(got.Schema) != 1 {
		t.Fatalf("schema objects = %d, want 1", len(got.Schema))
	}
	tbl := got.Schema[0]
	if tbl.Name != "customers" {
		t.Fatalf("table name = %q, want customers", tbl.Name)
	}

	byName := map[string]odcs.Property{}
	for _, p := range tbl.Properties {
		byName[p.Name] = p
	}
	if len(byName) != 4 {
		t.Fatalf("columns = %d, want 4 (%v)", len(byName), tbl.Properties)
	}

	// The integer primary key: logical integer, marked primary key.
	id := byName["id"]
	if id.LogicalType != "integer" {
		t.Fatalf("id logicalType = %q, want integer", id.LogicalType)
	}
	if id.PrimaryKey == nil || !*id.PrimaryKey {
		t.Fatalf("id primaryKey = %v, want true", id.PrimaryKey)
	}

	// The NOT NULL text column: logical string, required true.
	email := byName["email"]
	if email.LogicalType != "string" {
		t.Fatalf("email logicalType = %q, want string", email.LogicalType)
	}
	if email.Required == nil || !*email.Required {
		t.Fatalf("email required = %v, want true (NOT NULL)", email.Required)
	}

	// The timestamp and boolean columns keep their logical types.
	if got := byName["created_at"].LogicalType; got != "timestamp" {
		t.Fatalf("created_at logicalType = %q, want timestamp", got)
	}
	if got := byName["active"].LogicalType; got != "boolean" {
		t.Fatalf("active logicalType = %q, want boolean", got)
	}
}

func TestIntegration_FromAvro(t *testing.T) {
	if _, err := exec.LookPath(DefaultBinary); err != nil {
		t.Skipf("real %q not on PATH; install with `make import-tools` and run `make import-test`", DefaultBinary)
	}

	schema := filepath.Join(t.TempDir(), "schema.avsc")
	if err := os.WriteFile(schema, []byte(sampleAvro), 0o600); err != nil {
		t.Fatalf("write sample Avro: %v", err)
	}

	got, err := FromAvro(context.Background(), schema)
	if err != nil {
		t.Fatalf("FromAvro against real CLI: %v", err)
	}

	// A faithful ODCS contract: v3.1.0 header, the record as one schema
	// object, the typed fields carried through.
	if got.APIVersion != "v3.1.0" {
		t.Fatalf("apiVersion = %q, want v3.1.0", got.APIVersion)
	}
	if got.Kind != "DataContract" {
		t.Fatalf("kind = %q, want DataContract", got.Kind)
	}
	if len(got.Schema) != 1 {
		t.Fatalf("schema objects = %d, want 1", len(got.Schema))
	}
	rec := got.Schema[0]
	if rec.Name != "Customer" {
		t.Fatalf("record name = %q, want Customer", rec.Name)
	}

	byName := map[string]odcs.Property{}
	for _, p := range rec.Properties {
		byName[p.Name] = p
	}
	if len(byName) != 3 {
		t.Fatalf("fields = %d, want 3 (%v)", len(byName), rec.Properties)
	}

	// The Avro long maps to a logical integer; string and boolean keep their
	// logical types. A required (non-union) field comes back required.
	id := byName["id"]
	if id.LogicalType != "integer" {
		t.Fatalf("id logicalType = %q, want integer", id.LogicalType)
	}
	if id.Required == nil || !*id.Required {
		t.Fatalf("id required = %v, want true (non-union Avro field)", id.Required)
	}
	if got := byName["email"].LogicalType; got != "string" {
		t.Fatalf("email logicalType = %q, want string", got)
	}
	if got := byName["active"].LogicalType; got != "boolean" {
		t.Fatalf("active logicalType = %q, want boolean", got)
	}
}
