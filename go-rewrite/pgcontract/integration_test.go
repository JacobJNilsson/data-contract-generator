// go:build integration

package pgcontract

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAnalyze_Integration requires a running PostgreSQL instance.
// Set TEST_PG_CONN environment variable to the connection string.
//
// Example:
//
//	export TEST_PG_CONN="postgres://postgres:postgres@localhost:5432/testdb"
//	go test -tags=integration ./pgcontract/...
func TestAnalyze_Integration(t *testing.T) {
	connString := os.Getenv("TEST_PG_CONN")
	if connString == "" {
		t.Skip("Skipping integration test: TEST_PG_CONN not set")
	}

	ctx := context.Background()

	// Create a test table
	setupSQL := `
		DROP TABLE IF EXISTS test_users CASCADE;
		CREATE TABLE test_users (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		);
		COMMENT ON COLUMN test_users.email IS 'User email address';
	`

	// Run setup (requires pgx import in test)
	pool, err := getTestPool(ctx, connString)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, setupSQL); err != nil {
		t.Fatalf("Failed to setup test table: %v", err)
	}

	// Test analysis
	contract, err := Analyze(ctx, connString, "test_users", &Options{
		Schema:          "public",
		IncludeComments: true,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}

	// Verify contract structure
	if contract.ContractType != "destination" {
		t.Errorf("ContractType = %v, want destination", contract.ContractType)
	}

	if contract.DestinationID != "public.test_users" {
		t.Errorf("DestinationID = %v, want public.test_users", contract.DestinationID)
	}

	// Verify fields
	if len(contract.Schema.Fields) != 5 {
		t.Errorf("Got %d fields, want 5", len(contract.Schema.Fields))
	}

	// Find id field and verify it has primary key constraint
	var idField *FieldDefinition
	for i := range contract.Schema.Fields {
		if contract.Schema.Fields[i].Name == "id" {
			idField = &contract.Schema.Fields[i]
			break
		}
	}
	if idField == nil {
		t.Fatal("id field not found")
	}

	if idField.DataType != "integer" {
		t.Errorf("id DataType = %v, want integer", idField.DataType)
	}

	if idField.Nullable {
		t.Error("id should not be nullable")
	}

	hasPrimaryKey := false
	for _, c := range idField.Constraints {
		if c.Type == ConstraintPrimaryKey {
			hasPrimaryKey = true
			break
		}
	}
	if !hasPrimaryKey {
		t.Error("id field should have primary key constraint")
	}

	// Verify email field comment
	var emailField *FieldDefinition
	for i := range contract.Schema.Fields {
		if contract.Schema.Fields[i].Name == "email" {
			emailField = &contract.Schema.Fields[i]
			break
		}
	}
	if emailField == nil {
		t.Fatal("email field not found")
	}

	if emailField.Description == nil {
		t.Error("email Description should not be nil")
	} else if *emailField.Description != "User email address" {
		t.Errorf("email Description = %v, want 'User email address'", *emailField.Description)
	}

	// Verify validation rules
	if len(contract.ValidationRules.RequiredFields) < 2 {
		t.Errorf("Expected at least 2 required fields, got %d", len(contract.ValidationRules.RequiredFields))
	}

	if len(contract.ValidationRules.UniqueConstraints) < 2 {
		t.Errorf("Expected at least 2 unique constraints, got %d", len(contract.ValidationRules.UniqueConstraints))
	}

	// Print contract for manual inspection
	if testing.Verbose() {
		b, _ := json.MarshalIndent(contract, "", "  ")
		t.Logf("Generated contract:\n%s", b)
	}

	// Cleanup
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS test_users CASCADE"); err != nil {
		t.Logf("Warning: Failed to cleanup test table: %v", err)
	}
}

func TestAnalyze_Integration_TableNotFound(t *testing.T) {
	connString := os.Getenv("TEST_PG_CONN")
	if connString == "" {
		t.Skip("Skipping integration test: TEST_PG_CONN not set")
	}

	ctx := context.Background()

	_, err := Analyze(ctx, connString, "nonexistent_table", nil)
	if err == nil {
		t.Error("Expected error for nonexistent table, got nil")
	}
}

func TestAnalyze_Integration_InvalidConnection(t *testing.T) {
	ctx := context.Background()

	_, err := Analyze(ctx, "postgres://invalid:invalid@localhost:9999/invalid", "users", nil)
	if err == nil {
		t.Error("Expected error for invalid connection, got nil")
	}
}

// Helper function to get a test pool
func getTestPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, connString)
}
