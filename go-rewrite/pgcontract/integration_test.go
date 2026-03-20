package pgcontract

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAnalyzeDatabase_Integration requires a running PostgreSQL instance.
// Set TEST_PG_CONN to the connection string.
//
// Example:
//
//	export TEST_PG_CONN="postgres://postgres:postgres@localhost:5432/testdb"
//	go test -tags=integration ./pgcontract/... -v
func TestAnalyzeDatabase_Integration(t *testing.T) {
	connString := os.Getenv("TEST_PG_CONN")
	if connString == "" {
		t.Skip("Skipping integration test: TEST_PG_CONN not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	// Create test tables with relationships and seed data
	setup := `
		DROP TABLE IF EXISTS test_orders CASCADE;
		DROP TABLE IF EXISTS test_users CASCADE;

		CREATE TABLE test_users (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL UNIQUE,
			name TEXT NOT NULL,
			bio TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		COMMENT ON COLUMN test_users.email IS 'User email address';
		COMMENT ON COLUMN test_users.bio IS 'Short biography';

		INSERT INTO test_users (email, name, bio) VALUES
			('alice@example.com', 'Alice', 'Engineer'),
			('bob@example.com', 'Bob', NULL),
			('carol@example.com', 'Carol', 'Designer');

		CREATE TABLE test_orders (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES test_users(id),
			total NUMERIC(10,2) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			ordered_at TIMESTAMPTZ DEFAULT NOW()
		);

		INSERT INTO test_orders (user_id, total, status) VALUES
			(1, 99.99, 'shipped'),
			(1, 49.50, 'pending'),
			(2, 200.00, 'shipped'),
			(3, 15.00, 'cancelled');
	`
	if _, err := pool.Exec(ctx, setup); err != nil {
		t.Fatalf("Failed to setup test tables: %v", err)
	}

	// Analyze entire database
	contract, err := AnalyzeDatabase(ctx, connString, &Options{
		Schema:          "public",
		IncludeComments: true,
	})
	if err != nil {
		t.Fatalf("AnalyzeDatabase() error = %v", err)
	}

	if contract.ContractType != "destination" {
		t.Errorf("ContractType = %v, want destination", contract.ContractType)
	}

	if len(contract.Tables) < 2 {
		t.Fatalf("Expected at least 2 tables, got %d", len(contract.Tables))
	}

	// Find the test tables
	var usersTable, ordersTable *TableContract
	for i := range contract.Tables {
		switch contract.Tables[i].TableName {
		case "test_users":
			usersTable = &contract.Tables[i]
		case "test_orders":
			ordersTable = &contract.Tables[i]
		}
	}

	if usersTable == nil {
		t.Fatal("test_users table not found in contract")
	}
	if ordersTable == nil {
		t.Fatal("test_orders table not found in contract")
	}

	// Verify users table
	if len(usersTable.Fields) != 5 {
		t.Errorf("test_users: got %d fields, want 5", len(usersTable.Fields))
	}

	// Verify id has primary key
	idField := findField(usersTable.Fields, "id")
	if idField == nil {
		t.Fatal("id field not found")
	}
	if !hasConstraint(idField.Constraints, ConstraintPrimaryKey) {
		t.Error("id should have primary key constraint")
	}

	// Verify email has comment
	emailField := findField(usersTable.Fields, "email")
	if emailField == nil {
		t.Fatal("email field not found")
	}
	if emailField.Description == nil || *emailField.Description != "User email address" {
		t.Errorf("email description = %v, want 'User email address'", emailField.Description)
	}

	// Verify orders table has foreign key
	userIDField := findField(ordersTable.Fields, "user_id")
	if userIDField == nil {
		t.Fatal("user_id field not found in orders")
	}
	if !hasConstraint(userIDField.Constraints, ConstraintForeignKey) {
		t.Error("user_id should have foreign key constraint")
	}

	// Verify metadata
	if contract.Metadata["table_count"] != len(contract.Tables) {
		t.Errorf("table_count metadata = %v, want %d", contract.Metadata["table_count"], len(contract.Tables))
	}

	// Verify row counts
	if usersTable.RowCount == nil {
		t.Error("users row_count should not be nil")
	} else if *usersTable.RowCount != 3 {
		t.Errorf("users row_count = %d, want 3", *usersTable.RowCount)
	}

	if ordersTable.RowCount == nil {
		t.Error("orders row_count should not be nil")
	} else if *ordersTable.RowCount != 4 {
		t.Errorf("orders row_count = %d, want 4", *ordersTable.RowCount)
	}

	// Verify sample data
	if len(usersTable.SampleData) == 0 {
		t.Error("users should have sample data")
	}
	if len(usersTable.SampleData) > 5 {
		t.Errorf("users sample data = %d rows, want <= 5", len(usersTable.SampleData))
	}

	// Verify profiling on users.bio (has 1 null out of 3)
	bioField := findField(usersTable.Fields, "bio")
	if bioField == nil {
		t.Fatal("bio field not found")
	}
	if bioField.Profile == nil {
		t.Fatal("bio profile should not be nil")
	}
	if bioField.Profile.NullCount != 1 {
		t.Errorf("bio null_count = %d, want 1", bioField.Profile.NullCount)
	}
	if bioField.Profile.SampleSize != 3 {
		t.Errorf("bio sample_size = %d, want 3", bioField.Profile.SampleSize)
	}
	if bioField.Profile.DistinctCount != 2 {
		t.Errorf("bio distinct_count = %d, want 2", bioField.Profile.DistinctCount)
	}

	// Verify profiling on orders.status (has top values)
	statusField := findField(ordersTable.Fields, "status")
	if statusField == nil {
		t.Fatal("status field not found")
	}
	if statusField.Profile == nil {
		t.Fatal("status profile should not be nil")
	}
	if len(statusField.Profile.TopValues) == 0 {
		t.Error("status should have top values")
	}
	// "shipped" appears twice, should be first
	if statusField.Profile.TopValues[0].Value != "shipped" {
		t.Errorf("status top value = %q, want 'shipped'", statusField.Profile.TopValues[0].Value)
	}
	if statusField.Profile.TopValues[0].Count != 2 {
		t.Errorf("status top count = %d, want 2", statusField.Profile.TopValues[0].Count)
	}

	// Print for manual inspection
	if testing.Verbose() {
		b, _ := json.MarshalIndent(contract, "", "  ")
		t.Logf("Generated database contract:\n%s", b)
	}

	// Cleanup
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS test_orders CASCADE; DROP TABLE IF EXISTS test_users CASCADE")
}

func TestAnalyzeTable_Integration(t *testing.T) {
	connString := os.Getenv("TEST_PG_CONN")
	if connString == "" {
		t.Skip("Skipping integration test: TEST_PG_CONN not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	setup := `
		DROP TABLE IF EXISTS test_single CASCADE;
		CREATE TABLE test_single (
			id SERIAL PRIMARY KEY,
			value TEXT NOT NULL
		);
	`
	if _, err := pool.Exec(ctx, setup); err != nil {
		t.Fatalf("Failed to setup: %v", err)
	}

	table, err := AnalyzeTable(ctx, connString, "test_single", nil)
	if err != nil {
		t.Fatalf("AnalyzeTable() error = %v", err)
	}

	if table.TableName != "test_single" {
		t.Errorf("TableName = %v, want test_single", table.TableName)
	}
	if len(table.Fields) != 2 {
		t.Errorf("Fields count = %d, want 2", len(table.Fields))
	}

	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS test_single CASCADE")
}

func TestAnalyzeTable_Integration_TableNotFound(t *testing.T) {
	connString := os.Getenv("TEST_PG_CONN")
	if connString == "" {
		t.Skip("Skipping integration test: TEST_PG_CONN not set")
	}

	ctx := context.Background()
	_, err := AnalyzeTable(ctx, connString, "nonexistent_table_xyz", nil)
	if err == nil {
		t.Error("Expected error for nonexistent table, got nil")
	}
}

func TestAnalyzeDatabase_Integration_InvalidConnection(t *testing.T) {
	ctx := context.Background()
	_, err := AnalyzeDatabase(ctx, "postgres://invalid:invalid@localhost:9999/invalid", nil)
	if err == nil {
		t.Error("Expected error for invalid connection, got nil")
	}
}

// helpers

func findField(fields []FieldDefinition, name string) *FieldDefinition {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}

func hasConstraint(constraints []FieldConstraint, ct ConstraintType) bool {
	for _, c := range constraints {
		if c.Type == ct {
			return true
		}
	}
	return false
}
