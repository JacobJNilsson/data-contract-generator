package pgcontract

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/JacobJNilsson/data-contract-generator/contract"
)

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

	dc, err := AnalyzeDatabase(ctx, connString, &Options{
		Schema:          "public",
		IncludeComments: true,
	})
	if err != nil {
		t.Fatalf("AnalyzeDatabase() error = %v", err)
	}

	if dc.ContractType != "destination" {
		t.Errorf("ContractType = %v, want destination", dc.ContractType)
	}

	if len(dc.Schemas) < 2 {
		t.Fatalf("Expected at least 2 schemas, got %d", len(dc.Schemas))
	}

	var usersSchema, ordersSchema *contract.SchemaContract
	for i := range dc.Schemas {
		switch dc.Schemas[i].Name {
		case "test_users":
			usersSchema = &dc.Schemas[i]
		case "test_orders":
			ordersSchema = &dc.Schemas[i]
		}
	}

	if usersSchema == nil {
		t.Fatal("test_users schema not found")
	}
	if ordersSchema == nil {
		t.Fatal("test_orders schema not found")
	}

	if len(usersSchema.Fields) != 5 {
		t.Errorf("test_users: got %d fields, want 5", len(usersSchema.Fields))
	}

	idField := findField(usersSchema.Fields, "id")
	if idField == nil {
		t.Fatal("id field not found")
	}
	if !hasConstraint(idField.Constraints, contract.ConstraintPrimaryKey) {
		t.Error("id should have primary key constraint")
	}

	emailField := findField(usersSchema.Fields, "email")
	if emailField == nil {
		t.Fatal("email field not found")
	}
	if emailField.Description == nil || *emailField.Description != "User email address" {
		t.Errorf("email description = %v, want 'User email address'", emailField.Description)
	}

	userIDField := findField(ordersSchema.Fields, "user_id")
	if userIDField == nil {
		t.Fatal("user_id field not found in orders")
	}
	if !hasConstraint(userIDField.Constraints, contract.ConstraintForeignKey) {
		t.Error("user_id should have foreign key constraint")
	}

	if dc.Metadata["table_count"] != len(dc.Schemas) {
		t.Errorf("table_count metadata = %v, want %d", dc.Metadata["table_count"], len(dc.Schemas))
	}

	if usersSchema.RowCount == nil {
		t.Error("users row_count should not be nil")
	} else if *usersSchema.RowCount != 3 {
		t.Errorf("users row_count = %d, want 3", *usersSchema.RowCount)
	}

	if ordersSchema.RowCount == nil {
		t.Error("orders row_count should not be nil")
	} else if *ordersSchema.RowCount != 4 {
		t.Errorf("orders row_count = %d, want 4", *ordersSchema.RowCount)
	}

	if len(usersSchema.SampleData) == 0 {
		t.Error("users should have sample data")
	}
	if len(usersSchema.SampleData) > 5 {
		t.Errorf("users sample data = %d rows, want <= 5", len(usersSchema.SampleData))
	}

	bioField := findField(usersSchema.Fields, "bio")
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

	statusField := findField(ordersSchema.Fields, "status")
	if statusField == nil {
		t.Fatal("status field not found")
	}
	if statusField.Profile == nil {
		t.Fatal("status profile should not be nil")
	}
	if len(statusField.Profile.TopValues) == 0 {
		t.Error("status should have top values")
	}
	if statusField.Profile.TopValues[0].Value != "shipped" {
		t.Errorf("status top value = %q, want 'shipped'", statusField.Profile.TopValues[0].Value)
	}
	if statusField.Profile.TopValues[0].Count != 2 {
		t.Errorf("status top count = %d, want 2", statusField.Profile.TopValues[0].Count)
	}

	if testing.Verbose() {
		b, _ := json.MarshalIndent(dc, "", "  ")
		t.Logf("Generated data contract:\n%s", b)
	}

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

	sc, err := AnalyzeTable(ctx, connString, "test_single", nil)
	if err != nil {
		t.Fatalf("AnalyzeTable() error = %v", err)
	}

	if sc.Name != "test_single" {
		t.Errorf("Name = %v, want test_single", sc.Name)
	}
	if len(sc.Fields) != 2 {
		t.Errorf("Fields count = %d, want 2", len(sc.Fields))
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

func findField(fields []contract.FieldDefinition, name string) *contract.FieldDefinition {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}

func hasConstraint(constraints []contract.FieldConstraint, ct contract.ConstraintType) bool {
	for _, c := range constraints {
		if c.Type == ct {
			return true
		}
	}
	return false
}
