package pgcontract

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Analyze connects to a PostgreSQL database and generates a destination contract
// for the specified table.
func Analyze(ctx context.Context, connString, tableName string, opts *Options) (*DestinationContract, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	schema := opts.schema()
	includeComments := opts.includeComments()

	// Check if table exists
	exists, err := tableExists(ctx, pool, schema, tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table %s.%s does not exist", schema, tableName)
	}

	// Get columns
	fields, err := getColumns(ctx, pool, schema, tableName, includeComments)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get constraints
	constraints, err := getConstraints(ctx, pool, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get constraints: %w", err)
	}

	// Apply constraints to fields
	applyConstraints(fields, constraints)

	// Build validation rules
	rules := buildValidationRules(fields, constraints)

	contract := &DestinationContract{
		ContractType:  "destination",
		DestinationID: fmt.Sprintf("%s.%s", schema, tableName),
		Schema: DestinationSchema{
			Fields: fields,
		},
		ValidationRules: rules,
		Metadata: map[string]any{
			"source":     "postgresql",
			"schema":     schema,
			"table":      tableName,
			"connection": sanitizeConnString(connString),
		},
	}

	return contract, nil
}

// tableExists checks if a table exists in the specified schema.
func tableExists(ctx context.Context, pool *pgxpool.Pool, schema, tableName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = $1 
			AND table_name = $2
		)
	`
	var exists bool
	err := pool.QueryRow(ctx, query, schema, tableName).Scan(&exists)
	return exists, err
}

// columnInfo represents information about a single column.
type columnInfo struct {
	Name        string
	DataType    string
	IsNullable  bool
	Description *string
}

// getColumns retrieves column information from the database.
func getColumns(ctx context.Context, pool *pgxpool.Pool, schema, tableName string, includeComments bool) ([]FieldDefinition, error) {
	query := `
		SELECT 
			c.column_name,
			c.data_type,
			c.is_nullable = 'YES' as is_nullable,
			CASE 
				WHEN c.data_type = 'character varying' THEN 'varchar(' || c.character_maximum_length || ')'
				WHEN c.data_type = 'character' THEN 'char(' || c.character_maximum_length || ')'
				WHEN c.data_type = 'numeric' AND c.numeric_precision IS NOT NULL THEN 
					'numeric(' || c.numeric_precision || ',' || c.numeric_scale || ')'
				ELSE c.data_type
			END as full_type
		FROM information_schema.columns c
		WHERE c.table_schema = $1 
		AND c.table_name = $2
		ORDER BY c.ordinal_position
	`

	rows, err := pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []FieldDefinition
	for rows.Next() {
		var name, dataType, fullType string
		var isNullable bool
		if err := rows.Scan(&name, &dataType, &isNullable, &fullType); err != nil {
			return nil, err
		}

		field := FieldDefinition{
			Name:     name,
			DataType: normalizeDataType(fullType),
			Nullable: isNullable,
		}

		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Get column comments if requested
	if includeComments {
		if err := addColumnComments(ctx, pool, schema, tableName, fields); err != nil {
			return nil, err
		}
	}

	return fields, nil
}

// addColumnComments adds PostgreSQL column comments as descriptions.
func addColumnComments(ctx context.Context, pool *pgxpool.Pool, schema, tableName string, fields []FieldDefinition) error {
	query := `
		SELECT 
			a.attname as column_name,
			d.description
		FROM pg_catalog.pg_description d
		JOIN pg_catalog.pg_class c ON d.objoid = c.oid
		JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid AND d.objsubid = a.attnum
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 
		AND c.relname = $2
		AND d.description IS NOT NULL
	`

	rows, err := pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	comments := make(map[string]string)
	for rows.Next() {
		var columnName, description string
		if err := rows.Scan(&columnName, &description); err != nil {
			return err
		}
		comments[columnName] = description
	}

	// Apply comments to fields
	for i := range fields {
		if desc, ok := comments[fields[i].Name]; ok {
			fields[i].Description = &desc
		}
	}

	return rows.Err()
}

// constraintInfo represents a constraint on a column.
type constraintInfo struct {
	ColumnName     string
	ConstraintType string
	ConstraintName string
	RefTable       *string
	RefColumn      *string
}

// getConstraints retrieves constraint information from the database.
func getConstraints(ctx context.Context, pool *pgxpool.Pool, schema, tableName string) ([]constraintInfo, error) {
	query := `
		SELECT 
			kcu.column_name,
			tc.constraint_type,
			tc.constraint_name,
			ccu.table_name as ref_table,
			ccu.column_name as ref_column
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name 
			AND tc.table_schema = kcu.table_schema
		LEFT JOIN information_schema.constraint_column_usage ccu
			ON tc.constraint_name = ccu.constraint_name
			AND tc.table_schema = ccu.table_schema
		WHERE tc.table_schema = $1 
		AND tc.table_name = $2
		AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
		ORDER BY kcu.ordinal_position
	`

	rows, err := pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []constraintInfo
	for rows.Next() {
		var c constraintInfo
		if err := rows.Scan(&c.ColumnName, &c.ConstraintType, &c.ConstraintName, &c.RefTable, &c.RefColumn); err != nil {
			return nil, err
		}
		constraints = append(constraints, c)
	}

	return constraints, rows.Err()
}

// applyConstraints adds constraints to field definitions.
func applyConstraints(fields []FieldDefinition, constraints []constraintInfo) {
	// Group constraints by column
	byColumn := make(map[string][]constraintInfo)
	for _, c := range constraints {
		byColumn[c.ColumnName] = append(byColumn[c.ColumnName], c)
	}

	// Apply to fields
	for i := range fields {
		colConstraints := byColumn[fields[i].Name]
		for _, c := range colConstraints {
			fc := FieldConstraint{}

			switch c.ConstraintType {
			case "PRIMARY KEY":
				fc.Type = ConstraintPrimaryKey
			case "FOREIGN KEY":
				fc.Type = ConstraintForeignKey
				fc.ReferredTable = c.RefTable
				fc.ReferredColumn = c.RefColumn
			case "UNIQUE":
				fc.Type = ConstraintUnique
			}

			fields[i].Constraints = append(fields[i].Constraints, fc)
		}

		// Add NOT NULL constraint if applicable
		if !fields[i].Nullable {
			fields[i].Constraints = append(fields[i].Constraints, FieldConstraint{
				Type: ConstraintNotNull,
			})
		}
	}
}

// buildValidationRules creates validation rules from field definitions and constraints.
func buildValidationRules(fields []FieldDefinition, constraints []constraintInfo) ValidationRules {
	var rules ValidationRules

	// Collect required fields (NOT NULL)
	for _, f := range fields {
		if !f.Nullable {
			rules.RequiredFields = append(rules.RequiredFields, f.Name)
		}
	}

	// Collect unique constraints
	uniqueCols := make(map[string]bool)
	for _, c := range constraints {
		if c.ConstraintType == "UNIQUE" || c.ConstraintType == "PRIMARY KEY" {
			uniqueCols[c.ColumnName] = true
		}
	}
	for col := range uniqueCols {
		rules.UniqueConstraints = append(rules.UniqueConstraints, col)
	}

	return rules
}

// normalizeDataType normalizes PostgreSQL data types to common names.
func normalizeDataType(pgType string) string {
	// Map PostgreSQL types to standard contract types
	typeMap := map[string]string{
		"integer":                     "integer",
		"int":                         "integer",
		"int4":                        "integer",
		"bigint":                      "bigint",
		"int8":                        "bigint",
		"smallint":                    "smallint",
		"int2":                        "smallint",
		"text":                        "text",
		"character varying":           "varchar",
		"varchar":                     "varchar",
		"character":                   "char",
		"char":                        "char",
		"boolean":                     "boolean",
		"bool":                        "boolean",
		"date":                        "date",
		"timestamp":                   "timestamp",
		"timestamp without time zone": "timestamp",
		"timestamp with time zone":    "timestamptz",
		"timestamptz":                 "timestamptz",
		"numeric":                     "numeric",
		"decimal":                     "numeric",
		"real":                        "real",
		"double precision":            "double",
		"json":                        "json",
		"jsonb":                       "jsonb",
		"uuid":                        "uuid",
		"bytea":                       "bytea",
	}

	// Check exact match first
	if normalized, ok := typeMap[pgType]; ok {
		return normalized
	}

	// Handle types with parameters (e.g., varchar(255), char(10), numeric(10,2))
	// Check if pgType starts with any known type prefix
	for i := len(pgType); i > 0; i-- {
		if pgType[i-1] == '(' {
			// Found opening parenthesis, check the prefix
			prefix := pgType[:i-1]
			if normalized, ok := typeMap[prefix]; ok {
				return normalized
			}
		}
	}

	// Return as-is if no mapping found
	return pgType
}

// sanitizeConnString removes sensitive information from connection string.
func sanitizeConnString(connString string) string {
	// Parse and sanitize the connection string
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return "[connection string]"
	}

	// Return sanitized version without password
	return fmt.Sprintf("%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, cfg.Database)
}
