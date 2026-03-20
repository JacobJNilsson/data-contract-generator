package pgcontract

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AnalyzeDatabase connects to a PostgreSQL database and generates a contract
// describing every table in the specified schema. The AI agent uses this to
// decide which table to ingest data into or extract data from.
func AnalyzeDatabase(ctx context.Context, connString string, opts *Options) (*DatabaseContract, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	schema := opts.schema()

	tableNames, err := listTables(ctx, pool, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	tables := make([]TableContract, 0, len(tableNames))
	for _, name := range tableNames {
		table, err := analyzeTable(ctx, pool, schema, name, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to analyze table %s: %w", name, err)
		}
		tables = append(tables, *table)
	}

	var dbName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		return nil, fmt.Errorf("failed to get database name: %w", err)
	}

	return &DatabaseContract{
		ContractType: "destination",
		DatabaseID:   dbName,
		Tables:       tables,
		Metadata: map[string]any{
			"source":      "postgresql",
			"schema":      schema,
			"table_count": len(tables),
			"connection":  sanitizeConnString(connString),
		},
	}, nil
}

// AnalyzeTable connects to a PostgreSQL database and generates a contract
// for a single table.
func AnalyzeTable(ctx context.Context, connString, tableName string, opts *Options) (*TableContract, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	schema := opts.schema()

	exists, err := tableExists(ctx, pool, schema, tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table %s.%s does not exist", schema, tableName)
	}

	return analyzeTable(ctx, pool, schema, tableName, opts)
}

// --- internal helpers -------------------------------------------------------

// listTables returns all user table names in the specified schema.
func listTables(ctx context.Context, pool *pgxpool.Pool, schema string) ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = $1 
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := pool.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// tableExists checks if a table exists in the specified schema.
func tableExists(ctx context.Context, pool *pgxpool.Pool, schema, tableName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2
		)
	`
	var exists bool
	err := pool.QueryRow(ctx, query, schema, tableName).Scan(&exists)
	return exists, err
}

// analyzeTable introspects a single table and returns its contract,
// including data profiling from a bounded sample.
func analyzeTable(ctx context.Context, pool *pgxpool.Pool, schema, tableName string, opts *Options) (*TableContract, error) {
	fields, err := getColumns(ctx, pool, schema, tableName, opts.includeComments())
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	constraints, err := getConstraints(ctx, pool, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get constraints: %w", err)
	}

	applyConstraints(fields, constraints)

	// Get row count
	rowCount, err := getRowCount(ctx, pool, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// Profile data from a bounded sample
	sampleData, err := profileFields(ctx, pool, schema, tableName, fields, opts)
	if err != nil {
		// Profiling failure is non-fatal — we still have schema info
		sampleData = nil
	}

	return &TableContract{
		TableName:       tableName,
		Schema:          schema,
		RowCount:        &rowCount,
		Fields:          fields,
		SampleData:      sampleData,
		ValidationRules: buildValidationRules(fields, constraints),
	}, nil
}

// getRowCount returns the approximate row count for a table using pg_stat.
// Falls back to COUNT(*) with a limit for accuracy on small tables.
func getRowCount(ctx context.Context, pool *pgxpool.Pool, schema, tableName string) (int64, error) {
	// Use pg_stat for large tables (fast estimate)
	var estimate int64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = $2
	`, schema, tableName).Scan(&estimate)
	if err != nil {
		return 0, err
	}

	// If the estimate is low, get an exact count (cheap for small tables)
	if estimate < 10000 {
		var exact int64
		err := pool.QueryRow(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %q.%q", schema, tableName),
		).Scan(&exact)
		if err != nil {
			return estimate, nil // fall back to estimate
		}
		return exact, nil
	}

	return estimate, nil
}

// profileFields samples rows from the table in batches and computes
// per-column statistics. The total number of rows read is bounded by
// opts.sampleSize (default 10 000) and each batch fetches opts.batchSize
// rows (default 1000) to keep memory usage predictable.
func profileFields(ctx context.Context, pool *pgxpool.Pool, schema, tableName string, fields []FieldDefinition, opts *Options) ([][]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	sampleLimit := opts.sampleSize()
	batchSize := opts.batchSize()
	maxSample := opts.maxSampleRows()
	topN := opts.topN()

	// Build column list
	colList := ""
	for i, f := range fields {
		if i > 0 {
			colList += ", "
		}
		colList += fmt.Sprintf("%q", f.Name)
	}

	// Per-column accumulators
	type colStats struct {
		nulls    int
		min      *string
		max      *string
		freqs    map[string]int
		distinct map[string]bool
	}
	stats := make([]colStats, len(fields))
	for i := range stats {
		stats[i].freqs = make(map[string]int)
		stats[i].distinct = make(map[string]bool)
	}

	var sampleData [][]string
	totalRows := 0

	// Fetch in batches using LIMIT/OFFSET
	for offset := 0; offset < sampleLimit; offset += batchSize {
		remaining := sampleLimit - offset
		limit := batchSize
		if limit > remaining {
			limit = remaining
		}

		query := fmt.Sprintf(
			"SELECT %s FROM %q.%q LIMIT %d OFFSET %d",
			colList, schema, tableName, limit, offset,
		)

		rows, err := pool.Query(ctx, query)
		if err != nil {
			return nil, err
		}

		batchCount := 0
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				rows.Close()
				return nil, err
			}
			totalRows++
			batchCount++

			row := make([]string, len(fields))
			for i, v := range vals {
				if v == nil {
					stats[i].nulls++
					row[i] = ""
					continue
				}

				s := fmt.Sprintf("%v", v)
				row[i] = s

				// Min/max (lexicographic; good enough for profiling)
				if stats[i].min == nil || s < *stats[i].min {
					cp := s
					stats[i].min = &cp
				}
				if stats[i].max == nil || s > *stats[i].max {
					cp := s
					stats[i].max = &cp
				}

				stats[i].distinct[s] = true

				// Track frequency (cap map to avoid memory blowup)
				if len(stats[i].freqs) < 10000 {
					stats[i].freqs[s]++
				} else if _, exists := stats[i].freqs[s]; exists {
					stats[i].freqs[s]++
				}
			}

			if len(sampleData) < maxSample {
				sampleData = append(sampleData, row)
			}
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, err
		}

		// Table exhausted before reaching sampleLimit
		if batchCount < limit {
			break
		}
	}

	// Build profiles
	for i := range fields {
		if totalRows == 0 {
			continue
		}

		s := stats[i]
		profile := &FieldProfile{
			NullCount:      s.nulls,
			NullPercentage: float64(s.nulls) / float64(totalRows) * 100,
			DistinctCount:  len(s.distinct),
			MinValue:       s.min,
			MaxValue:       s.max,
			SampleSize:     totalRows,
			TopValues:      topNValues(s.freqs, topN),
		}

		fields[i].Profile = profile
	}

	return sampleData, nil
}

// topNValues returns the N most frequent values from a frequency map,
// sorted by count descending then key ascending for deterministic output.
func topNValues(freqs map[string]int, n int) []TopValue {
	if len(freqs) == 0 {
		return nil
	}

	type kv struct {
		key   string
		count int
	}
	sorted := make([]kv, 0, len(freqs))
	for k, v := range freqs {
		sorted = append(sorted, kv{k, v})
	}

	for i := range sorted {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].count > sorted[i].count ||
				(sorted[j].count == sorted[i].count && sorted[j].key < sorted[i].key) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	if n > len(sorted) {
		n = len(sorted)
	}

	result := make([]TopValue, n)
	for i := 0; i < n; i++ {
		result[i] = TopValue{Value: sorted[i].key, Count: sorted[i].count}
	}
	return result
}

// getColumns retrieves column information from information_schema.
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
		WHERE c.table_schema = $1 AND c.table_name = $2
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
		fields = append(fields, FieldDefinition{
			Name:     name,
			DataType: normalizeDataType(fullType),
			Nullable: isNullable,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if includeComments {
		if err := addColumnComments(ctx, pool, schema, tableName, fields); err != nil {
			return nil, err
		}
	}

	return fields, nil
}

// addColumnComments reads pg_description and sets field Description pointers.
func addColumnComments(ctx context.Context, pool *pgxpool.Pool, schema, tableName string, fields []FieldDefinition) error {
	query := `
		SELECT a.attname, d.description
		FROM pg_catalog.pg_description d
		JOIN pg_catalog.pg_class c ON d.objoid = c.oid
		JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid AND d.objsubid = a.attnum
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1 AND c.relname = $2
		AND d.description IS NOT NULL
	`

	rows, err := pool.Query(ctx, query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	comments := make(map[string]string)
	for rows.Next() {
		var col, desc string
		if err := rows.Scan(&col, &desc); err != nil {
			return err
		}
		comments[col] = desc
	}

	for i := range fields {
		if desc, ok := comments[fields[i].Name]; ok {
			fields[i].Description = &desc
		}
	}

	return rows.Err()
}

// constraintInfo represents a raw constraint row from the database.
type constraintInfo struct {
	ColumnName     string
	ConstraintType string
	ConstraintName string
	RefTable       *string
	RefColumn      *string
}

// getConstraints retrieves constraint information for a table.
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
		WHERE tc.table_schema = $1 AND tc.table_name = $2
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

// applyConstraints annotates field definitions with their constraints.
func applyConstraints(fields []FieldDefinition, constraints []constraintInfo) {
	byColumn := make(map[string][]constraintInfo)
	for _, c := range constraints {
		byColumn[c.ColumnName] = append(byColumn[c.ColumnName], c)
	}

	for i := range fields {
		for _, c := range byColumn[fields[i].Name] {
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

		if !fields[i].Nullable {
			fields[i].Constraints = append(fields[i].Constraints, FieldConstraint{
				Type: ConstraintNotNull,
			})
		}
	}
}

// buildValidationRules derives validation rules from fields and constraints.
func buildValidationRules(fields []FieldDefinition, constraints []constraintInfo) ValidationRules {
	var rules ValidationRules

	for _, f := range fields {
		if !f.Nullable {
			rules.RequiredFields = append(rules.RequiredFields, f.Name)
		}
	}

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

// normalizeDataType maps PostgreSQL types to standard contract type names.
func normalizeDataType(pgType string) string {
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

	if normalized, ok := typeMap[pgType]; ok {
		return normalized
	}

	// Handle types with parameters (e.g. varchar(255), numeric(10,2))
	for i := len(pgType); i > 0; i-- {
		if pgType[i-1] == '(' {
			if normalized, ok := typeMap[pgType[:i-1]]; ok {
				return normalized
			}
		}
	}

	return pgType
}

// sanitizeConnString strips the password from a connection string.
func sanitizeConnString(connString string) string {
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return "[connection string]"
	}
	return fmt.Sprintf("%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, cfg.Database)
}
