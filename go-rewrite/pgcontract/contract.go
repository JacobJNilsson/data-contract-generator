// Package pgcontract analyzes PostgreSQL database schemas and produces
// contracts describing table structures, types, and constraints. The AI
// agent uses these contracts to decide which table to ingest data into
// or extract data from.
package pgcontract

// DatabaseContract is the complete analysis of a PostgreSQL database,
// containing every table in the target schema.
type DatabaseContract struct {
	ContractType string          `json:"contract_type"`
	DatabaseID   string          `json:"database_id"`
	Tables       []TableContract `json:"tables"`
	Metadata     map[string]any  `json:"metadata,omitempty"`
}

// TableContract describes a single table: its columns, types, constraints,
// and a sample of the data it contains.
type TableContract struct {
	TableName       string            `json:"table_name"`
	Schema          string            `json:"schema"`
	RowCount        *int64            `json:"row_count,omitempty"`
	Fields          []FieldDefinition `json:"fields"`
	SampleData      [][]string        `json:"sample_data,omitempty"`
	ValidationRules ValidationRules   `json:"validation_rules"`
}

// FieldDefinition describes a single column in a table.
type FieldDefinition struct {
	Name        string            `json:"name"`
	DataType    string            `json:"data_type"`
	Nullable    bool              `json:"nullable"`
	Description *string           `json:"description,omitempty"`
	Constraints []FieldConstraint `json:"constraints,omitempty"`
	Profile     *FieldProfile     `json:"profile,omitempty"`
}

// FieldProfile contains statistical observations about a column's values,
// computed from a bounded sample (default 1000 rows).
type FieldProfile struct {
	NullCount      int        `json:"null_count"`
	NullPercentage float64    `json:"null_percentage"`
	DistinctCount  int        `json:"distinct_count"`
	MinValue       *string    `json:"min_value"`
	MaxValue       *string    `json:"max_value"`
	TopValues      []TopValue `json:"top_values,omitempty"`
	SampleSize     int        `json:"sample_size"`
}

// TopValue pairs a value with how many times it appears in the sample.
type TopValue struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// FieldConstraint represents a constraint on a column.
type FieldConstraint struct {
	Type           ConstraintType `json:"type"`
	Value          any            `json:"value,omitempty"`
	ReferredTable  *string        `json:"referred_table,omitempty"`
	ReferredColumn *string        `json:"referred_column,omitempty"`
}

// ConstraintType enumerates the supported constraint types.
type ConstraintType string

const (
	ConstraintNotNull    ConstraintType = "not_null"
	ConstraintUnique     ConstraintType = "unique"
	ConstraintPrimaryKey ConstraintType = "primary_key"
	ConstraintForeignKey ConstraintType = "foreign_key"
	ConstraintCheck      ConstraintType = "check"
)

// ValidationRules summarises the table-level validation requirements.
type ValidationRules struct {
	RequiredFields    []string `json:"required_fields,omitempty"`
	UniqueConstraints []string `json:"unique_constraints,omitempty"`
}

// Options controls the analysis behaviour.
type Options struct {
	// Schema is the PostgreSQL schema to analyze (default: "public").
	Schema string

	// IncludeComments includes PostgreSQL column comments as field descriptions.
	IncludeComments bool

	// SampleSize is the maximum number of rows to sample per table for
	// profiling. Zero means use the default (10000). Rows are fetched
	// in batches of BatchSize to bound memory.
	SampleSize int

	// BatchSize is the number of rows fetched per batch during profiling.
	// Zero means use the default (1000).
	BatchSize int

	// MaxSampleRows is the maximum number of rows to include in
	// SampleData. Zero means use the default (5).
	MaxSampleRows int

	// TopN is the number of most frequent values to report per column.
	// Zero means use the default (5).
	TopN int
}

func (o *Options) schema() string {
	if o == nil || o.Schema == "" {
		return "public"
	}
	return o.Schema
}

func (o *Options) includeComments() bool {
	return o != nil && o.IncludeComments
}

func (o *Options) sampleSize() int {
	if o == nil || o.SampleSize <= 0 {
		return 10000
	}
	return o.SampleSize
}

func (o *Options) batchSize() int {
	if o == nil || o.BatchSize <= 0 {
		return 1000
	}
	return o.BatchSize
}

func (o *Options) maxSampleRows() int {
	if o == nil || o.MaxSampleRows <= 0 {
		return 5
	}
	return o.MaxSampleRows
}

func (o *Options) topN() int {
	if o == nil || o.TopN <= 0 {
		return 5
	}
	return o.TopN
}
