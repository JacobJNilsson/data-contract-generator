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

// TableContract describes a single table: its columns, types, and constraints.
type TableContract struct {
	TableName       string            `json:"table_name"`
	Schema          string            `json:"schema"`
	Fields          []FieldDefinition `json:"fields"`
	ValidationRules ValidationRules   `json:"validation_rules"`
}

// FieldDefinition describes a single column in a table.
type FieldDefinition struct {
	Name        string            `json:"name"`
	DataType    string            `json:"data_type"`
	Nullable    bool              `json:"nullable"`
	Description *string           `json:"description,omitempty"`
	Constraints []FieldConstraint `json:"constraints,omitempty"`
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
