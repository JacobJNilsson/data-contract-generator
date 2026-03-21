// Package contract defines the shared data contract types used across
// all analyzers (pgcontract, supacontract, apicontract). A DataContract
// describes the structure, types, and constraints of any structured
// data endpoint — databases, APIs, or files.
package contract

// DataContract is the complete analysis of a structured data endpoint,
// containing one or more schemas (tables, endpoints, file sections).
type DataContract struct {
	ContractType string           `json:"contract_type"`
	ID           string           `json:"id"`
	Schemas      []SchemaContract `json:"schemas"`
	Metadata     map[string]any   `json:"metadata,omitempty"`
}

// SchemaContract describes a single schema within a data endpoint:
// a database table, an API endpoint, or a file structure.
type SchemaContract struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace,omitempty"`
	RowCount        *int64            `json:"row_count,omitempty"`
	Fields          []FieldDefinition `json:"fields"`
	SampleData      [][]string        `json:"sample_data,omitempty"`
	ValidationRules ValidationRules   `json:"validation_rules"`
	Issues          []string          `json:"issues,omitempty"`
}

// FieldDefinition describes a single field in a schema.
type FieldDefinition struct {
	Name        string            `json:"name"`
	DataType    string            `json:"data_type"`
	Nullable    bool              `json:"nullable"`
	Description *string           `json:"description,omitempty"`
	Constraints []FieldConstraint `json:"constraints,omitempty"`
	Profile     *FieldProfile     `json:"profile,omitempty"`
}

// FieldProfile contains statistical observations about a field's values,
// computed from a bounded sample.
type FieldProfile struct {
	NullCount      int        `json:"null_count"`
	NullPercentage float64    `json:"null_percentage"`
	DistinctCount  int        `json:"distinct_count"`
	MinValue       *string    `json:"min_value"`
	MaxValue       *string    `json:"max_value"`
	TopValues      []TopValue `json:"top_values,omitempty"`
	SampleSize     int        `json:"sample_size"`
}

// TopValue pairs a value with how many times it appears.
type TopValue struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// FieldConstraint represents a constraint on a field.
type FieldConstraint struct {
	Type           ConstraintType `json:"type"`
	ReferredTable  *string        `json:"referred_table,omitempty"`
	ReferredColumn *string        `json:"referred_column,omitempty"`
}

// ConstraintType enumerates the supported constraint types.
type ConstraintType string

// Supported constraint types.
const (
	ConstraintNotNull    ConstraintType = "not_null"
	ConstraintUnique     ConstraintType = "unique"
	ConstraintPrimaryKey ConstraintType = "primary_key"
	ConstraintForeignKey ConstraintType = "foreign_key"
)

// ValidationRules summarises schema-level validation requirements.
type ValidationRules struct {
	RequiredFields    []string `json:"required_fields,omitempty"`
	UniqueConstraints []string `json:"unique_constraints,omitempty"`
}
