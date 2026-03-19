// Package pgcontract analyzes PostgreSQL database schemas and produces
// destination contracts describing table structures, types, and constraints.
package pgcontract

// DestinationContract is the complete analysis result for a PostgreSQL table.
type DestinationContract struct {
	ContractType    string            `json:"contract_type"`
	DestinationID   string            `json:"destination_id"`
	Schema          DestinationSchema `json:"schema"`
	ValidationRules ValidationRules   `json:"validation_rules"`
	Metadata        map[string]any    `json:"metadata,omitempty"`
}

// DestinationSchema defines the structure of the destination table.
type DestinationSchema struct {
	Fields []FieldDefinition `json:"fields"`
}

// FieldDefinition describes a single column in the table.
type FieldDefinition struct {
	Name        string            `json:"name"`
	DataType    string            `json:"data_type"`
	Nullable    bool              `json:"nullable"`
	Description *string           `json:"description,omitempty"`
	Constraints []FieldConstraint `json:"constraints,omitempty"`
}

// FieldConstraint represents a constraint on a field.
type FieldConstraint struct {
	Type           ConstraintType `json:"type"`
	Value          any            `json:"value,omitempty"`
	ReferredTable  *string        `json:"referred_table,omitempty"`
	ReferredColumn *string        `json:"referred_column,omitempty"`
}

// ConstraintType represents the type of constraint.
type ConstraintType string

const (
	ConstraintNotNull    ConstraintType = "not_null"
	ConstraintUnique     ConstraintType = "unique"
	ConstraintPrimaryKey ConstraintType = "primary_key"
	ConstraintForeignKey ConstraintType = "foreign_key"
	ConstraintCheck      ConstraintType = "check"
)

// ValidationRules defines validation requirements for the destination.
type ValidationRules struct {
	RequiredFields    []string `json:"required_fields,omitempty"`
	UniqueConstraints []string `json:"unique_constraints,omitempty"`
}

// Options controls the analysis behavior.
type Options struct {
	// Schema is the PostgreSQL schema to analyze (default: "public")
	Schema string

	// IncludeComments includes PostgreSQL column comments as descriptions
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
