// Package contract defines the shared data contract types used across
// the schema-style analyzers (excelcontract). A DataContract describes
// the structure, types, and constraints of a structured data source.
package contract

// DataContract is the complete analysis of a structured data endpoint,
// containing one or more schemas (tables, endpoints, file sections).
type DataContract struct {
	ContractType string           `json:"contract_type"`
	ID           string           `json:"id"`
	Schemas      []SchemaContract `json:"schemas"`
	Metadata     Metadata         `json:"metadata,omitempty"`
}

// Metadata holds source-specific facts such as the file format, the
// decimal mark, or sheet issues. It is a map because each analyzer
// records its own keys, and the values are free-form JSON.
type Metadata map[string]any

// SchemaContract describes a single schema within a data endpoint:
// a database table, an API endpoint, or a file structure.
type SchemaContract struct {
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	Namespace       string            `json:"namespace,omitempty"`
	RowCount        *int64            `json:"row_count,omitempty"`
	Fields          []FieldDefinition `json:"fields"`
	SampleData      [][]string        `json:"sample_data,omitempty"`
	ValidationRules ValidationRules   `json:"validation_rules"`
	Issues          []string          `json:"issues,omitempty"`
	Metadata        Metadata          `json:"metadata,omitempty"`
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
	NullCount      int     `json:"null_count"`
	NullPercentage float64 `json:"null_percentage"`
	DistinctCount  int     `json:"distinct_count"`
	// DistinctCountCapped is true when distinct tracking hit its cap, so
	// DistinctCount is a floor ("at least this many") rather than exact
	// and TopValues may be stale. Mirrors profile.FieldProfile's flag so
	// the shared contract type carries the same honesty as the CSV path.
	DistinctCountCapped bool       `json:"distinct_count_capped"`
	MinValue            *string    `json:"min_value"`
	MaxValue            *string    `json:"max_value"`
	TopValues           []TopValue `json:"top_values,omitempty"`
	SampleSize          int        `json:"sample_size"`
	// Shape is the column's value-shape evidence. It is nil for an
	// analyzer that records no shape, and then the JSON has no shape key.
	Shape *ShapeSignature `json:"shape,omitempty"`
}

// ShapeSignature holds the data of profile.ShapeSignature with the same
// JSON tags. The type lives here because profile imports contract, so
// contract cannot name the profile type. Use profile.ShapeFromContract
// to compare two signatures.
type ShapeSignature struct {
	Masks         []MaskCount `json:"masks"`
	DominantShare float64     `json:"dominant_share"`
	LengthMin     int         `json:"length_min"`
	LengthMax     int         `json:"length_max"`
}

// MaskCount is one format mask and how many values matched it.
type MaskCount struct {
	Mask  string `json:"mask"`
	Count int    `json:"count"`
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
