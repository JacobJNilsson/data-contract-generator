// Package transform defines transformation contracts that map source
// fields to destination fields across one or more sources and
// destinations. The AI agent uses these to generate data ingestion code.
package transform

// Contract describes how to transform data from one or more sources
// to one or more destinations. Each destination schema gets its own
// MappingGroup with field-level mappings.
type Contract struct {
	ContractType    string         `json:"contract_type"`
	TransformID     string         `json:"transformation_id"`
	SourceRefs      []string       `json:"source_refs"`
	DestinationRefs []string       `json:"destination_refs"`
	MappingGroups   []MappingGroup `json:"mapping_groups"`
	ExecutionPlan   ExecutionPlan  `json:"execution_plan"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// MappingGroup holds field mappings for a single destination schema.
type MappingGroup struct {
	DestinationRef string         `json:"destination_ref"`
	FieldMappings  []FieldMapping `json:"field_mappings"`
}

// FieldMapping maps a single destination field to a source, which can be
// a source field (qualified by SourceRef), an explicit null, a constant
// value, or a transform computed from multiple source fields.
type FieldMapping struct {
	DestinationField string               `json:"destination_field"`
	SourceType       SourceType           `json:"source_type"`
	SourceRef        string               `json:"source_ref,omitempty"`
	SourceField      string               `json:"source_field,omitempty"`
	SourceConstant   string               `json:"source_constant,omitempty"`
	Transformation   *FieldTransformation `json:"transformation,omitempty"`
	Confidence       float64              `json:"confidence"`
}

// SourceType indicates how the destination field gets its value.
type SourceType string

// Supported source types for field mappings.
const (
	// SourceTypeUnmapped means the user has not yet decided how to fill
	// this destination field. Verification will flag non-nullable fields
	// with this source type as needing attention.
	SourceTypeUnmapped SourceType = "unmapped"
	// SourceTypeField maps from a source field.
	SourceTypeField SourceType = "field"
	// SourceTypeNull sets the destination field to null.
	SourceTypeNull SourceType = "null"
	// SourceTypeConstant sets the destination field to a constant value.
	SourceTypeConstant SourceType = "constant"
	// SourceTypeTransform computes the destination field from one or more
	// source fields with a free-text description of the transformation.
	SourceTypeTransform SourceType = "transform"
)

// FieldTransformation describes how to convert a source value to fit
// the destination.
type FieldTransformation struct {
	Type       Type           `json:"type"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

// Type enumerates supported transformation types.
type Type string

// Supported transformation types.
const (
	TypeRename  Type = "rename"
	TypeCast    Type = "cast"
	TypeFormat  Type = "format"
	TypeDefault Type = "default"
)

// ExecutionPlan controls the runtime behavior of the transformation.
type ExecutionPlan struct {
	BatchSize         int     `json:"batch_size"`
	ErrorThreshold    float64 `json:"error_threshold"`
	ValidationEnabled bool    `json:"validation_enabled"`
}

// DefaultExecutionPlan returns sensible defaults for execution.
func DefaultExecutionPlan() ExecutionPlan {
	return ExecutionPlan{
		BatchSize:         100,
		ErrorThreshold:    0.1,
		ValidationEnabled: true,
	}
}
