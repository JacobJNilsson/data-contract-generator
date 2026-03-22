// Package transform defines transformation contracts that map source
// fields to destination fields. The AI agent uses these to generate
// data ingestion code.
package transform

// Contract describes how to transform data from a source to a destination.
type Contract struct {
	ContractType   string         `json:"contract_type"`
	TransformID    string         `json:"transformation_id"`
	SourceRef      string         `json:"source_ref"`
	DestinationRef string         `json:"destination_ref"`
	FieldMappings  []FieldMapping `json:"field_mappings"`
	ExecutionPlan  ExecutionPlan  `json:"execution_plan"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// FieldMapping maps a single destination field to a source, which can be
// a source field, an explicit null, or a constant value.
type FieldMapping struct {
	DestinationField string               `json:"destination_field"`
	SourceType       SourceType           `json:"source_type"`
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
	// SuggestMappings never generates this — it is set by the user or
	// AI agent when a fixed value is desired.
	SourceTypeConstant SourceType = "constant"
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
