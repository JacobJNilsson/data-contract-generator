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

// FieldMapping maps a single destination field to a source field,
// with an optional transformation.
type FieldMapping struct {
	SourceField      string               `json:"source_field"`
	DestinationField string               `json:"destination_field"`
	Transformation   *FieldTransformation `json:"transformation,omitempty"`
	Confidence       float64              `json:"confidence"` // 0.0 to 1.0
}

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
