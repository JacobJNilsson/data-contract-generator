package verify

import (
	"encoding/json"
	"fmt"
)

// transformContract mirrors the transform.Contract type for validation.
type transformContract struct {
	TransformID    string         `json:"transformation_id"`
	SourceRef      string         `json:"source_ref"`
	DestinationRef string         `json:"destination_ref"`
	FieldMappings  []fieldMapping `json:"field_mappings"`
	ExecutionPlan  executionPlan  `json:"execution_plan"`
}

type fieldMapping struct {
	SourceField      string               `json:"source_field"`
	DestinationField string               `json:"destination_field"`
	Transformation   *fieldTransformation `json:"transformation"`
	Confidence       float64              `json:"confidence"`
}

type fieldTransformation struct {
	Type string `json:"type"`
}

type executionPlan struct {
	BatchSize         int     `json:"batch_size"`
	ErrorThreshold    float64 `json:"error_threshold"`
	ValidationEnabled bool    `json:"validation_enabled"`
}

// Valid transformation types.
var validTransformTypes = map[string]bool{
	"rename": true, "cast": true, "format": true, "default": true,
}

// verifyTransformation validates a transformation contract structurally.
func verifyTransformation(data []byte) []string {
	var tc transformContract
	if err := json.Unmarshal(data, &tc); err != nil {
		return []string{fmt.Sprintf("failed to parse transformation contract: %s", err)}
	}

	var issues []string

	if tc.SourceRef == "" {
		issues = append(issues, "missing source_ref")
	}
	if tc.DestinationRef == "" {
		issues = append(issues, "missing destination_ref")
	}
	if len(tc.FieldMappings) == 0 {
		issues = append(issues, "no field_mappings defined")
	}

	for i, m := range tc.FieldMappings {
		prefix := fmt.Sprintf("field_mappings[%d]", i)

		if m.SourceField == "" {
			issues = append(issues, prefix+": missing source_field")
		}
		if m.DestinationField == "" {
			issues = append(issues, prefix+": missing destination_field")
		}
		if m.Confidence < 0 || m.Confidence > 1 {
			issues = append(issues, fmt.Sprintf(
				"%s: confidence %.2f out of range [0, 1]", prefix, m.Confidence))
		}
		if m.Transformation != nil {
			if m.Transformation.Type == "" {
				issues = append(issues, prefix+".transformation: missing type")
			} else if !validTransformTypes[m.Transformation.Type] {
				issues = append(issues, fmt.Sprintf(
					"%s.transformation: unknown type %q", prefix, m.Transformation.Type))
			}
		}
	}

	if tc.ExecutionPlan.BatchSize < 0 {
		issues = append(issues, fmt.Sprintf(
			"execution_plan: batch_size is negative: %d", tc.ExecutionPlan.BatchSize))
	}
	if tc.ExecutionPlan.ErrorThreshold < 0 || tc.ExecutionPlan.ErrorThreshold > 1 {
		issues = append(issues, fmt.Sprintf(
			"execution_plan: error_threshold %.2f out of range [0, 1]",
			tc.ExecutionPlan.ErrorThreshold))
	}

	return issues
}

// TransformationWithContext validates a transformation contract against
// its source and destination contracts. In addition to structural validation,
// it checks that every mapped source_field exists in the source contract and
// every mapped destination_field exists in the destination contract.
func TransformationWithContext(transformData, sourceData, destData []byte) Result {
	// First, structurally validate the transformation.
	base := Verify(transformData)
	if !base.Valid {
		return base
	}

	var tc transformContract
	_ = json.Unmarshal(transformData, &tc) // already validated

	// Extract source field names.
	sourceFields := extractFieldNames(sourceData)
	destFields := extractDestFieldNames(destData)

	var issues []string

	for i, m := range tc.FieldMappings {
		prefix := fmt.Sprintf("field_mappings[%d]", i)

		if sourceFields != nil && m.SourceField != "" {
			if !sourceFields[m.SourceField] {
				issues = append(issues, fmt.Sprintf(
					"%s: source_field %q not found in source contract", prefix, m.SourceField))
			}
		}

		if destFields != nil && m.DestinationField != "" {
			if !destFields[m.DestinationField] {
				issues = append(issues, fmt.Sprintf(
					"%s: destination_field %q not found in destination contract", prefix, m.DestinationField))
			}
		}
	}

	return Result{
		Valid:        len(issues) == 0,
		ContractType: "transformation",
		Issues:       issues,
	}
}

// extractFieldNames extracts field names from a source contract (CSV, JSON).
func extractFieldNames(data []byte) map[string]bool {
	var raw struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}
	names := make(map[string]bool, len(raw.Fields))
	for _, f := range raw.Fields {
		names[f.Name] = true
	}
	return names
}

// extractDestFieldNames extracts field names from a destination contract
// (DataContract with schemas). Collects fields across all schemas.
func extractDestFieldNames(data []byte) map[string]bool {
	var raw struct {
		Schemas []struct {
			Fields []struct {
				Name string `json:"name"`
			} `json:"fields"`
		} `json:"schemas"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}
	names := make(map[string]bool)
	for _, s := range raw.Schemas {
		for _, f := range s.Fields {
			names[f.Name] = true
		}
	}
	return names
}
