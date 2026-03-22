package verify

import (
	"encoding/json"
	"fmt"
)

// transformContract mirrors the transform.Contract type for validation.
type transformContract struct {
	TransformID     string         `json:"transformation_id"`
	SourceRefs      []string       `json:"source_refs"`
	DestinationRefs []string       `json:"destination_refs"`
	MappingGroups   []mappingGroup `json:"mapping_groups"`
	ExecutionPlan   executionPlan  `json:"execution_plan"`
}

type mappingGroup struct {
	DestinationRef string         `json:"destination_ref"`
	FieldMappings  []fieldMapping `json:"field_mappings"`
}

type fieldMapping struct {
	DestinationField string               `json:"destination_field"`
	SourceType       string               `json:"source_type"`
	SourceRef        string               `json:"source_ref"`
	SourceField      string               `json:"source_field"`
	SourceConstant   string               `json:"source_constant"`
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

// Valid source types for field mappings.
var validSourceTypes = map[string]bool{
	"field": true, "null": true, "constant": true, "unmapped": true, "transform": true,
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

	if len(tc.SourceRefs) == 0 {
		issues = append(issues, "missing source_refs")
	}
	if len(tc.DestinationRefs) == 0 {
		issues = append(issues, "missing destination_refs")
	}
	if len(tc.MappingGroups) == 0 {
		issues = append(issues, "no mapping_groups defined")
	}

	for gi, group := range tc.MappingGroups {
		groupPrefix := fmt.Sprintf("mapping_groups[%d]", gi)
		if group.DestinationRef == "" {
			issues = append(issues, groupPrefix+": missing destination_ref")
		}
		if len(group.FieldMappings) == 0 {
			issues = append(issues, groupPrefix+": no field_mappings defined")
		}

		for i, m := range group.FieldMappings {
			prefix := fmt.Sprintf("%s.field_mappings[%d]", groupPrefix, i)
			label := m.DestinationField
			if label == "" {
				label = fmt.Sprintf("#%d", i)
			}

			if m.DestinationField == "" {
				issues = append(issues, prefix+": missing destination_field")
			}

			if m.SourceType != "" && !validSourceTypes[m.SourceType] {
				issues = append(issues, fmt.Sprintf(
					"%s (%s): unknown source_type %q", prefix, label, m.SourceType))
			}

			if m.SourceType == "field" && m.SourceField == "" {
				issues = append(issues, fmt.Sprintf(
					"%s (%s): source_type is \"field\" but source_field is empty", prefix, label))
			}

			if m.Confidence < 0 || m.Confidence > 1 {
				issues = append(issues, fmt.Sprintf(
					"%s (%s): confidence %.2f out of range [0, 1]", prefix, label, m.Confidence))
			}

			if m.Transformation != nil {
				if m.Transformation.Type == "" {
					issues = append(issues, fmt.Sprintf(
						"%s (%s).transformation: missing type", prefix, label))
				} else if !validTransformTypes[m.Transformation.Type] {
					issues = append(issues, fmt.Sprintf(
						"%s (%s).transformation: unknown type %q", prefix, label, m.Transformation.Type))
				}
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

// destFieldInfo holds the name, nullability, and constraints of a
// destination field, extracted from the destination contract.
type destFieldInfo struct {
	nullable    bool
	constraints []string
}

// TransformationWithContext validates a transformation contract against
// its source and destination contracts. In addition to structural
// validation, it checks:
//   - source_field references exist in the correct source contract (matched by source_ref)
//   - destination_field references exist in the correct destination contract
//   - non-nullable destination fields have a source mapping (not null, not unmapped)
//   - primary key destination fields have a source mapping
//
// The sources and destinations maps are keyed by ref name. For backward
// compatibility, if sources/destinations are nil, cross-reference checks
// are skipped.
func TransformationWithContext(transformData []byte, sources, destinations map[string]json.RawMessage) Result {
	base := Verify(transformData)
	if !base.Valid {
		return base
	}

	var tc transformContract
	_ = json.Unmarshal(transformData, &tc)

	// Build source field maps keyed by ref.
	sourceFieldMaps := make(map[string]map[string]bool)
	for ref, data := range sources {
		sourceFieldMaps[ref] = extractFieldNames(data)
	}

	// Build destination field maps keyed by ref.
	destFieldMaps := make(map[string]map[string]destFieldInfo)
	for ref, data := range destinations {
		destFieldMaps[ref] = extractDestFieldInfo(data)
	}

	var issues []string

	for _, group := range tc.MappingGroups {
		destFields := destFieldMaps[group.DestinationRef]

		for _, m := range group.FieldMappings {
			label := m.DestinationField

			// Cross-reference: source field exists in the correct source?
			if m.SourceType == "field" && m.SourceField != "" && m.SourceRef != "" {
				srcFields := sourceFieldMaps[m.SourceRef]
				if srcFields != nil && !srcFields[m.SourceField] {
					issues = append(issues, fmt.Sprintf(
						"'%s': source field '%s' not found in source '%s'",
						label, m.SourceField, m.SourceRef))
				}
			}

			// Cross-reference: destination field exists and check constraints
			if m.DestinationField != "" && destFields != nil {
				info, exists := destFields[m.DestinationField]
				if !exists {
					issues = append(issues, fmt.Sprintf(
						"'%s': destination field not found in destination '%s'",
						label, group.DestinationRef))
					continue
				}

				if m.SourceType == "null" && !info.nullable {
					constraintList := formatConstraints(info.constraints)
					issues = append(issues, fmt.Sprintf(
						"'%s' is NOT NULL%s but mapped to NULL",
						label, constraintList))
				}

				if (m.SourceType == "unmapped" || m.SourceType == "") && !info.nullable {
					constraintList := formatConstraints(info.constraints)
					issues = append(issues, fmt.Sprintf(
						"'%s' is NOT NULL%s and has no source mapping",
						label, constraintList))
				}
			}
		}
	}

	return Result{
		Valid:        len(issues) == 0,
		ContractType: "transformation",
		Issues:       issues,
	}
}

// formatConstraints returns a human-readable suffix like " (PRIMARY KEY)"
// or "" if no noteworthy constraints exist beyond NOT NULL.
func formatConstraints(constraints []string) string {
	for _, c := range constraints {
		switch c {
		case "primary_key":
			return " (PRIMARY KEY)"
		case "unique":
			return " (UNIQUE)"
		case "foreign_key":
			return " (FOREIGN KEY)"
		}
	}
	return ""
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

// extractDestFieldInfo extracts field names with nullability and constraints
// from a destination contract (DataContract with schemas).
func extractDestFieldInfo(data []byte) map[string]destFieldInfo {
	var raw struct {
		Schemas []struct {
			Fields []struct {
				Name        string `json:"name"`
				Nullable    bool   `json:"nullable"`
				Constraints []struct {
					Type string `json:"type"`
				} `json:"constraints"`
			} `json:"fields"`
		} `json:"schemas"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}
	result := make(map[string]destFieldInfo)
	for _, s := range raw.Schemas {
		for _, f := range s.Fields {
			var constraints []string
			for _, c := range f.Constraints {
				constraints = append(constraints, c.Type)
			}
			result[f.Name] = destFieldInfo{
				nullable:    f.Nullable,
				constraints: constraints,
			}
		}
	}
	return result
}
