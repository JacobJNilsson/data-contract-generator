package transform

import "strings"

// SourceField represents a field from a source contract (CSV, JSON, etc.).
type SourceField struct {
	Name     string
	DataType string
}

// DestinationField represents a field from a destination contract (PostgreSQL).
type DestinationField struct {
	Name     string
	DataType string
	Nullable bool
}

// New creates a transformation contract skeleton with the given references
// and default execution plan. Field mappings are left empty for the caller
// (typically an AI agent) to populate.
func New(transformID, sourceRef, destRef string) *Contract {
	return &Contract{
		ContractType:   "transformation",
		TransformID:    transformID,
		SourceRef:      sourceRef,
		DestinationRef: destRef,
		FieldMappings:  []FieldMapping{},
		ExecutionPlan:  DefaultExecutionPlan(),
	}
}

// SuggestMappings attempts to match source fields to destination fields
// and returns a list of suggested mappings with confidence scores.
//
// Matching strategy (in priority order):
//  1. Exact name match (case-insensitive): confidence 1.0
//  2. Normalized name match (underscores/hyphens removed): confidence 0.8
//  3. Type-compatible unmatched fields: confidence 0.3
//
// Fields that don't match anything are omitted. The caller should review
// and adjust the suggestions before using them.
func SuggestMappings(source []SourceField, dest []DestinationField) []FieldMapping {
	var mappings []FieldMapping

	// Track which destination fields have been matched.
	matched := make(map[int]bool, len(dest))

	// Pass 1: exact name match (case-insensitive).
	for _, sf := range source {
		for j, df := range dest {
			if matched[j] {
				continue
			}
			if strings.EqualFold(sf.Name, df.Name) {
				m := FieldMapping{
					SourceField:      sf.Name,
					DestinationField: df.Name,
					Confidence:       1.0,
				}
				if needsCast(sf.DataType, df.DataType) {
					m.Transformation = &FieldTransformation{
						Type:       TypeCast,
						Parameters: map[string]any{"target_type": df.DataType},
					}
					m.Confidence = 0.9
				}
				mappings = append(mappings, m)
				matched[j] = true
				break
			}
		}
	}

	// Pass 2: normalized name match.
	for _, sf := range source {
		if isSourceMatched(mappings, sf.Name) {
			continue
		}
		normSrc := normalize(sf.Name)
		for j, df := range dest {
			if matched[j] {
				continue
			}
			if normSrc == normalize(df.Name) {
				m := FieldMapping{
					SourceField:      sf.Name,
					DestinationField: df.Name,
					Confidence:       0.8,
					Transformation: &FieldTransformation{
						Type:       TypeRename,
						Parameters: map[string]any{"from": sf.Name, "to": df.Name},
					},
				}
				if needsCast(sf.DataType, df.DataType) {
					m.Confidence = 0.7
				}
				mappings = append(mappings, m)
				matched[j] = true
				break
			}
		}
	}

	return mappings
}

// normalize strips underscores, hyphens, spaces and lowercases.
func normalize(s string) string {
	s = strings.ToLower(s)
	s = strings.NewReplacer("_", "", "-", "", " ", "").Replace(s)
	return s
}

// needsCast returns true if source and destination types are not directly
// compatible and a cast transformation is needed.
func needsCast(srcType, destType string) bool {
	srcNorm := strings.ToLower(srcType)
	destNorm := strings.ToLower(destType)

	if srcNorm == destNorm {
		return false
	}

	// Compatible type pairs that don't need an explicit cast.
	compatible := map[string]map[string]bool{
		"text":    {"varchar": true, "char": true, "text": true},
		"numeric": {"integer": true, "bigint": true, "smallint": true, "numeric": true, "real": true, "double": true},
		"date":    {"date": true, "timestamp": true, "timestamptz": true},
		"boolean": {"boolean": true},
	}

	if targets, ok := compatible[srcNorm]; ok {
		return !targets[destNorm]
	}

	return true
}

// isSourceMatched checks if a source field name already has a mapping.
func isSourceMatched(mappings []FieldMapping, name string) bool {
	for _, m := range mappings {
		if m.SourceField == name {
			return true
		}
	}
	return false
}
