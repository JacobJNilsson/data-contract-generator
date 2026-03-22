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

// SuggestMappings returns a mapping for every destination field. The
// destination schema is the target — every field needs to be accounted
// for. Matched destination fields get a suggested source_field with a
// confidence score. Unmatched destination fields get an empty
// source_field and confidence 0, signaling the user needs to map them
// manually (or accept a null/default value).
//
// Matching strategy (in priority order):
//  1. Exact name match (case-insensitive): confidence 1.0
//  2. Normalized name match (underscores/hyphens removed): confidence 0.8
func SuggestMappings(source []SourceField, dest []DestinationField) []FieldMapping {
	// One mapping per destination field. Unmatched fields default to
	// null for nullable fields, empty source_type for non-nullable.
	mappings := make([]FieldMapping, len(dest))
	for i, df := range dest {
		st := SourceTypeNull
		if !df.Nullable {
			st = SourceTypeUnmapped
		}
		mappings[i] = FieldMapping{
			DestinationField: df.Name,
			SourceType:       st,
			Confidence:       0,
		}
	}

	// Track which source fields have been matched.
	matched := make(map[int]bool, len(source))

	// Pass 1: exact name match (case-insensitive).
	for i, df := range dest {
		for j, sf := range source {
			if matched[j] {
				continue
			}
			if strings.EqualFold(df.Name, sf.Name) {
				mappings[i].SourceType = SourceTypeField
				mappings[i].SourceField = sf.Name
				mappings[i].Confidence = 1.0
				if needsCast(sf.DataType, df.DataType) {
					mappings[i].Transformation = &FieldTransformation{
						Type:       TypeCast,
						Parameters: map[string]any{"target_type": df.DataType},
					}
					mappings[i].Confidence = 0.9
				}
				matched[j] = true
				break
			}
		}
	}

	// Pass 2: normalized name match (only for unmatched destination fields).
	for i, df := range dest {
		if mappings[i].SourceType == SourceTypeField {
			continue
		}
		normDst := normalize(df.Name)
		for j, sf := range source {
			if matched[j] {
				continue
			}
			if normDst == normalize(sf.Name) {
				mappings[i].SourceType = SourceTypeField
				mappings[i].SourceField = sf.Name
				mappings[i].Confidence = 0.8
				params := map[string]any{"from": sf.Name, "to": df.Name}
				if needsCast(sf.DataType, df.DataType) {
					params["target_type"] = df.DataType
					mappings[i].Confidence = 0.7
				}
				mappings[i].Transformation = &FieldTransformation{
					Type:       TypeRename,
					Parameters: params,
				}
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

	// Groups of compatible types that don't need an explicit cast
	// in either direction.
	groups := []map[string]bool{
		{"text": true, "varchar": true, "char": true},
		{"integer": true, "bigint": true, "smallint": true, "numeric": true, "real": true, "double": true},
		{"date": true, "timestamp": true, "timestamptz": true},
	}

	for _, group := range groups {
		if group[srcNorm] && group[destNorm] {
			return false
		}
	}

	return true
}
