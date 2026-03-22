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

// NamedSourceFields pairs a source reference label with its fields.
type NamedSourceFields struct {
	Ref    string
	Fields []SourceField
}

// New creates a transformation contract skeleton with a single source
// and destination. Field mappings are left empty for the caller
// (typically an AI agent) to populate.
func New(transformID, sourceRef, destRef string) *Contract {
	return &Contract{
		ContractType:    "transformation",
		TransformID:     transformID,
		SourceRefs:      []string{sourceRef},
		DestinationRefs: []string{destRef},
		MappingGroups: []MappingGroup{{
			DestinationRef: destRef,
			FieldMappings:  []FieldMapping{},
		}},
		ExecutionPlan: DefaultExecutionPlan(),
	}
}

// SuggestMappings returns a mapping for every destination field, drawing
// from one or more named source field sets. Each matched mapping includes
// the SourceRef identifying which source the field comes from.
//
// Matching strategy (in priority order):
//  1. Exact name match (case-insensitive): confidence 1.0
//  2. Normalized name match (underscores/hyphens removed): confidence 0.8
//
// When multiple sources have a field with the same name, the first source
// in the slice wins.
func SuggestMappings(sources []NamedSourceFields, dest []DestinationField) []FieldMapping {
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

	// Build a flat list of source fields with their ref, for matching.
	type qualifiedField struct {
		ref   string
		field SourceField
		used  bool
	}
	var allFields []*qualifiedField
	for _, src := range sources {
		for _, f := range src.Fields {
			allFields = append(allFields, &qualifiedField{ref: src.Ref, field: f})
		}
	}

	// Pass 1: exact name match (case-insensitive).
	for i, df := range dest {
		for _, qf := range allFields {
			if qf.used {
				continue
			}
			if strings.EqualFold(df.Name, qf.field.Name) {
				mappings[i].SourceType = SourceTypeField
				mappings[i].SourceRef = qf.ref
				mappings[i].SourceField = qf.field.Name
				mappings[i].Confidence = 1.0
				if needsCast(qf.field.DataType, df.DataType) {
					mappings[i].Transformation = &FieldTransformation{
						Type:       TypeCast,
						Parameters: map[string]any{"target_type": df.DataType},
					}
					mappings[i].Confidence = 0.9
				}
				qf.used = true
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
		for _, qf := range allFields {
			if qf.used {
				continue
			}
			if normDst == normalize(qf.field.Name) {
				mappings[i].SourceType = SourceTypeField
				mappings[i].SourceRef = qf.ref
				mappings[i].SourceField = qf.field.Name
				mappings[i].Confidence = 0.8
				params := map[string]any{"from": qf.field.Name, "to": df.Name}
				if needsCast(qf.field.DataType, df.DataType) {
					params["target_type"] = df.DataType
					mappings[i].Confidence = 0.7
				}
				mappings[i].Transformation = &FieldTransformation{
					Type:       TypeRename,
					Parameters: params,
				}
				qf.used = true
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
