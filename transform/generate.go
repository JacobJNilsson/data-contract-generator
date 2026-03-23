package transform

import (
	"slices"
	"strings"
)

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
// from one or more named source field sets. destRef identifies which
// destination schema these mappings are for; when a source has a ref
// matching destRef, its fields are preferred over identically-named
// fields from other sources.
//
// Matching strategy (in priority order):
//  1. Exact name match from the preferred source (ref == destRef): confidence 1.0
//  2. Exact name match from any other source: confidence 1.0
//  3. Normalized name match from preferred source: confidence 0.8
//  4. Normalized name match from any other source: confidence 0.8
func SuggestMappings(sources []NamedSourceFields, dest []DestinationField, destRef string) []FieldMapping {
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

	// Score each source by how many destination field names it contains.
	// The source with the most overlap is most likely the "right" source
	// for this destination -- its fields are checked first so they win
	// ties when multiple sources share a field name.
	//
	// When destRef matches a source ref exactly, that source gets maximum
	// score (len(dest)+1) to guarantee it comes first.
	destFieldSet := make(map[string]bool, len(dest))
	for _, df := range dest {
		destFieldSet[strings.ToLower(df.Name)] = true
	}

	type scoredSource struct {
		src   NamedSourceFields
		score int
	}
	scored := make([]scoredSource, len(sources))
	for i, src := range sources {
		score := 0
		if src.Ref == destRef {
			score = len(dest) + 1 // exact ref match: highest priority
		} else {
			for _, f := range src.Fields {
				if destFieldSet[strings.ToLower(f.Name)] {
					score++
				}
			}
		}
		scored[i] = scoredSource{src: src, score: score}
	}
	slices.SortFunc(scored, func(a, b scoredSource) int {
		return b.score - a.score // descending
	})

	type qualifiedField struct {
		ref   string
		field SourceField
	}
	allFields := make([]qualifiedField, 0)
	for _, ss := range scored {
		for _, f := range ss.src.Fields {
			allFields = append(allFields, qualifiedField{ref: ss.src.Ref, field: f})
		}
	}

	// Pass 1: exact name match (case-insensitive).
	for i, df := range dest {
		for _, qf := range allFields {
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
