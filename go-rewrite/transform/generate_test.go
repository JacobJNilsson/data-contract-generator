package transform

import (
	"encoding/json"
	"testing"
)

// --- New tests --------------------------------------------------------------

func TestNew(t *testing.T) {
	c := New("t1", "source-csv", "dest-pg")

	if c.ContractType != "transformation" {
		t.Errorf("contract_type = %q, want transformation", c.ContractType)
	}
	if c.TransformID != "t1" {
		t.Errorf("transformation_id = %q, want t1", c.TransformID)
	}
	if c.SourceRef != "source-csv" {
		t.Errorf("source_ref = %q, want source-csv", c.SourceRef)
	}
	if c.DestinationRef != "dest-pg" {
		t.Errorf("destination_ref = %q, want dest-pg", c.DestinationRef)
	}
	if len(c.FieldMappings) != 0 {
		t.Errorf("field_mappings should be empty, got %d", len(c.FieldMappings))
	}
	if c.ExecutionPlan.BatchSize != 100 {
		t.Errorf("batch_size = %d, want 100", c.ExecutionPlan.BatchSize)
	}
	if c.ExecutionPlan.ErrorThreshold != 0.1 {
		t.Errorf("error_threshold = %f, want 0.1", c.ExecutionPlan.ErrorThreshold)
	}
	if !c.ExecutionPlan.ValidationEnabled {
		t.Error("validation_enabled should be true")
	}
}

func TestNewSerializesToJSON(t *testing.T) {
	c := New("t1", "src", "dst")
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	if raw["contract_type"] != "transformation" {
		t.Errorf("JSON contract_type = %v", raw["contract_type"])
	}
}

// --- SuggestMappings tests --------------------------------------------------

func TestSuggestMappingsExactMatch(t *testing.T) {
	src := []SourceField{
		{Name: "name", DataType: "text"},
		{Name: "age", DataType: "numeric"},
	}
	dst := []DestinationField{
		{Name: "name", DataType: "text"},
		{Name: "age", DataType: "integer"},
	}

	mappings := SuggestMappings(src, dst)

	if len(mappings) != 2 {
		t.Fatalf("mappings = %d, want 2", len(mappings))
	}

	// name -> name (exact match, same type)
	m0 := mappings[0]
	if m0.SourceField != "name" || m0.DestinationField != "name" {
		t.Errorf("mapping[0] = %s -> %s, want name -> name", m0.SourceField, m0.DestinationField)
	}
	if m0.Confidence != 1.0 {
		t.Errorf("mapping[0] confidence = %f, want 1.0", m0.Confidence)
	}
	if m0.Transformation != nil {
		t.Error("mapping[0] should not have transformation (compatible types)")
	}

	// age -> age (exact match, needs cast from numeric to integer)
	m1 := mappings[1]
	if m1.SourceField != "age" || m1.DestinationField != "age" {
		t.Errorf("mapping[1] = %s -> %s, want age -> age", m1.SourceField, m1.DestinationField)
	}
	// numeric -> integer is compatible, no cast needed
	if m1.Transformation != nil {
		t.Error("mapping[1] should not have transformation (numeric is compatible with integer)")
	}
}

func TestSuggestMappingsCaseInsensitive(t *testing.T) {
	src := []SourceField{{Name: "Name", DataType: "text"}}
	dst := []DestinationField{{Name: "name", DataType: "text"}}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].Confidence != 1.0 {
		t.Errorf("confidence = %f, want 1.0 (case-insensitive exact match)", mappings[0].Confidence)
	}
}

func TestSuggestMappingsNormalizedMatch(t *testing.T) {
	src := []SourceField{{Name: "first_name", DataType: "text"}}
	dst := []DestinationField{{Name: "firstname", DataType: "varchar"}}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].Confidence != 0.8 {
		t.Errorf("confidence = %f, want 0.8 (normalized match)", mappings[0].Confidence)
	}
	if mappings[0].Transformation == nil {
		t.Fatal("expected rename transformation")
	}
	if mappings[0].Transformation.Type != TypeRename {
		t.Errorf("transform type = %q, want rename", mappings[0].Transformation.Type)
	}
}

func TestSuggestMappingsNormalizedWithCast(t *testing.T) {
	src := []SourceField{{Name: "created-at", DataType: "text"}}
	dst := []DestinationField{{Name: "created_at", DataType: "timestamptz"}}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	// Normalized match + needs cast: confidence drops to 0.7
	if mappings[0].Confidence != 0.7 {
		t.Errorf("confidence = %f, want 0.7 (normalized + cast needed)", mappings[0].Confidence)
	}
}

func TestSuggestMappingsSkipsAlreadyMatched(t *testing.T) {
	// "name" matches exactly in pass 1. Pass 2 should not try to match it again.
	src := []SourceField{
		{Name: "name", DataType: "text"},
		{Name: "user_id", DataType: "numeric"},
	}
	dst := []DestinationField{
		{Name: "name", DataType: "text"},
		{Name: "userid", DataType: "integer"}, // normalized match for user_id
	}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 2 {
		t.Fatalf("mappings = %d, want 2", len(mappings))
	}

	// name matched exactly (pass 1), user_id matched normalized (pass 2)
	if mappings[0].Confidence != 1.0 {
		t.Errorf("mapping[0] confidence = %f, want 1.0", mappings[0].Confidence)
	}
	if mappings[1].Confidence != 0.8 {
		t.Errorf("mapping[1] confidence = %f, want 0.8", mappings[1].Confidence)
	}
}

func TestSuggestMappingsNoMatch(t *testing.T) {
	src := []SourceField{{Name: "foo", DataType: "text"}}
	dst := []DestinationField{{Name: "bar", DataType: "text"}}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 0 {
		t.Errorf("mappings = %d, want 0 (no match)", len(mappings))
	}
}

func TestSuggestMappingsEmpty(t *testing.T) {
	mappings := SuggestMappings(nil, nil)
	if len(mappings) != 0 {
		t.Errorf("mappings = %d, want 0", len(mappings))
	}
}

func TestSuggestMappingsExactTakesPriority(t *testing.T) {
	// If both exact and normalized would match, exact wins.
	src := []SourceField{{Name: "email", DataType: "text"}}
	dst := []DestinationField{
		{Name: "email", DataType: "varchar"},
	}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	// Exact match should not have rename transformation
	if mappings[0].Transformation != nil && mappings[0].Transformation.Type == TypeRename {
		t.Error("exact match should not produce a rename transformation")
	}
}

func TestSuggestMappingsNeedsCast(t *testing.T) {
	src := []SourceField{{Name: "active", DataType: "text"}}
	dst := []DestinationField{{Name: "active", DataType: "boolean"}}

	mappings := SuggestMappings(src, dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].Transformation == nil {
		t.Fatal("expected cast transformation (text -> boolean)")
	}
	if mappings[0].Transformation.Type != TypeCast {
		t.Errorf("transform type = %q, want cast", mappings[0].Transformation.Type)
	}
	if mappings[0].Confidence != 0.9 {
		t.Errorf("confidence = %f, want 0.9 (exact match but needs cast)", mappings[0].Confidence)
	}
}

// --- needsCast tests --------------------------------------------------------

func TestNeedsCast(t *testing.T) {
	tests := []struct {
		src, dst string
		want     bool
	}{
		{"text", "text", false},
		{"text", "varchar", false},
		{"text", "char", false},
		{"numeric", "integer", false},
		{"numeric", "bigint", false},
		{"numeric", "real", false},
		{"date", "timestamp", false},
		{"date", "timestamptz", false},
		{"boolean", "boolean", false},
		{"text", "boolean", true},
		{"text", "integer", true},
		{"numeric", "text", true},
		{"text", "timestamptz", true},
		{"unknown", "integer", true},
	}

	for _, tt := range tests {
		got := needsCast(tt.src, tt.dst)
		if got != tt.want {
			t.Errorf("needsCast(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
		}
	}
}

// --- normalize tests --------------------------------------------------------

func TestNormalize(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"first_name", "firstname"},
		{"first-name", "firstname"},
		{"First Name", "firstname"},
		{"firstName", "firstname"},
		{"ID", "id"},
	}

	for _, tt := range tests {
		got := normalize(tt.input)
		if got != tt.want {
			t.Errorf("normalize(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- DefaultExecutionPlan tests ---------------------------------------------

func TestDefaultExecutionPlan(t *testing.T) {
	p := DefaultExecutionPlan()
	if p.BatchSize != 100 {
		t.Errorf("batch_size = %d, want 100", p.BatchSize)
	}
	if p.ErrorThreshold != 0.1 {
		t.Errorf("error_threshold = %f, want 0.1", p.ErrorThreshold)
	}
	if !p.ValidationEnabled {
		t.Error("validation_enabled should be true")
	}
}
