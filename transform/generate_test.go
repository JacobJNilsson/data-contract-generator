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
	if len(c.SourceRefs) != 1 || c.SourceRefs[0] != "source-csv" {
		t.Errorf("source_refs = %v, want [source-csv]", c.SourceRefs)
	}
	if len(c.DestinationRefs) != 1 || c.DestinationRefs[0] != "dest-pg" {
		t.Errorf("destination_refs = %v, want [dest-pg]", c.DestinationRefs)
	}
	if len(c.MappingGroups) != 1 {
		t.Fatalf("mapping_groups count = %d, want 1", len(c.MappingGroups))
	}
	if c.MappingGroups[0].DestinationRef != "dest-pg" {
		t.Errorf("mapping_groups[0].destination_ref = %q, want dest-pg", c.MappingGroups[0].DestinationRef)
	}
	if len(c.MappingGroups[0].FieldMappings) != 0 {
		t.Errorf("field_mappings should be empty, got %d", len(c.MappingGroups[0].FieldMappings))
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
	if raw["source_refs"] == nil {
		t.Error("JSON should have source_refs")
	}
	if raw["destination_refs"] == nil {
		t.Error("JSON should have destination_refs")
	}
	if raw["mapping_groups"] == nil {
		t.Error("JSON should have mapping_groups")
	}
}

// --- SuggestMappings tests (single source) ----------------------------------

func singleSource(fields []SourceField) []NamedSourceFields {
	return []NamedSourceFields{{Ref: "source", Fields: fields}}
}

func TestSuggestMappingsExactMatch(t *testing.T) {
	src := []SourceField{
		{Name: "name", DataType: "text"},
		{Name: "age", DataType: "numeric"},
	}
	dst := []DestinationField{
		{Name: "name", DataType: "text"},
		{Name: "age", DataType: "integer"},
	}

	mappings := SuggestMappings(singleSource(src), dst)

	if len(mappings) != 2 {
		t.Fatalf("mappings = %d, want 2", len(mappings))
	}

	m0 := mappings[0]
	if m0.SourceType != SourceTypeField {
		t.Errorf("mapping[0] source_type = %q, want field", m0.SourceType)
	}
	if m0.SourceRef != "source" {
		t.Errorf("mapping[0] source_ref = %q, want source", m0.SourceRef)
	}
	if m0.SourceField != "name" || m0.DestinationField != "name" {
		t.Errorf("mapping[0] = %s -> %s, want name -> name", m0.SourceField, m0.DestinationField)
	}
	if m0.Confidence != 1.0 {
		t.Errorf("mapping[0] confidence = %f, want 1.0", m0.Confidence)
	}
	if m0.Transformation != nil {
		t.Error("mapping[0] should not have transformation (compatible types)")
	}

	m1 := mappings[1]
	if m1.SourceType != SourceTypeField {
		t.Errorf("mapping[1] source_type = %q, want field", m1.SourceType)
	}
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

	mappings := SuggestMappings(singleSource(src), dst)
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

	mappings := SuggestMappings(singleSource(src), dst)
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

	mappings := SuggestMappings(singleSource(src), dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].Confidence != 0.7 {
		t.Errorf("confidence = %f, want 0.7 (normalized + cast needed)", mappings[0].Confidence)
	}
}

func TestSuggestMappingsSkipsAlreadyMatched(t *testing.T) {
	src := []SourceField{
		{Name: "name", DataType: "text"},
		{Name: "user_id", DataType: "numeric"},
	}
	dst := []DestinationField{
		{Name: "name", DataType: "text"},
		{Name: "userid", DataType: "integer"},
	}

	mappings := SuggestMappings(singleSource(src), dst)
	if len(mappings) != 2 {
		t.Fatalf("mappings = %d, want 2", len(mappings))
	}

	if mappings[0].DestinationField != "name" || mappings[0].SourceField != "name" {
		t.Errorf("mapping[0] = %s → %s, want name → name", mappings[0].SourceField, mappings[0].DestinationField)
	}
	if mappings[0].Confidence != 1.0 {
		t.Errorf("mapping[0] confidence = %f, want 1.0", mappings[0].Confidence)
	}

	if mappings[1].DestinationField != "userid" || mappings[1].SourceField != "user_id" {
		t.Errorf("mapping[1] = %s → %s, want user_id → userid", mappings[1].SourceField, mappings[1].DestinationField)
	}
	if mappings[1].Confidence != 0.8 {
		t.Errorf("mapping[1] confidence = %f, want 0.8", mappings[1].Confidence)
	}
}

func TestSuggestMappingsNoMatch_Nullable(t *testing.T) {
	src := []SourceField{{Name: "foo", DataType: "text"}}
	dst := []DestinationField{{Name: "bar", DataType: "text", Nullable: true}}

	mappings := SuggestMappings(singleSource(src), dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].SourceType != SourceTypeNull {
		t.Errorf("source_type = %q, want null (nullable + unmatched)", mappings[0].SourceType)
	}
	if mappings[0].SourceField != "" {
		t.Errorf("source_field = %q, want empty", mappings[0].SourceField)
	}
}

func TestSuggestMappingsNoMatch_NonNullable(t *testing.T) {
	src := []SourceField{{Name: "foo", DataType: "text"}}
	dst := []DestinationField{{Name: "bar", DataType: "text", Nullable: false}}

	mappings := SuggestMappings(singleSource(src), dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].SourceType != SourceTypeUnmapped {
		t.Errorf("source_type = %q, want unmapped (non-nullable + unmatched = user must decide)", mappings[0].SourceType)
	}
}

func TestSuggestMappingsEmpty(t *testing.T) {
	mappings := SuggestMappings(nil, nil)
	if len(mappings) != 0 {
		t.Errorf("mappings = %d, want 0", len(mappings))
	}
}

func TestSuggestMappingsExactTakesPriority(t *testing.T) {
	src := []SourceField{{Name: "email", DataType: "text"}}
	dst := []DestinationField{
		{Name: "email", DataType: "varchar"},
	}

	mappings := SuggestMappings(singleSource(src), dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].Transformation != nil && mappings[0].Transformation.Type == TypeRename {
		t.Error("exact match should not produce a rename transformation")
	}
}

func TestSuggestMappingsNeedsCast(t *testing.T) {
	src := []SourceField{{Name: "active", DataType: "text"}}
	dst := []DestinationField{{Name: "active", DataType: "boolean"}}

	mappings := SuggestMappings(singleSource(src), dst)
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

// --- Multi-source SuggestMappings tests ------------------------------------

func TestSuggestMappingsMultiSource(t *testing.T) {
	sources := []NamedSourceFields{
		{Ref: "orders.csv", Fields: []SourceField{
			{Name: "order_id", DataType: "integer"},
			{Name: "customer_id", DataType: "integer"},
			{Name: "amount", DataType: "numeric"},
		}},
		{Ref: "customers", Fields: []SourceField{
			{Name: "id", DataType: "integer"},
			{Name: "name", DataType: "text"},
			{Name: "email", DataType: "varchar"},
		}},
	}
	dst := []DestinationField{
		{Name: "order_id", DataType: "integer"},
		{Name: "name", DataType: "varchar"},
		{Name: "amount", DataType: "numeric"},
		{Name: "email", DataType: "varchar"},
	}

	mappings := SuggestMappings(sources, dst)
	if len(mappings) != 4 {
		t.Fatalf("mappings = %d, want 4", len(mappings))
	}

	// order_id matches orders.csv
	if mappings[0].SourceRef != "orders.csv" || mappings[0].SourceField != "order_id" {
		t.Errorf("mapping[0] = %s:%s, want orders.csv:order_id", mappings[0].SourceRef, mappings[0].SourceField)
	}

	// name matches customers
	if mappings[1].SourceRef != "customers" || mappings[1].SourceField != "name" {
		t.Errorf("mapping[1] = %s:%s, want customers:name", mappings[1].SourceRef, mappings[1].SourceField)
	}

	// amount matches orders.csv
	if mappings[2].SourceRef != "orders.csv" || mappings[2].SourceField != "amount" {
		t.Errorf("mapping[2] = %s:%s, want orders.csv:amount", mappings[2].SourceRef, mappings[2].SourceField)
	}

	// email matches customers
	if mappings[3].SourceRef != "customers" || mappings[3].SourceField != "email" {
		t.Errorf("mapping[3] = %s:%s, want customers:email", mappings[3].SourceRef, mappings[3].SourceField)
	}
}

func TestSuggestMappingsMultiSourceFirstWins(t *testing.T) {
	// Both sources have "id". First source in the list wins.
	sources := []NamedSourceFields{
		{Ref: "orders", Fields: []SourceField{{Name: "id", DataType: "integer"}}},
		{Ref: "customers", Fields: []SourceField{{Name: "id", DataType: "integer"}}},
	}
	dst := []DestinationField{{Name: "id", DataType: "integer"}}

	mappings := SuggestMappings(sources, dst)
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	if mappings[0].SourceRef != "orders" {
		t.Errorf("source_ref = %q, want orders (first source wins)", mappings[0].SourceRef)
	}
}

func TestSuggestMappingsMultiSourceCrossSourceNormalized(t *testing.T) {
	// Exact match from source A, normalized match from source B for different fields.
	sources := []NamedSourceFields{
		{Ref: "file.csv", Fields: []SourceField{{Name: "order_id", DataType: "integer"}}},
		{Ref: "db", Fields: []SourceField{{Name: "customer_name", DataType: "text"}}},
	}
	dst := []DestinationField{
		{Name: "order_id", DataType: "integer"},
		{Name: "customername", DataType: "varchar"},
	}

	mappings := SuggestMappings(sources, dst)
	if len(mappings) != 2 {
		t.Fatalf("mappings = %d, want 2", len(mappings))
	}

	// order_id: exact from file.csv
	if mappings[0].SourceRef != "file.csv" || mappings[0].Confidence != 1.0 {
		t.Errorf("mapping[0] = %s (%.1f), want file.csv (1.0)", mappings[0].SourceRef, mappings[0].Confidence)
	}

	// customername: normalized from db
	if mappings[1].SourceRef != "db" || mappings[1].Confidence != 0.8 {
		t.Errorf("mapping[1] = %s (%.1f), want db (0.8)", mappings[1].SourceRef, mappings[1].Confidence)
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
		{"varchar", "text", false},
		{"char", "varchar", false},
		{"numeric", "integer", false},
		{"numeric", "bigint", false},
		{"numeric", "real", false},
		{"integer", "numeric", false},
		{"bigint", "integer", false},
		{"date", "timestamp", false},
		{"date", "timestamptz", false},
		{"timestamp", "date", false},
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
