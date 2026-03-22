package verify

import (
	"testing"
)

// --- Structural validation tests -------------------------------------------

func TestVerifyTransformation_Valid(t *testing.T) {
	r := Verify([]byte(validTransformContract()))
	if !r.Valid {
		t.Errorf("expected valid, got issues: %v", r.Issues)
	}
	if r.ContractType != "transformation" {
		t.Errorf("contract_type = %q, want transformation", r.ContractType)
	}
}

func TestVerifyTransformation_MissingSourceRef(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing source_ref")
}

func TestVerifyTransformation_MissingDestRef(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing destination_ref")
}

func TestVerifyTransformation_NoMappings(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "no field_mappings")
}

func TestVerifyTransformation_MissingSourceField(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "", "destination_field": "b", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing source_field")
}

func TestVerifyTransformation_MissingDestField(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing destination_field")
}

func TestVerifyTransformation_ConfidenceOutOfRange(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.5}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "confidence")
	assertInvalid(t, r, "out of range")
}

func TestVerifyTransformation_NegativeConfidence(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": -0.1}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "confidence")
}

func TestVerifyTransformation_UnknownTransformType(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0, "transformation": {"type": "magic"}}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown type")
}

func TestVerifyTransformation_EmptyTransformType(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0, "transformation": {"type": ""}}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing type")
}

func TestVerifyTransformation_ValidTransformTypes(t *testing.T) {
	for _, tt := range []string{"rename", "cast", "format", "default"} {
		data := `{
			"contract_type": "transformation",
			"source_ref": "src",
			"destination_ref": "dest",
			"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0, "transformation": {"type": "` + tt + `"}}],
			"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
		}`
		r := Verify([]byte(data))
		if !r.Valid {
			t.Errorf("transform type %q should be valid, got: %v", tt, r.Issues)
		}
	}
}

func TestVerifyTransformation_NegativeBatchSize(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0}],
		"execution_plan": {"batch_size": -1, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "batch_size is negative")
}

func TestVerifyTransformation_ErrorThresholdOutOfRange(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "a", "destination_field": "b", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 2.0, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "error_threshold")
}

func TestVerifyTransformation_BadJSON(t *testing.T) {
	data := `{"contract_type": "transformation", "field_mappings": "not an array"}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "failed to parse")
}

// --- Cross-reference validation tests --------------------------------------

func TestVerifyWithContext_Valid(t *testing.T) {
	r := TransformationWithContext(
		[]byte(validTransformContract()),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	if !r.Valid {
		t.Errorf("expected valid, got issues: %v", r.Issues)
	}
}

func TestVerifyWithContext_SourceFieldNotFound(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "nonexistent", "destination_field": "id", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "source_field \"nonexistent\" not found in source")
}

func TestVerifyWithContext_DestFieldNotFound(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"source_field": "name", "destination_field": "ghost", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "destination_field \"ghost\" not found in destination")
}

func TestVerifyWithContext_StructuralFailure(t *testing.T) {
	// If the transformation itself is structurally invalid, cross-reference
	// is skipped and the structural issues are returned.
	r := TransformationWithContext(
		[]byte(`{"contract_type": "transformation"}`),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	if r.Valid {
		t.Error("expected invalid for missing required fields")
	}
}

func TestVerifyWithContext_BadSourceJSON(t *testing.T) {
	// If source contract can't be parsed, cross-reference is skipped
	// (sourceFields is nil, no cross-reference issues).
	r := TransformationWithContext(
		[]byte(validTransformContract()),
		[]byte(`not json`),
		[]byte(validDestForTransform()),
	)
	if !r.Valid {
		t.Errorf("expected valid (bad source skips cross-ref), got: %v", r.Issues)
	}
}

func TestVerifyWithContext_BadDestJSON(t *testing.T) {
	r := TransformationWithContext(
		[]byte(validTransformContract()),
		[]byte(validSourceForTransform()),
		[]byte(`not json`),
	)
	if !r.Valid {
		t.Errorf("expected valid (bad dest skips cross-ref), got: %v", r.Issues)
	}
}

// --- Helpers ----------------------------------------------------------------

func validTransformContract() string {
	return `{
		"contract_type": "transformation",
		"transformation_id": "t1",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [
			{"source_field": "name", "destination_field": "name", "confidence": 1.0},
			{"source_field": "age", "destination_field": "age", "confidence": 0.9, "transformation": {"type": "cast"}}
		],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
}

func validSourceForTransform() string {
	return `{
		"contract_type": "source",
		"source_format": "csv",
		"fields": [
			{"name": "name", "data_type": "text"},
			{"name": "age", "data_type": "numeric"}
		]
	}`
}

func validDestForTransform() string {
	return `{
		"contract_type": "destination",
		"id": "db",
		"schemas": [{
			"name": "users",
			"fields": [
				{"name": "name", "data_type": "varchar"},
				{"name": "age", "data_type": "integer"},
				{"name": "id", "data_type": "integer"}
			]
		}]
	}`
}
