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
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0}],
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
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0}],
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

func TestVerifyTransformation_MissingDestField(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "", "source_type": "field", "source_field": "a", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing destination_field")
}

func TestVerifyTransformation_UnknownSourceType(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "a", "source_type": "magic", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown source_type")
}

func TestVerifyTransformation_FieldTypeEmptySourceField(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "source_field is empty")
}

func TestVerifyTransformation_NullTypeValid(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "bio", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	if !r.Valid {
		t.Errorf("expected valid for null source_type, got: %v", r.Issues)
	}
}

func TestVerifyTransformation_ConstantTypeValid(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "status", "source_type": "constant", "source_constant": "active", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	if !r.Valid {
		t.Errorf("expected valid for constant source_type, got: %v", r.Issues)
	}
}

func TestVerifyTransformation_UnmappedTypeValid(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "todo", "source_type": "unmapped", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	if !r.Valid {
		t.Errorf("expected valid for unmapped source_type, got: %v", r.Issues)
	}
}

func TestVerifyTransformation_ValidTransformTypes(t *testing.T) {
	for _, tt := range []string{"rename", "cast", "format", "default"} {
		data := `{
			"contract_type": "transformation",
			"source_ref": "src",
			"destination_ref": "dest",
			"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0, "transformation": {"type": "` + tt + `"}}],
			"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
		}`
		r := Verify([]byte(data))
		if !r.Valid {
			t.Errorf("transform type %q should be valid, got: %v", tt, r.Issues)
		}
	}
}

func TestVerifyTransformation_ConfidenceOutOfRange(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.5}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "confidence")
}

func TestVerifyTransformation_NegativeBatchSize(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0}],
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
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0}],
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

func TestVerifyTransformation_EmptyTransformType(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0, "transformation": {"type": ""}}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing type")
}

func TestVerifyTransformation_UnknownTransformType(t *testing.T) {
	data := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "a", "source_type": "field", "source_field": "b", "confidence": 1.0, "transformation": {"type": "magic"}}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown type")
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
		"field_mappings": [{"destination_field": "name", "source_type": "field", "source_field": "nonexistent", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "source field 'nonexistent' not found")
}

func TestVerifyWithContext_DestFieldNotFound(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "ghost", "source_type": "field", "source_field": "name", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "destination field not found")
}

func TestVerifyWithContext_NullOnNonNullable(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "id", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "'id' is NOT NULL")
	assertInvalid(t, r, "PRIMARY KEY")
	assertInvalid(t, r, "mapped to NULL")
}

func TestVerifyWithContext_UnmappedNonNullable(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "name", "source_type": "unmapped", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "'name' is NOT NULL")
	assertInvalid(t, r, "no source mapping")
}

func TestVerifyWithContext_NullOnNullableOK(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "bio", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	if !r.Valid {
		t.Errorf("expected valid (null on nullable), got: %v", r.Issues)
	}
}

func TestVerifyWithContext_ConstantOnNonNullableOK(t *testing.T) {
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "name", "source_type": "constant", "source_constant": "default", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	if !r.Valid {
		t.Errorf("expected valid (constant on non-nullable), got: %v", r.Issues)
	}
}

func TestVerifyWithContext_NullOnUniqueNonNullable(t *testing.T) {
	dest := `{
		"contract_type": "destination",
		"id": "db",
		"schemas": [{"name": "t", "fields": [
			{"name": "email", "data_type": "varchar", "nullable": false, "constraints": [{"type": "unique"}, {"type": "not_null"}]}
		]}]
	}`
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "email", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext([]byte(transform), []byte(validSourceForTransform()), []byte(dest))
	assertInvalid(t, r, "UNIQUE")
}

func TestVerifyWithContext_NullOnForeignKeyNonNullable(t *testing.T) {
	dest := `{
		"contract_type": "destination",
		"id": "db",
		"schemas": [{"name": "t", "fields": [
			{"name": "user_id", "data_type": "integer", "nullable": false, "constraints": [{"type": "foreign_key"}, {"type": "not_null"}]}
		]}]
	}`
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "user_id", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext([]byte(transform), []byte(validSourceForTransform()), []byte(dest))
	assertInvalid(t, r, "FOREIGN KEY")
}

func TestVerifyWithContext_EmptySourceTypeOnNonNullable(t *testing.T) {
	// Empty source_type (zero value) should be treated same as "unmapped"
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "name", "source_type": "", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	assertInvalid(t, r, "'name' is NOT NULL")
	assertInvalid(t, r, "no source mapping")
}

func TestVerifyWithContext_MultipleConstraintsShowsMostImportant(t *testing.T) {
	// A field with both primary_key and unique shows PRIMARY KEY (highest priority)
	dest := `{
		"contract_type": "destination",
		"id": "db",
		"schemas": [{"name": "t", "fields": [
			{"name": "id", "data_type": "integer", "nullable": false, "constraints": [{"type": "primary_key"}, {"type": "unique"}, {"type": "not_null"}]}
		]}]
	}`
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "id", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext([]byte(transform), []byte(validSourceForTransform()), []byte(dest))
	assertInvalid(t, r, "PRIMARY KEY")
}

func TestVerifyWithContext_StructuralFailure(t *testing.T) {
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
	r := TransformationWithContext(
		[]byte(validTransformContract()),
		[]byte(`not json`),
		[]byte(validDestForTransform()),
	)
	if !r.Valid {
		t.Errorf("expected valid (bad source skips cross-ref), got: %v", r.Issues)
	}
}

func TestVerifyWithContext_NullOnNonNullableNoConstraints(t *testing.T) {
	// A non-nullable field with no explicit constraints (just nullable: false)
	dest := `{
		"contract_type": "destination",
		"id": "db",
		"schemas": [{"name": "t", "fields": [
			{"name": "required_field", "data_type": "text", "nullable": false}
		]}]
	}`
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "required_field", "source_type": "null", "confidence": 0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext([]byte(transform), []byte(validSourceForTransform()), []byte(dest))
	assertInvalid(t, r, "NOT NULL")
}

func TestVerifyWithContext_FieldOnNullableSkipsConstraintCheck(t *testing.T) {
	// A mapped field on a nullable destination — should always pass
	transform := `{
		"contract_type": "transformation",
		"source_ref": "src",
		"destination_ref": "dest",
		"field_mappings": [{"destination_field": "bio", "source_type": "field", "source_field": "name", "confidence": 1.0}],
		"execution_plan": {"batch_size": 100, "error_threshold": 0.1, "validation_enabled": true}
	}`
	r := TransformationWithContext(
		[]byte(transform),
		[]byte(validSourceForTransform()),
		[]byte(validDestForTransform()),
	)
	if !r.Valid {
		t.Errorf("expected valid, got: %v", r.Issues)
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
			{"destination_field": "name", "source_type": "field", "source_field": "name", "confidence": 1.0},
			{"destination_field": "age", "source_type": "field", "source_field": "age", "confidence": 0.9, "transformation": {"type": "cast"}}
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
				{"name": "id", "data_type": "integer", "nullable": false, "constraints": [{"type": "primary_key"}, {"type": "not_null"}]},
				{"name": "name", "data_type": "varchar", "nullable": false, "constraints": [{"type": "not_null"}]},
				{"name": "age", "data_type": "integer", "nullable": true},
				{"name": "bio", "data_type": "text", "nullable": true}
			]
		}]
	}`
}
