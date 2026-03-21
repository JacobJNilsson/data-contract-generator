package verify

import (
	"fmt"
	"strings"
	"testing"
)

// --- Verify dispatch tests --------------------------------------------------

func TestVerifyInvalidJSON(t *testing.T) {
	r := Verify([]byte(`{not json`))
	assertInvalid(t, r, "invalid JSON")
}

func TestVerifyMissingContractType(t *testing.T) {
	r := Verify([]byte(`{"foo": "bar"}`))
	assertInvalid(t, r, "missing or empty contract_type")
}

func TestVerifyEmptyContractType(t *testing.T) {
	r := Verify([]byte(`{"contract_type": ""}`))
	assertInvalid(t, r, "missing or empty contract_type")
}

func TestVerifyUnknownContractType(t *testing.T) {
	r := Verify([]byte(`{"contract_type": "magic"}`))
	assertInvalid(t, r, "unknown contract_type")
}

func TestVerifyNullContractType(t *testing.T) {
	r := Verify([]byte(`{"contract_type": null}`))
	assertInvalid(t, r, "missing or empty contract_type")
}

// --- VerifyReader tests -----------------------------------------------------

func TestVerifyReaderValid(t *testing.T) {
	data := validCSVSourceContract()
	r, err := Reader(strings.NewReader(data))
	if err != nil {
		t.Fatalf("VerifyReader: %v", err)
	}
	if !r.Valid {
		t.Errorf("expected valid, got issues: %v", r.Issues)
	}
}

// --- Source contract tests --------------------------------------------------

func TestVerifySourceValid_CSV(t *testing.T) {
	r := Verify([]byte(validCSVSourceContract()))
	if !r.Valid {
		t.Errorf("expected valid CSV source, got issues: %v", r.Issues)
	}
	if r.ContractType != "source" {
		t.Errorf("contract_type = %q, want source", r.ContractType)
	}
}

func TestVerifySourceValid_JSON(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 2,
		"fields": [
			{"name": "id", "data_type": "numeric", "profile": {"total_count": 2, "null_count": 0, "null_percentage": 0, "top_values": []}}
		],
		"sample_data": []
	}`
	r := Verify([]byte(data))
	if !r.Valid {
		t.Errorf("expected valid JSON source, got issues: %v", r.Issues)
	}
}

func TestVerifySourceValid_NDJSON(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "ndjson",
		"total_rows": 0,
		"fields": [],
		"sample_data": []
	}`
	r := Verify([]byte(data))
	if !r.Valid {
		t.Errorf("expected valid NDJSON source, got issues: %v", r.Issues)
	}
}

func TestVerifySourceMissingFormat(t *testing.T) {
	data := `{
		"contract_type": "source",
		"total_rows": 0,
		"fields": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing source_format")
}

func TestVerifySourceUnknownFormat(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "xml",
		"total_rows": 0,
		"fields": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown source_format")
}

func TestVerifySourceCSVMissingDelimiter(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "csv",
		"has_header": true,
		"total_rows": 0,
		"fields": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing delimiter")
}

func TestVerifySourceCSVMissingHasHeader(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "csv",
		"delimiter": ",",
		"total_rows": 0,
		"fields": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing has_header")
}

func TestVerifySourceNegativeTotalRows(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": -1,
		"fields": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "total_rows is negative")
}

func TestVerifySourceRowsButNoFields(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 10,
		"fields": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "total_rows > 0 but no fields")
}

func TestVerifySourceDuplicateFieldName(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "id", "data_type": "numeric", "profile": {"total_count": 1, "null_count": 0, "null_percentage": 0, "top_values": []}},
			{"name": "id", "data_type": "text", "profile": {"total_count": 1, "null_count": 0, "null_percentage": 0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "duplicate field name")
}

func TestVerifySourceMissingFieldName(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "", "data_type": "text", "profile": {"total_count": 1, "null_count": 0, "null_percentage": 0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing name")
}

func TestVerifySourceUnknownDataType(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "x", "data_type": "foobar", "profile": {"total_count": 1, "null_count": 0, "null_percentage": 0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown data_type")
}

func TestVerifySourceMissingDataType(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "x", "data_type": "", "profile": {"total_count": 1, "null_count": 0, "null_percentage": 0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing data_type")
}

func TestVerifySourceNullCountExceedsTotal(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "x", "data_type": "text", "profile": {"total_count": 5, "null_count": 10, "null_percentage": 200, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "null_count")
	assertInvalid(t, r, "null_percentage")
}

func TestVerifySourceNullPercentageInconsistent(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 2,
		"fields": [
			{"name": "x", "data_type": "text", "profile": {"total_count": 100, "null_count": 50, "null_percentage": 10.0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "null_percentage")
	assertInvalid(t, r, "inconsistent")
}

func TestVerifySourceTopValueZeroCount(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "x", "data_type": "text", "profile": {"total_count": 1, "null_count": 0, "null_percentage": 0, "top_values": [{"value": "a", "count": 0}]}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "count must be positive")
}

// --- Destination contract tests ---------------------------------------------

func TestVerifyDestinationValid(t *testing.T) {
	r := Verify([]byte(validDestinationContract()))
	if !r.Valid {
		t.Errorf("expected valid destination, got issues: %v", r.Issues)
	}
	if r.ContractType != "destination" {
		t.Errorf("contract_type = %q, want destination", r.ContractType)
	}
}

func TestVerifyDestinationMissingDatabaseID(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "",
		"tables": [{"table_name": "t", "fields": [{"name": "id", "data_type": "integer", "nullable": false}], "validation_rules": {}}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing database_id")
}

func TestVerifyDestinationNoTables(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": []
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "no tables defined")
}

func TestVerifyDestinationDuplicateTable(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [
			{"table_name": "t", "fields": [{"name": "id", "data_type": "integer"}], "validation_rules": {}},
			{"table_name": "t", "fields": [{"name": "id", "data_type": "integer"}], "validation_rules": {}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "duplicate table_name")
}

func TestVerifyDestinationMissingTableName(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [
			{"table_name": "", "fields": [{"name": "id", "data_type": "integer"}], "validation_rules": {}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing table_name")
}

func TestVerifyDestinationUnknownDataType(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [
			{"table_name": "t", "fields": [{"name": "id", "data_type": "foobar"}], "validation_rules": {}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown data_type")
}

func TestVerifyDestinationDuplicateField(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [
			{"table_name": "t", "fields": [
				{"name": "id", "data_type": "integer"},
				{"name": "id", "data_type": "text"}
			], "validation_rules": {}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "duplicate field name")
}

func TestVerifyDestinationUnknownConstraintType(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "id", "data_type": "integer", "constraints": [{"type": "magic"}]}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unknown constraint type")
}

func TestVerifyDestinationEmptyConstraintType(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "id", "data_type": "integer", "constraints": [{"type": ""}]}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing type")
}

func TestVerifyDestinationFKMissingReferences(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "user_id", "data_type": "integer", "constraints": [{"type": "foreign_key"}]}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing referred_table")
	assertInvalid(t, r, "missing referred_column")
}

func TestVerifyDestinationFKEmptyReferences(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "user_id", "data_type": "integer", "constraints": [{"type": "foreign_key", "referred_table": "", "referred_column": ""}]}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing referred_table")
	assertInvalid(t, r, "missing referred_column")
}

// --- Semantic: validation rules reference real fields -----------------------

func TestVerifyDestinationRequiredFieldUnknown(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "id", "data_type": "integer", "nullable": false, "constraints": [{"type": "not_null"}]}],
			"validation_rules": {"required_fields": ["id", "ghost"]}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "required_fields references unknown field")
	assertInvalid(t, r, "ghost")
}

func TestVerifyDestinationUniqueConstraintUnknown(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "id", "data_type": "integer"}],
			"validation_rules": {"unique_constraints": ["id", "typo"]}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "unique_constraints references unknown field")
	assertInvalid(t, r, "typo")
}

// --- Semantic: not_null consistency ----------------------------------------

func TestVerifyDestinationNotNullableMissingConstraint(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "id", "data_type": "integer", "nullable": false, "constraints": [{"type": "primary_key"}]}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "not nullable but has no not_null constraint")
}

// --- VerifyReader error path ------------------------------------------------

func TestVerifyReaderError(t *testing.T) {
	_, err := Reader(&failReader{})
	if err == nil {
		t.Fatal("expected error for failing reader")
	}
}

type failReader struct{}

func (r *failReader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("disk error")
}

// --- Source unmarshal error --------------------------------------------------

func TestVerifySourceBadJSON(t *testing.T) {
	// contract_type is "source" but the rest is invalid for sourceContract struct.
	// Since JSON unmarshal into structs is lenient (ignores unknown, zero-values missing),
	// we need a truly broken structure. Use a non-object contract_type field.
	data := `{"contract_type": "source", "fields": "not an array"}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "failed to parse source contract")
}

// --- Destination additional tests -------------------------------------------

func TestVerifyDestinationBadJSON(t *testing.T) {
	data := `{"contract_type": "destination", "tables": "not an array"}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "failed to parse destination contract")
}

func TestVerifyDestinationNegativeRowCount(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"row_count": -5,
			"fields": [{"name": "id", "data_type": "integer", "nullable": false, "constraints": [{"type": "not_null"}]}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "row_count is negative")
}

func TestVerifyDestinationMissingFieldName(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "", "data_type": "integer"}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing name")
}

func TestVerifyDestinationMissingFieldDataType(t *testing.T) {
	data := `{
		"contract_type": "destination",
		"database_id": "db",
		"tables": [{
			"table_name": "t",
			"fields": [{"name": "id", "data_type": ""}],
			"validation_rules": {}
		}]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "missing data_type")
}

func TestVerifySourceNegativeTotalCount(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "x", "data_type": "text", "profile": {"total_count": -1, "null_count": 0, "null_percentage": 0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "total_count is negative")
}

func TestVerifySourceNegativeNullCount(t *testing.T) {
	data := `{
		"contract_type": "source",
		"source_format": "json",
		"total_rows": 1,
		"fields": [
			{"name": "x", "data_type": "text", "profile": {"total_count": 1, "null_count": -1, "null_percentage": 0, "top_values": []}}
		]
	}`
	r := Verify([]byte(data))
	assertInvalid(t, r, "null_count is negative")
}

// --- Test helpers -----------------------------------------------------------

func assertInvalid(t *testing.T, r Result, substr string) {
	t.Helper()
	if r.Valid {
		t.Errorf("expected invalid result containing %q, got valid", substr)
		return
	}
	for _, issue := range r.Issues {
		if strings.Contains(issue, substr) {
			return
		}
	}
	t.Errorf("expected issue containing %q, got: %v", substr, r.Issues)
}

func validCSVSourceContract() string {
	return `{
		"contract_type": "source",
		"source_format": "csv",
		"encoding": "utf-8",
		"delimiter": ",",
		"has_header": true,
		"total_rows": 3,
		"fields": [
			{
				"name": "id",
				"data_type": "numeric",
				"profile": {
					"total_count": 3,
					"null_count": 0,
					"null_percentage": 0,
					"top_values": [{"value": "1", "count": 1}]
				}
			},
			{
				"name": "name",
				"data_type": "text",
				"profile": {
					"total_count": 3,
					"null_count": 0,
					"null_percentage": 0,
					"top_values": [{"value": "Alice", "count": 1}]
				}
			}
		],
		"sample_data": [["1", "Alice"]]
	}`
}

func validDestinationContract() string {
	return `{
		"contract_type": "destination",
		"database_id": "mydb",
		"tables": [{
			"table_name": "users",
			"schema": "public",
			"row_count": 100,
			"fields": [
				{
					"name": "id",
					"data_type": "integer",
					"nullable": false,
					"constraints": [{"type": "primary_key"}, {"type": "not_null"}]
				},
				{
					"name": "email",
					"data_type": "varchar",
					"nullable": false,
					"constraints": [{"type": "unique"}, {"type": "not_null"}]
				},
				{
					"name": "bio",
					"data_type": "text",
					"nullable": true
				}
			],
			"validation_rules": {
				"required_fields": ["id", "email"],
				"unique_constraints": ["id", "email"]
			}
		}]
	}`
}
