package odcs_test

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// The official ODCS v3.1.0 JSON Schema (the offline copy the Data Contract CLI
// ships). Validating against it makes ODCS conformance a checked fact rather
// than an asserted one: if the model drifts from a required field or a
// constraint the standard imposes, this test fails instead of the breakage
// surfacing later in an ecosystem tool.
//
//go:embed testdata/odcs-3.1.0.schema.json
var odcsSchema []byte

func compileODCSSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	if err := c.AddResource("odcs.json", bytes.NewReader(odcsSchema)); err != nil {
		t.Fatalf("add schema resource: %v", err)
	}
	sch, err := c.Compile("odcs.json")
	if err != nil {
		t.Fatalf("compile ODCS schema: %v", err)
	}
	return sch
}

func validate(t *testing.T, sch *jsonschema.Schema, c odcs.Contract) error {
	t.Helper()
	raw, err := c.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal contract: %v", err)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode contract json: %v", err)
	}
	return sch.Validate(v)
}

// representativeContract is the OP-8 destination shape: every native type the
// platform cares about, including an enum encoded as the invalidValues quality
// rule and a typed array. It mirrors what the emitter produces.
func representativeContract() odcs.Contract {
	b := func(v bool) *bool { return &v }
	i := func(v int) *int { return &v }
	return odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         "spike-accounts",
		Version:    "1.0.0",
		Status:     odcs.StatusActive,
		Schema: []odcs.SchemaObject{{
			Name:         "accounts",
			PhysicalName: "accounts",
			PhysicalType: "table",
			Properties: []odcs.Property{
				{
					Name: "id", LogicalType: odcs.LogicalString, PhysicalType: "uuid",
					Required: b(true), PrimaryKey: b(true), PrimaryKeyPosition: i(1),
					LogicalTypeOptions: &odcs.LogicalTypeOptions{Format: "uuid"},
				},
				{Name: "balance", LogicalType: odcs.LogicalNumber, PhysicalType: "numeric(12,2)", Required: b(true)},
				{Name: "opened_on", LogicalType: odcs.LogicalDate, PhysicalType: "date"},
				{Name: "created_at", LogicalType: odcs.LogicalTimestamp, PhysicalType: "timestamptz"},
				{Name: "metadata", LogicalType: odcs.LogicalObject, PhysicalType: "jsonb"},
				{
					Name: "status", LogicalType: odcs.LogicalString, PhysicalType: "account_status",
					Quality: []odcs.Quality{{
						ID: "status_valid_values", Type: odcs.QualityLibrary, Metric: odcs.MetricInvalidValues,
						Arguments: map[string]any{"validValues": []string{"active", "closed", "pending"}},
						MustBe:    0, Unit: "rows",
					}},
				},
				{
					Name: "tags", LogicalType: odcs.LogicalArray, PhysicalType: "text[]",
					Items: &odcs.Property{LogicalType: odcs.LogicalString, PhysicalType: "text"},
				},
			},
		}},
	}
}

func TestRepresentativeContractValidatesAgainstODCSSchema(t *testing.T) {
	sch := compileODCSSchema(t)
	if err := validate(t, sch, representativeContract()); err != nil {
		t.Fatalf("emitted-shape contract failed official ODCS v3.1.0 schema: %v", err)
	}
}

// TestMissingStatusFailsODCSSchema proves the validator is real (not vacuous):
// the contract status is required by ODCS, and dropping it must fail. This is
// the exact conformance gap the real-tools spike found: the Data Contract CLI
// refused to export a contract without a status.
func TestMissingStatusFailsODCSSchema(t *testing.T) {
	sch := compileODCSSchema(t)
	c := representativeContract()
	c.Status = ""
	if err := validate(t, sch, c); err == nil {
		t.Fatal("expected ODCS schema validation to fail when status is absent")
	}
}
