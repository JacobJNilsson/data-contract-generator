package odcsemit

import (
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/jsoncontract"
	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/profile"
)

// Every emitter entry point must stamp the ODCS-required contract status.
// Without it the document fails official ODCS schema validation and the Data
// Contract CLI refuses to export it, which breaks the emission pipeline.
func TestEmittersSetContractStatus(t *testing.T) {
	csv := FromSourceContract(csvcontract.SourceContract{
		SourcePath: "t.csv", Delimiter: ",", Encoding: "UTF-8", HasHeader: true,
		Fields: []csvcontract.Field{{Name: "a", DataType: profile.TypeText}},
	})
	js := FromJSONContract(jsoncontract.SourceContract{
		SourcePath: "t.json", Encoding: "UTF-8",
		Fields: []jsoncontract.Field{{Name: "a", DataType: jsoncontract.TypeText}},
	})
	dc := FromDataContract(contract.DataContract{
		ID:      "t",
		Schemas: []contract.SchemaContract{{Name: "s", Fields: []contract.FieldDefinition{{Name: "a", DataType: "text"}}}},
	})
	for name, got := range map[string]odcs.Contract{"csv": csv, "json": js, "data": dc} {
		if got.Status != odcs.StatusActive {
			t.Errorf("%s emitter: status = %q, want %q", name, got.Status, odcs.StatusActive)
		}
	}
}
