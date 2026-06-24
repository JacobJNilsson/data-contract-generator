package fingerprint

import (
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsemit"
	"github.com/JacobJNilsson/data-contract-generator/profile"
)

// foreignBase returns a fingerprintable schema object (a valid source format
// and parse facts in customProperties) whose properties the caller replaces.
// It lets these tests feed FromODCS documents our own emitter would never
// produce, the way a DDL importer or "datacontract export odcs" would: native
// type names in mixed or upper case, and physical types paired with logical
// types the emitter does not emit.
func foreignBase(props []odcs.Property) odcs.Contract {
	base := odcsemit.FromSourceContract(csvcontract.SourceContract{
		SourcePath: "t.csv", Delimiter: ",", Encoding: "UTF-8", HasHeader: true,
		Fields: []csvcontract.Field{{Name: "seed", DataType: profile.TypeText}},
	}).Schema[0]
	base.Properties = props
	return odcs.Contract{Schema: []odcs.SchemaObject{base}}
}

// TestFromODCSForeignPhysicalTypeCasing proves the type projection folds the
// case of native physical types and treats bytea as binary regardless of the
// logical type it is paired with. A real producer emits "BYTEA"/"JSONB" and
// may type a binary column as object; the old exact-lowercase match silently
// collapsed those to the wrong canonical type instead of failing closed.
func TestFromODCSForeignPhysicalTypeCasing(t *testing.T) {
	c := foreignBase([]odcs.Property{
		{Name: "bin_upper", LogicalType: odcs.LogicalString, PhysicalType: "BYTEA"},
		{Name: "bin_under_object", LogicalType: odcs.LogicalObject, PhysicalType: "bytea"},
		{Name: "bin_spaced", LogicalType: odcs.LogicalString, PhysicalType: "  Bytea  "},
		{Name: "json_upper", LogicalType: odcs.LogicalString, PhysicalType: "JSONB"},
		{Name: "uuid_upper", LogicalType: odcs.LogicalString, PhysicalType: "UUID"},
		{Name: "num_upper", LogicalType: odcs.LogicalNumber, PhysicalType: "NUMERIC(10,2)"},
	})
	units, skipped, err := FromODCS(c)
	if err != nil {
		t.Fatalf("FromODCS: %v (skipped=%+v)", err, skipped)
	}
	if len(units) != 1 {
		t.Fatalf("want one unit, got %d (skipped=%+v)", len(units), skipped)
	}
	got := map[string]CanonicalType{}
	for _, f := range units[0].Object.Fields {
		got[f.Name] = f.Type
	}
	want := map[string]CanonicalType{
		"bin_upper":        TypeBinary,
		"bin_under_object": TypeBinary,
		"bin_spaced":       TypeBinary,
		"json_upper":       TypeObject,
		"uuid_upper":       TypeString,
		"num_upper":        TypeNumber,
	}
	for name, wantType := range want {
		if got[name] != wantType {
			t.Errorf("field %q: got %s, want %s", name, got[name], wantType)
		}
	}
}

// TestFromODCSForeignUnknownFailsClosed proves an unmodelled logical type from
// a foreign document still fails closed (the object is skipped) rather than
// being coerced to a confident wrong canonical type.
func TestFromODCSForeignUnknownFailsClosed(t *testing.T) {
	c := foreignBase([]odcs.Property{
		{Name: "weird", LogicalType: odcs.LogicalType("geography"), PhysicalType: "geography"},
	})
	units, skipped, err := FromODCS(c)
	if err == nil {
		t.Fatalf("expected fail-closed on an unmodelled logical type, got units=%+v", units)
	}
	if len(skipped) != 1 {
		t.Fatalf("expected one skipped object, got %+v", skipped)
	}
}
