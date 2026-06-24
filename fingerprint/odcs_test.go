package fingerprint

import (
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/jsoncontract"
	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsemit"
	"github.com/JacobJNilsson/data-contract-generator/profile"
)

// --- the byte-identity conformance corpus (spec section 5, the DG-3 gate) -

// csvCorpus is the set of CSV analyses the conformance proof runs over. Each
// exercises a different facet of the canonicalisation: the plain types, the
// temporal split, an unobservable column, duplicate names, non-ASCII names,
// and varied parse profiles. For every one of them, FromCSV and
// FromODCS(FromSourceContract(.)) must produce byte-identical canonical
// bytes.
func csvCorpus() []csvcontract.SourceContract {
	return []csvcontract.SourceContract{
		{
			SourcePath: "plain.csv", Delimiter: ",", Encoding: "UTF-8", HasHeader: true,
			Fields: []csvcontract.Field{
				{Name: "name", DataType: profile.TypeText},
				{Name: "qty", DataType: profile.TypeNumeric},
				{Name: "ok", DataType: profile.TypeBoolean},
			},
		},
		{
			SourcePath: "temporal.csv", Delimiter: ";", Encoding: "ISO-8859-1", HasHeader: false,
			Fields: []csvcontract.Field{
				{Name: "day", DataType: profile.TypeDate},
				{Name: "moment", DataType: profile.TypeTimestamp},
			},
		},
		{
			SourcePath: "unknowns.csv", Delimiter: "\t", Encoding: "UTF-8", HasHeader: true,
			Fields: []csvcontract.Field{
				{Name: "filled", DataType: profile.TypeText},
				{Name: "blank", DataType: profile.TypeEmpty},
			},
		},
		{
			SourcePath: "dupes.csv", Delimiter: ",", Encoding: "UTF-8", HasHeader: true,
			Fields: []csvcontract.Field{
				{Name: "id", DataType: profile.TypeNumeric},
				{Name: "id", DataType: profile.TypeText},
				{Name: "Ärlig", DataType: profile.TypeText},
			},
		},
	}
}

func TestConformanceCSVByteIdentical(t *testing.T) {
	for _, sc := range csvCorpus() {
		t.Run(sc.SourcePath, func(t *testing.T) {
			native, err := FromCSV(&sc)
			if err != nil {
				t.Fatalf("FromCSV: %v", err)
			}
			units, skipped, err := FromODCS(odcsemit.FromSourceContract(sc))
			if err != nil {
				t.Fatalf("FromODCS: %v", err)
			}
			if len(skipped) != 0 {
				t.Fatalf("unexpected skipped: %+v", skipped)
			}
			if len(units) != 1 {
				t.Fatalf("expected 1 unit, got %d", len(units))
			}
			gotBytes := units[0].Object.CanonicalBytes()
			wantBytes := native.CanonicalBytes()
			if string(gotBytes) != string(wantBytes) {
				t.Errorf("canonical bytes differ:\nnative: %s\nodcs:   %s", wantBytes, gotBytes)
			}
			if units[0].Object.Hash() != native.Hash() {
				t.Errorf("hash differs: native %s vs odcs %s", native.Hash(), units[0].Object.Hash())
			}
		})
	}
}

func jsonCorpus() []jsoncontract.SourceContract {
	return []jsoncontract.SourceContract{
		{
			SourcePath: "events.json", SourceFormat: "json", Encoding: "UTF-8",
			Fields: []jsoncontract.Field{
				{Name: "label", DataType: jsoncontract.TypeText},
				{Name: "count", DataType: jsoncontract.TypeNumeric},
				{Name: "flag", DataType: jsoncontract.TypeBoolean},
				{Name: "payload", DataType: jsoncontract.TypeObject},
				{Name: "items", DataType: jsoncontract.TypeArray},
			},
		},
		{
			SourcePath: "stream.ndjson", SourceFormat: "ndjson", Encoding: "UTF-8",
			Fields: []jsoncontract.Field{
				{Name: "a", DataType: jsoncontract.TypeText},
				{Name: "missing", DataType: jsoncontract.TypeNull},
				{Name: "blank", DataType: jsoncontract.TypeEmpty},
			},
		},
	}
}

func TestConformanceJSONByteIdentical(t *testing.T) {
	for _, sc := range jsonCorpus() {
		t.Run(sc.SourcePath, func(t *testing.T) {
			native, err := FromJSON(&sc)
			if err != nil {
				t.Fatalf("FromJSON: %v", err)
			}
			units, skipped, err := FromODCS(odcsemit.FromJSONContract(sc))
			if err != nil {
				t.Fatalf("FromODCS: %v", err)
			}
			if len(skipped) != 0 || len(units) != 1 {
				t.Fatalf("units=%d skipped=%+v", len(units), skipped)
			}
			if string(units[0].Object.CanonicalBytes()) != string(native.CanonicalBytes()) {
				t.Errorf("canonical bytes differ:\nnative: %s\nodcs:   %s",
					native.CanonicalBytes(), units[0].Object.CanonicalBytes())
			}
		})
	}
}

func TestConformanceDataContractByteIdentical(t *testing.T) {
	dc := contract.DataContract{
		ID:       "workbook",
		Metadata: map[string]any{"source_format": "xlsx"},
		Schemas: []contract.SchemaContract{
			{Name: "Sheet1", Fields: []contract.FieldDefinition{
				{Name: "a", DataType: "text"},
				{Name: "n", DataType: "numeric"},
			}},
			{Name: "Sheet2", Fields: []contract.FieldDefinition{
				{Name: "d", DataType: "date"},
			}},
		},
	}
	nativeUnits, nativeSkipped, err := FromDataContract(&dc)
	if err != nil {
		t.Fatalf("FromDataContract: %v", err)
	}
	odcsUnits, odcsSkipped, err := FromODCS(odcsemit.FromDataContract(dc))
	if err != nil {
		t.Fatalf("FromODCS: %v", err)
	}
	if len(nativeSkipped) != 0 || len(odcsSkipped) != 0 {
		t.Fatalf("skipped native=%+v odcs=%+v", nativeSkipped, odcsSkipped)
	}
	if len(nativeUnits) != len(odcsUnits) {
		t.Fatalf("unit count native=%d odcs=%d", len(nativeUnits), len(odcsUnits))
	}
	for i := range nativeUnits {
		if nativeUnits[i].Locator != odcsUnits[i].Locator {
			t.Errorf("unit %d locator native=%q odcs=%q", i, nativeUnits[i].Locator, odcsUnits[i].Locator)
		}
		if string(nativeUnits[i].Object.CanonicalBytes()) != string(odcsUnits[i].Object.CanonicalBytes()) {
			t.Errorf("unit %d bytes differ:\nnative: %s\nodcs:   %s", i,
				nativeUnits[i].Object.CanonicalBytes(), odcsUnits[i].Object.CanonicalBytes())
		}
	}
}

// TestConformanceAPIByteIdentical covers the openapi-marked format, the
// other branch of dataContractFormat.
func TestConformanceAPIByteIdentical(t *testing.T) {
	dc := contract.DataContract{
		ID:       "spec",
		Metadata: map[string]any{"source": "openapi"},
		Schemas: []contract.SchemaContract{
			{Name: "GET /things", Fields: []contract.FieldDefinition{{Name: "id", DataType: "integer"}}},
		},
	}
	nativeUnits, _, err := FromDataContract(&dc)
	if err != nil {
		t.Fatalf("FromDataContract: %v", err)
	}
	odcsUnits, _, err := FromODCS(odcsemit.FromDataContract(dc))
	if err != nil {
		t.Fatalf("FromODCS: %v", err)
	}
	if string(nativeUnits[0].Object.CanonicalBytes()) != string(odcsUnits[0].Object.CanonicalBytes()) {
		t.Errorf("bytes differ:\nnative: %s\nodcs:   %s",
			nativeUnits[0].Object.CanonicalBytes(), odcsUnits[0].Object.CanonicalBytes())
	}
}

// --- direct unit coverage of FromODCS and the ODCS token map --------------

func TestFromODCSNoSchema(t *testing.T) {
	if _, _, err := FromODCS(odcs.Contract{}); err == nil {
		t.Error("expected error for ODCS contract with no schema objects")
	}
}

func TestFromODCSMissingFormat(t *testing.T) {
	c := odcs.Contract{Schema: []odcs.SchemaObject{{
		Name:       "t",
		Properties: []odcs.Property{{Name: "a", LogicalType: odcs.LogicalString, PhysicalType: "text"}},
	}}}
	_, skipped, err := FromODCS(c)
	if err == nil {
		t.Fatal("expected error when every object is unfingerprintable")
	}
	if len(skipped) != 1 {
		t.Fatalf("expected one skipped object, got %+v", skipped)
	}
}

func TestFromODCSPartialSkip(t *testing.T) {
	// One good object, one missing its format: the good one survives, the
	// bad one is isolated into skipped.
	good := odcsemit.FromSourceContract(csvcontract.SourceContract{
		SourcePath: "ok.csv", Delimiter: ",", Encoding: "UTF-8", HasHeader: true,
		Fields: []csvcontract.Field{{Name: "a", DataType: profile.TypeText}},
	}).Schema[0]
	bad := odcs.SchemaObject{Name: "bad", Properties: []odcs.Property{{Name: "x", LogicalType: odcs.LogicalString, PhysicalType: "text"}}}
	c := odcs.Contract{Schema: []odcs.SchemaObject{good, bad}}
	units, skipped, err := FromODCS(c)
	if err != nil {
		t.Fatalf("FromODCS: %v", err)
	}
	if len(units) != 1 || units[0].Locator != "ok.csv" {
		t.Errorf("units = %+v", units)
	}
	if len(skipped) != 1 || skipped[0].Locator != "bad" {
		t.Errorf("skipped = %+v", skipped)
	}
}

func TestFromODCSFormatTypeAndValueErrors(t *testing.T) {
	mk := func(props ...odcs.CustomProperty) odcs.Contract {
		return odcs.Contract{Schema: []odcs.SchemaObject{{
			Name:             "t",
			CustomProperties: props,
			Properties:       []odcs.Property{{Name: "a", LogicalType: odcs.LogicalString, PhysicalType: "text"}},
		}}}
	}
	cases := []struct {
		name  string
		props []odcs.CustomProperty
	}{
		{"non-string format", []odcs.CustomProperty{{Property: odcs.CustomKeySourceFormat, Value: 7}}},
		{"unrecognised format", []odcs.CustomProperty{{Property: odcs.CustomKeySourceFormat, Value: "parquet"}}},
		{"csv missing delimiter", []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "csv"},
			{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
			{Property: odcs.CustomKeyHasHeader, Value: true},
		}},
		{"csv non-string delimiter", []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "csv"},
			{Property: odcs.CustomKeyDelimiter, Value: 1},
			{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
			{Property: odcs.CustomKeyHasHeader, Value: true},
		}},
		{"csv missing encoding", []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "csv"},
			{Property: odcs.CustomKeyDelimiter, Value: ","},
			{Property: odcs.CustomKeyHasHeader, Value: true},
		}},
		{"csv missing header", []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "csv"},
			{Property: odcs.CustomKeyDelimiter, Value: ","},
			{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
		}},
		{"csv non-bool header", []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "csv"},
			{Property: odcs.CustomKeyDelimiter, Value: ","},
			{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
			{Property: odcs.CustomKeyHasHeader, Value: "yes"},
		}},
		{"json missing encoding", []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "json"},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := FromODCS(mk(c.props...)); err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

func TestFromODCSUnmappedFieldType(t *testing.T) {
	// A property whose type token mapDataType does not know fails the whole
	// object (which then becomes a contract-level error here).
	c := odcs.Contract{Schema: []odcs.SchemaObject{{
		Name: "t",
		CustomProperties: []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "json"},
			{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
		},
		Properties: []odcs.Property{{Name: "a", LogicalType: odcs.LogicalType("mystery")}},
	}}}
	if _, _, err := FromODCS(c); err == nil {
		t.Error("expected error for unmapped logical type")
	}
}

func TestFromODCSNoFields(t *testing.T) {
	c := odcs.Contract{Schema: []odcs.SchemaObject{{
		Name: "t",
		CustomProperties: []odcs.CustomProperty{
			{Property: odcs.CustomKeySourceFormat, Value: "json"},
			{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
		},
	}}}
	if _, _, err := FromODCS(c); err == nil {
		t.Error("expected error for schema object with no properties")
	}
}

// TestODCSTokenSpecialTypes covers the spec section 5 physicalType overrides
// directly: bytea -> BINARY, uuid -> STRING, json/jsonb -> OBJECT, array with
// items -> ARRAY<inner>, and time -> TEMPORAL. These types do not arise from
// the file analysers yet, so they are proven through FromODCS against the
// canonical lattice rather than through a native byte comparison.
func TestODCSTokenSpecialTypes(t *testing.T) {
	cases := []struct {
		name string
		prop odcs.Property
		want CanonicalType
	}{
		{"bytea-binary", odcs.Property{Name: "b", LogicalType: odcs.LogicalString, PhysicalType: "bytea"}, TypeBinary},
		{"uuid-string", odcs.Property{Name: "u", LogicalType: odcs.LogicalString, PhysicalType: "uuid"}, TypeString},
		{"json-object", odcs.Property{Name: "j", LogicalType: odcs.LogicalString, PhysicalType: "json"}, TypeObject},
		{"jsonb-object", odcs.Property{Name: "k", LogicalType: odcs.LogicalString, PhysicalType: "jsonb"}, TypeObject},
		{"integer-number", odcs.Property{Name: "i", LogicalType: odcs.LogicalInteger, PhysicalType: "bigint"}, TypeNumber},
		{"time-temporal", odcs.Property{Name: "t", LogicalType: odcs.LogicalTime}, TypeTemporal},
		{"array-inner", odcs.Property{
			Name: "a", LogicalType: odcs.LogicalArray, PhysicalType: "text[]",
			Items: &odcs.Property{LogicalType: odcs.LogicalString, PhysicalType: "text"},
		}, CanonicalType("ARRAY<STRING>")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := odcs.Contract{Schema: []odcs.SchemaObject{{
				Name: "t",
				CustomProperties: []odcs.CustomProperty{
					{Property: odcs.CustomKeySourceFormat, Value: "json"},
					{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
				},
				Properties: []odcs.Property{c.prop},
			}}}
			units, _, err := FromODCS(obj)
			if err != nil {
				t.Fatalf("FromODCS: %v", err)
			}
			if units[0].Object.Fields[0].Type != c.want {
				t.Errorf("type = %q, want %q", units[0].Object.Fields[0].Type, c.want)
			}
		})
	}
}

// TestODCSTokenUnknownColumn covers the empty/null physical token collapse to
// UNKNOWN and the echo-through of an unrecognised physical token under no
// logical type.
func TestODCSTokenUnknownColumn(t *testing.T) {
	cases := []struct {
		physical string
		want     CanonicalType
		wantErr  bool
	}{
		{"empty", TypeUnknown, false},
		{"null", TypeUnknown, false},
		{"", TypeUnknown, false},
		{"weird", "", true}, // echoed through, then mapDataType fails closed
	}
	for _, c := range cases {
		t.Run(c.physical, func(t *testing.T) {
			obj := odcs.Contract{Schema: []odcs.SchemaObject{{
				Name: "t",
				CustomProperties: []odcs.CustomProperty{
					{Property: odcs.CustomKeySourceFormat, Value: "json"},
					{Property: odcs.CustomKeyEncoding, Value: "UTF-8"},
				},
				Properties: []odcs.Property{{Name: "x", PhysicalType: c.physical}},
			}}}
			units, _, err := FromODCS(obj)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error for physical %q", c.physical)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromODCS: %v", err)
			}
			if units[0].Object.Fields[0].Type != c.want {
				t.Errorf("type = %q, want %q", units[0].Object.Fields[0].Type, c.want)
			}
		})
	}
}
