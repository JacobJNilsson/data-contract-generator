package odcsemit

import (
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/jsoncontract"
	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/profile"
)

func TestFromSourceContractTypeEncoding(t *testing.T) {
	sc := csvcontract.SourceContract{
		SourcePath: "orders.csv",
		Fields: []csvcontract.Field{
			{Name: "name", DataType: profile.TypeText},
			{Name: "qty", DataType: profile.TypeNumeric},
			{Name: "day", DataType: profile.TypeDate},
			{Name: "seen", DataType: profile.TypeTimestamp},
			{Name: "ok", DataType: profile.TypeBoolean},
			{Name: "blank", DataType: profile.TypeEmpty},
		},
	}
	c := FromSourceContract(sc)

	if c.APIVersion != odcs.APIVersion || c.Kind != odcs.KindDataContract {
		t.Fatalf("contract envelope = %q/%q", c.APIVersion, c.Kind)
	}
	if c.ID != "orders.csv" {
		t.Errorf("id = %q, want orders.csv", c.ID)
	}
	if len(c.Schema) != 1 {
		t.Fatalf("expected one schema object, got %d", len(c.Schema))
	}
	obj := c.Schema[0]
	if obj.Name != "orders.csv" || obj.PhysicalName != "orders.csv" || obj.PhysicalType != "file" {
		t.Errorf("schema object = %+v", obj)
	}

	type want struct {
		logical  odcs.LogicalType
		physical string
		format   string
	}
	wants := []want{
		{odcs.LogicalString, "text", ""},
		{odcs.LogicalNumber, "numeric", ""},
		{odcs.LogicalDate, "date", "date"},
		{odcs.LogicalTimestamp, "timestamp", "date-time"},
		{odcs.LogicalBoolean, "boolean", ""},
		{"", "empty", ""},
	}
	if len(obj.Properties) != len(wants) {
		t.Fatalf("got %d properties, want %d", len(obj.Properties), len(wants))
	}
	for i, w := range wants {
		p := obj.Properties[i]
		if p.LogicalType != w.logical {
			t.Errorf("prop %d logicalType = %q, want %q", i, p.LogicalType, w.logical)
		}
		if p.PhysicalType != w.physical {
			t.Errorf("prop %d physicalType = %q, want %q", i, p.PhysicalType, w.physical)
		}
		gotFormat := ""
		if p.LogicalTypeOptions != nil {
			gotFormat = p.LogicalTypeOptions.Format
		}
		if gotFormat != w.format {
			t.Errorf("prop %d format = %q, want %q", i, gotFormat, w.format)
		}
	}
}

func TestFromSourceContractUnknownToken(t *testing.T) {
	// An unrecognised native token is echoed into physicalType with no
	// logical type, so the gap surfaces rather than being guessed away.
	sc := csvcontract.SourceContract{
		SourcePath: "x.csv",
		Fields:     []csvcontract.Field{{Name: "weird", DataType: profile.DataType("mystery")}},
	}
	p := FromSourceContract(sc).Schema[0].Properties[0]
	if p.LogicalType != "" || p.PhysicalType != "mystery" {
		t.Errorf("unknown token property = %+v", p)
	}
}

func TestFromJSONContractTypeEncoding(t *testing.T) {
	sc := jsoncontract.SourceContract{
		SourcePath:   "events.json",
		SourceFormat: "json",
		Fields: []jsoncontract.Field{
			{Name: "label", DataType: jsoncontract.TypeText},
			{Name: "count", DataType: jsoncontract.TypeNumeric},
			{Name: "flag", DataType: jsoncontract.TypeBoolean},
			{Name: "payload", DataType: jsoncontract.TypeObject},
			{Name: "items", DataType: jsoncontract.TypeArray},
			{Name: "missing", DataType: jsoncontract.TypeNull},
			{Name: "blank", DataType: jsoncontract.TypeEmpty},
		},
	}
	obj := FromJSONContract(sc).Schema[0]

	got := map[string]odcs.Property{}
	for _, p := range obj.Properties {
		got[p.Name] = p
	}
	if p := got["payload"]; p.LogicalType != odcs.LogicalObject || p.PhysicalType != "object" {
		t.Errorf("payload = %+v", p)
	}
	if p := got["items"]; p.LogicalType != odcs.LogicalArray || p.PhysicalType != "array" || p.Items != nil {
		t.Errorf("items = %+v (Items=%v)", p, p.Items)
	}
	if p := got["missing"]; p.LogicalType != "" || p.PhysicalType != "null" {
		t.Errorf("missing = %+v", p)
	}
	if p := got["blank"]; p.LogicalType != "" || p.PhysicalType != "empty" {
		t.Errorf("blank = %+v", p)
	}
	if p := got["label"]; p.LogicalType != odcs.LogicalString || p.PhysicalType != "text" {
		t.Errorf("label = %+v", p)
	}
}

func TestFromDataContractMultiSchemaAndNullability(t *testing.T) {
	dc := contract.DataContract{
		ID: "workbook",
		Schemas: []contract.SchemaContract{
			{
				Name: "Sheet1",
				Fields: []contract.FieldDefinition{
					{Name: "a", DataType: "text", Nullable: false},
					{Name: "b", DataType: "numeric", Nullable: true},
				},
			},
			{
				Name: "Sheet2",
				Fields: []contract.FieldDefinition{
					{Name: "c", DataType: "boolean", Nullable: false},
				},
			},
		},
	}
	c := FromDataContract(dc)
	if c.ID != "workbook" {
		t.Errorf("id = %q", c.ID)
	}
	if len(c.Schema) != 2 {
		t.Fatalf("expected 2 schema objects, got %d", len(c.Schema))
	}
	s1 := c.Schema[0]
	if s1.Name != "Sheet1" || s1.PhysicalType != "table" {
		t.Errorf("sheet1 = %+v", s1)
	}
	// Nullable=false -> required true (NOT NULL); Nullable=true -> required unset.
	if s1.Properties[0].Required == nil || !*s1.Properties[0].Required {
		t.Errorf("a should be required (NOT NULL): %+v", s1.Properties[0])
	}
	if s1.Properties[1].Required != nil {
		t.Errorf("b should have no required flag (nullable): %+v", s1.Properties[1])
	}
	if c.Schema[1].Properties[0].LogicalType != odcs.LogicalBoolean {
		t.Errorf("c logicalType = %q", c.Schema[1].Properties[0].LogicalType)
	}
}

// The Excel header-detection outcome rides the document as a custom
// property when the schema metadata carries it (fp2), and is absent
// otherwise, so the fingerprint's ODCS path reproduces the native one.
func TestFromDataContractHeaderFlag(t *testing.T) {
	dc := contract.DataContract{
		ID:       "workbook",
		Metadata: map[string]any{"source_format": "xlsx"},
		Schemas: []contract.SchemaContract{
			{
				Name:     "WithFlag",
				Metadata: map[string]any{"has_header": false},
				Fields:   []contract.FieldDefinition{{Name: "a", DataType: "text"}},
			},
			{
				Name:   "WithoutFlag",
				Fields: []contract.FieldDefinition{{Name: "b", DataType: "text"}},
			},
		},
	}
	c := FromDataContract(dc)
	got, ok := odcs.CustomProp(c.Schema[0].CustomProperties, odcs.CustomKeyHasHeader)
	if !ok || got != false {
		t.Errorf("WithFlag has_header = %v (present %v), want false", got, ok)
	}
	if _, ok := odcs.CustomProp(c.Schema[1].CustomProperties, odcs.CustomKeyHasHeader); ok {
		t.Error("WithoutFlag must not carry a has_header custom property")
	}
}

// TestEnumRoundTrip is the spec section 4.2 proof: emit an enum to ODCS,
// read it back, and recover the identical ordered labels and native type
// name. Arguments is free-form in ODCS, so this round-trip is owned by our
// encoder/reader pair.
func TestEnumRoundTrip(t *testing.T) {
	typeName := "account_status"
	labels := []string{"active", "closed", "pending"}

	p := EnumProperty("status", typeName, labels)

	if p.LogicalType != odcs.LogicalString {
		t.Errorf("enum logicalType = %q, want string", p.LogicalType)
	}
	if p.PhysicalType != typeName {
		t.Errorf("enum physicalType = %q, want %q", p.PhysicalType, typeName)
	}
	if len(p.Quality) != 1 {
		t.Fatalf("expected one quality rule, got %d", len(p.Quality))
	}
	q := p.Quality[0]
	if q.Type != odcs.QualityLibrary || q.Metric != odcs.MetricInvalidValues || q.Unit != "rows" {
		t.Errorf("quality rule = %+v", q)
	}
	if q.MustBe != 0 {
		t.Errorf("mustBe = %v, want 0", q.MustBe)
	}

	gotType, gotLabels, ok := ReadEnumLabels(p)
	if !ok {
		t.Fatal("ReadEnumLabels reported no enum rule")
	}
	if gotType != typeName {
		t.Errorf("recovered type = %q, want %q", gotType, typeName)
	}
	if !reflect.DeepEqual(gotLabels, labels) {
		t.Errorf("recovered labels = %v, want %v (ordered)", gotLabels, labels)
	}
}

// TestEnumRoundTripThroughYAML proves the labels survive a real ODCS YAML
// serialisation, not just the in-memory structs: this is the form a reader
// in another process actually consumes.
func TestEnumRoundTripThroughYAML(t *testing.T) {
	labels := []string{"red", "green", "blue"}
	c := odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         "x",
		Schema: []odcs.SchemaObject{
			{Name: "t", Properties: []odcs.Property{EnumProperty("color", "color_enum", labels)}},
		},
	}
	data, err := c.MarshalYAML()
	if err != nil {
		t.Fatalf("MarshalYAML: %v", err)
	}
	back, err := odcs.UnmarshalYAML(data)
	if err != nil {
		t.Fatalf("UnmarshalYAML: %v", err)
	}
	gotType, gotLabels, ok := ReadEnumLabels(back.Schema[0].Properties[0])
	if !ok {
		t.Fatal("no enum rule after YAML round-trip")
	}
	if gotType != "color_enum" || !reflect.DeepEqual(gotLabels, labels) {
		t.Errorf("after YAML: type=%q labels=%v, want color_enum %v", gotType, gotLabels, labels)
	}
}

func TestReadEnumLabelsNonEnum(t *testing.T) {
	// A plain string property carries no enum rule.
	if _, _, ok := ReadEnumLabels(propertyFromProfileType("name", "text")); ok {
		t.Error("plain text property should not read as an enum")
	}
	// A library rule missing validValues is not an enum rule.
	p := odcs.Property{
		Name: "x",
		Quality: []odcs.Quality{
			{Type: odcs.QualityLibrary, Metric: odcs.MetricInvalidValues, Arguments: map[string]any{}},
			{Type: odcs.QualitySQL, Metric: odcs.MetricNullValues},
		},
	}
	if _, _, ok := ReadEnumLabels(p); ok {
		t.Error("library rule without validValues should not read as an enum")
	}
	// validValues present but not a list.
	p2 := odcs.Property{Quality: []odcs.Quality{{
		Type: odcs.QualityLibrary, Metric: odcs.MetricInvalidValues,
		Arguments: map[string]any{"validValues": "active"},
	}}}
	if _, _, ok := ReadEnumLabels(p2); ok {
		t.Error("non-list validValues should not read as an enum")
	}
	// validValues is a list but holds a non-string: fail closed.
	p3 := odcs.Property{Quality: []odcs.Quality{{
		Type: odcs.QualityLibrary, Metric: odcs.MetricInvalidValues,
		Arguments: map[string]any{"validValues": []any{"active", 7}},
	}}}
	if _, _, ok := ReadEnumLabels(p3); ok {
		t.Error("non-string label should fail closed")
	}
}

func customVal(props []odcs.CustomProperty, key string) (any, bool) {
	for _, p := range props {
		if p.Property == key {
			return p.Value, true
		}
	}
	return nil, false
}

func TestFromSourceContractCarriesParseFacts(t *testing.T) {
	sc := csvcontract.SourceContract{
		SourcePath: "x.csv",
		Delimiter:  ";",
		Encoding:   "UTF-8",
		HasHeader:  true,
		Fields:     []csvcontract.Field{{Name: "a", DataType: profile.TypeText}},
	}
	cp := FromSourceContract(sc).Schema[0].CustomProperties
	if v, _ := customVal(cp, odcs.CustomKeySourceFormat); v != "csv" {
		t.Errorf("source format = %v, want csv", v)
	}
	if v, _ := customVal(cp, odcs.CustomKeyDelimiter); v != ";" {
		t.Errorf("delimiter = %v, want ;", v)
	}
	if v, _ := customVal(cp, odcs.CustomKeyEncoding); v != "UTF-8" {
		t.Errorf("encoding = %v", v)
	}
	if v, _ := customVal(cp, odcs.CustomKeyHasHeader); v != true {
		t.Errorf("hasHeader = %v, want true", v)
	}
}

func TestFromJSONContractCarriesParseFacts(t *testing.T) {
	for _, format := range []string{"json", "ndjson"} {
		sc := jsoncontract.SourceContract{
			SourcePath:   "x",
			SourceFormat: format,
			Encoding:     "UTF-8",
			Fields:       []jsoncontract.Field{{Name: "a", DataType: jsoncontract.TypeText}},
		}
		cp := FromJSONContract(sc).Schema[0].CustomProperties
		if v, _ := customVal(cp, odcs.CustomKeySourceFormat); v != format {
			t.Errorf("source format = %v, want %v", v, format)
		}
		if _, ok := customVal(cp, odcs.CustomKeyDelimiter); ok {
			t.Errorf("%s should carry no delimiter", format)
		}
	}
}

func TestFromDataContractFormatMarkers(t *testing.T) {
	cases := []struct {
		name     string
		metadata map[string]any
		want     string
	}{
		{"xlsx", map[string]any{"source_format": "xlsx"}, "xlsx"},
		{"openapi", map[string]any{"source": "openapi"}, "api"},
		{"none", nil, ""},
		{"unknown", map[string]any{"source_format": "weird"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dc := contract.DataContract{
				ID:       "d",
				Metadata: c.metadata,
				Schemas: []contract.SchemaContract{
					{Name: "S", Fields: []contract.FieldDefinition{{Name: "a", DataType: "text"}}},
				},
			}
			cp := FromDataContract(dc).Schema[0].CustomProperties
			if v, _ := customVal(cp, odcs.CustomKeySourceFormat); v != c.want {
				t.Errorf("format = %v, want %v", v, c.want)
			}
		})
	}
}
