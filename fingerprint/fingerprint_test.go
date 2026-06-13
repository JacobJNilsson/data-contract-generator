package fingerprint

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/jsoncontract"
	"github.com/JacobJNilsson/data-contract-generator/profile"
)

func csvContract(fields ...csvcontract.Field) *csvcontract.SourceContract {
	return &csvcontract.SourceContract{
		SourceFormat: "csv",
		Encoding:     "utf-8",
		Delimiter:    ",",
		HasHeader:    true,
		TotalRows:    100,
		Fields:       fields,
	}
}

func csvField(name string, dataType profile.DataType) csvcontract.Field {
	return csvcontract.Field{Name: name, DataType: dataType}
}

func jsonContract(sourceFormat string, fields ...jsoncontract.Field) *jsoncontract.SourceContract {
	return &jsoncontract.SourceContract{
		SourceFormat: sourceFormat,
		Encoding:     "utf-8",
		TotalRows:    50,
		Fields:       fields,
	}
}

func jsonField(name string, dataType jsoncontract.DataType) jsoncontract.Field {
	return jsoncontract.Field{Name: name, DataType: dataType}
}

func mustCSV(t *testing.T, sc *csvcontract.SourceContract) Object {
	t.Helper()
	o, err := FromCSV(sc)
	if err != nil {
		t.Fatalf("FromCSV: %v", err)
	}
	return o
}

func mustJSON(t *testing.T, sc *jsoncontract.SourceContract) Object {
	t.Helper()
	o, err := FromJSON(sc)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	return o
}

// Acceptance: same file hashed twice yields the identical hash. Platform
// independence holds by construction: canonicalisation never consults map
// order, locale, time, or environment.
func TestDeterminism(t *testing.T) {
	build := func() Object {
		return mustCSV(t, csvContract(
			csvField("order_id", profile.TypeText),
			csvField("amount", profile.TypeNumeric),
			csvField("placed_at", profile.TypeDate),
		))
	}
	first, second := build(), build()
	if first.Hash() != second.Hash() {
		t.Errorf("hashes differ across builds: %s vs %s", first.Hash(), second.Hash())
	}
	repeat := first.Hash()
	if repeat != first.Hash() {
		t.Errorf("hash is not stable on repeat calls")
	}
	if !Match(first, second) {
		t.Errorf("identical objects do not Match")
	}
}

// Acceptance: reordered named columns, more rows, and a different sample of
// the same shape all yield the identical hash.
func TestStability(t *testing.T) {
	base := mustCSV(t, csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeNumeric),
	))

	reordered := csvContract(
		csvField("b", profile.TypeNumeric),
		csvField("a", profile.TypeText),
	)
	if got := mustCSV(t, reordered).Hash(); got != base.Hash() {
		t.Errorf("reordered named columns changed hash: %s vs %s", got, base.Hash())
	}

	bigger := csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeNumeric),
	)
	bigger.TotalRows = 1_000_000
	bigger.SampleData = [][]string{{"x", "1"}}
	bigger.Fields[0].Profile = profile.FieldProfile{TotalCount: 1_000_000, DistinctCount: 999}
	if got := mustCSV(t, bigger).Hash(); got != base.Hash() {
		t.Errorf("row count / sample / profiling changed hash: %s vs %s", got, base.Hash())
	}
}

// Acceptance: added field, removed field, type change, delimiter change, and
// header toggle each yield a different hash.
func TestSensitivity(t *testing.T) {
	base := mustCSV(t, csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeNumeric),
	))

	variants := map[string]*csvcontract.SourceContract{
		"added field": csvContract(
			csvField("a", profile.TypeText),
			csvField("b", profile.TypeNumeric),
			csvField("c", profile.TypeText),
		),
		"removed field": csvContract(
			csvField("a", profile.TypeText),
		),
		"type change": csvContract(
			csvField("a", profile.TypeText),
			csvField("b", profile.TypeText),
		),
		"renamed field": csvContract(
			csvField("a", profile.TypeText),
			csvField("B", profile.TypeNumeric),
		),
	}
	delimiterChanged := csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeNumeric),
	)
	delimiterChanged.Delimiter = ";"
	variants["delimiter change"] = delimiterChanged

	headerToggled := csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeNumeric),
	)
	headerToggled.HasHeader = false
	variants["header toggle"] = headerToggled

	encodingChanged := csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeNumeric),
	)
	encodingChanged.Encoding = "windows-1252"
	variants["encoding change"] = encodingChanged

	for name, variant := range variants {
		got := mustCSV(t, variant)
		if got.Hash() == base.Hash() {
			t.Errorf("%s: hash unchanged", name)
		}
		if Match(base, got) {
			t.Errorf("%s: objects still Match", name)
		}
	}
}

// Acceptance: a numeric column with and without decimals maps to the same
// coarse NUMBER token, so profiling differences never reach identity.
func TestTypeStability(t *testing.T) {
	min1, max1 := "1", "9"
	withInts := csvContract(csvField("amount", profile.TypeNumeric))
	withInts.Fields[0].Profile = profile.FieldProfile{MinValue: &min1, MaxValue: &max1}

	min2, max2 := "0.25", "99.99"
	withDecimals := csvContract(csvField("amount", profile.TypeNumeric))
	withDecimals.Fields[0].Profile = profile.FieldProfile{MinValue: &min2, MaxValue: &max2}

	if mustCSV(t, withInts).Hash() != mustCSV(t, withDecimals).Hash() {
		t.Errorf("decimal vs integer numeric profiles changed hash")
	}
}

// Acceptance: a column null in one file and valued in another, type
// otherwise known, yields the identical hash — nullability is excluded.
func TestNullStability(t *testing.T) {
	noNulls := jsonContract("json", jsonField("name", jsoncontract.TypeText))
	noNulls.Fields[0].Profile = jsoncontract.FieldProfile{TotalCount: 50, NullCount: 0}

	someNulls := jsonContract("json", jsonField("name", jsoncontract.TypeText))
	someNulls.Fields[0].Profile = jsoncontract.FieldProfile{TotalCount: 50, NullCount: 20}

	if mustJSON(t, noNulls).Hash() != mustJSON(t, someNulls).Hash() {
		t.Errorf("null counts changed hash")
	}
}

// Acceptance: an all-null column later resolving to a real type yields a
// different hash — a documented miss, not a silent re-route.
// Every remaining analyzer token maps onto the lattice.
func TestTypeLattice(t *testing.T) {
	o := mustJSON(t, jsonContract("json",
		jsonField("nested", jsoncontract.TypeObject),
		jsonField("list", jsoncontract.TypeArray),
		jsonField("blank", jsoncontract.TypeEmpty),
		jsonField("flag", jsoncontract.TypeBoolean),
		jsonField("when", jsoncontract.TypeDate),
	))
	want := map[string]CanonicalType{
		"nested": TypeObject, "list": TypeArray, "blank": TypeUnknown,
		"flag": TypeBoolean, "when": TypeTemporal,
	}
	for _, f := range o.Fields {
		if want[f.Name] != f.Type {
			t.Errorf("field %q mapped to %q, want %q", f.Name, f.Type, want[f.Name])
		}
	}
}

func TestUnknownResolution(t *testing.T) {
	allNull := mustJSON(t, jsonContract("json", jsonField("status", jsoncontract.TypeNull)))
	resolved := mustJSON(t, jsonContract("json", jsonField("status", jsoncontract.TypeText)))
	if allNull.Hash() == resolved.Hash() {
		t.Errorf("UNKNOWN resolving to STRING did not change hash")
	}
}

// Acceptance (collision guard): a cache hit requires hash equality AND
// object deep-equality, so structurally different objects must never Match,
// and every structural facet participates in the comparison.
func TestCollisionGuardMatch(t *testing.T) {
	base := mustCSV(t, csvContract(csvField("a", profile.TypeText)))

	same := mustCSV(t, csvContract(csvField("a", profile.TypeText)))
	if !Match(base, same) {
		t.Fatalf("structurally identical objects do not Match")
	}

	differentVersion := same
	differentVersion.AlgoVersion = "fp2"
	if Match(base, differentVersion) {
		t.Errorf("differing algo_version still Matches")
	}

	differentFormat := same
	differentFormat.Format = FormatJSON
	if Match(base, differentFormat) {
		t.Errorf("differing format still Matches")
	}

	differentFieldCount := mustCSV(t, csvContract(
		csvField("a", profile.TypeText),
		csvField("b", profile.TypeText),
	))
	if Match(base, differentFieldCount) {
		t.Errorf("differing field count still Matches")
	}

	differentField := mustCSV(t, csvContract(csvField("a", profile.TypeNumeric)))
	if Match(base, differentField) {
		t.Errorf("differing field type still Matches")
	}

	differentProfile := mustCSV(t, csvContract(csvField("a", profile.TypeText)))
	differentProfile.ParseProfile.HasHeader = nil
	if Match(base, differentProfile) {
		t.Errorf("differing parse profile still Matches")
	}

	noProfile := same
	noProfile.ParseProfile = nil
	if Match(base, noProfile) {
		t.Errorf("nil vs non-nil parse profile still Matches")
	}
	if !Match(noProfile, noProfile) {
		t.Errorf("nil parse profiles do not Match themselves")
	}

	withNesting := same
	withNesting.Nesting = json.RawMessage(`{"depth":1}`)
	if Match(base, withNesting) {
		t.Errorf("differing nesting still Matches")
	}
}

// The canonical form is plain RFC 8259 JSON: no Go-specific HTML escaping,
// so the hash is reproducible by any JSON implementation.
func TestCanonicalBytesPlainJSON(t *testing.T) {
	o := mustCSV(t, csvContract(csvField("a<b&c>", profile.TypeText)))
	canonical := string(o.CanonicalBytes())
	if strings.Contains(canonical, `\u003c`) || strings.Contains(canonical, `\u0026`) {
		t.Errorf("canonical bytes use Go HTML escaping: %s", canonical)
	}
	if !strings.Contains(canonical, `"a<b&c>"`) {
		t.Errorf("canonical bytes missing plain-JSON name: %s", canonical)
	}
}

// Acceptance: a 3-schema contract fans out into 3 units with 3 fingerprints.
func TestMultiSchemaFanout(t *testing.T) {
	dc := &contract.DataContract{
		ContractType: "source",
		Metadata:     map[string]any{"source_format": "xlsx"},
		Schemas: []contract.SchemaContract{
			{Name: "Orders", Fields: []contract.FieldDefinition{{Name: "id", DataType: "text"}}},
			{Name: "Customers", Fields: []contract.FieldDefinition{{Name: "id", DataType: "numeric"}}},
			{Name: "Items", Fields: []contract.FieldDefinition{{Name: "sku", DataType: "text"}, {Name: "qty", DataType: "numeric"}}},
		},
	}
	units, skipped, err := FromDataContract(dc)
	if err != nil {
		t.Fatalf("FromDataContract: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("unexpected skipped schemas: %v", skipped)
	}
	if len(units) != 3 {
		t.Fatalf("expected 3 units, got %d", len(units))
	}
	hashes := map[string]bool{}
	locators := map[string]bool{}
	for _, u := range units {
		hashes[u.Object.Hash()] = true
		locators[u.Locator] = true
		if u.Object.Format != FormatXLSX {
			t.Errorf("unit %q format = %q, want xlsx", u.Locator, u.Object.Format)
		}
	}
	if len(hashes) != 3 {
		t.Errorf("expected 3 distinct fingerprints, got %d", len(hashes))
	}
	if !locators["Orders"] || !locators["Customers"] || !locators["Items"] {
		t.Errorf("locators not taken from sheet names: %v", locators)
	}
}

func TestFromDataContractAPI(t *testing.T) {
	dc := &contract.DataContract{
		Metadata: map[string]any{"source": "openapi"},
		Schemas: []contract.SchemaContract{
			{Name: "GET /users", Fields: []contract.FieldDefinition{
				{Name: "id", DataType: "integer"},
				{Name: "email", DataType: "text"},
				{Name: "created_at", DataType: "timestamptz"},
				{Name: "active", DataType: "boolean"},
				{Name: "tags", DataType: "array[text]"},
				{Name: "settings", DataType: "jsonb"},
				{Name: "ref", DataType: "uuid"},
				{Name: "blob", DataType: "bytea"},
			}},
		},
	}
	units, skipped, err := FromDataContract(dc)
	if err != nil {
		t.Fatalf("FromDataContract: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("unexpected skipped schemas: %v", skipped)
	}
	if units[0].Object.Format != FormatAPI {
		t.Errorf("format = %q, want api", units[0].Object.Format)
	}
	want := map[string]CanonicalType{
		"id": TypeNumber, "email": TypeString, "created_at": TypeTemporal,
		"active": TypeBoolean, "tags": "ARRAY<STRING>", "settings": TypeObject,
		"ref": TypeString, "blob": TypeBinary,
	}
	for _, f := range units[0].Object.Fields {
		if want[f.Name] != f.Type {
			t.Errorf("field %q mapped to %q, want %q", f.Name, f.Type, want[f.Name])
		}
	}
}

// The canonical form is pinned: this is the documented serialization other
// components (and future algo versions) are measured against.
func TestCanonicalBytesGolden(t *testing.T) {
	o := mustCSV(t, csvContract(
		csvField("amount", profile.TypeNumeric),
		csvField("order_id", profile.TypeText),
	))
	want := `{"algo_version":"fp1","fields":[{"name":"amount","type":"NUMBER"},{"name":"order_id","type":"STRING"}],"format":"csv","nesting":null,"parse_profile":{"delimiter":",","encoding":"utf-8","has_header":true}}`
	if got := string(o.CanonicalBytes()); got != want {
		t.Errorf("canonical bytes:\n got %s\nwant %s", got, want)
	}
	if !strings.HasPrefix(o.Hash(), "fp1:") {
		t.Errorf("hash %q lacks algo version prefix", o.Hash())
	}

	jsonObject := mustJSON(t, jsonContract("ndjson", jsonField("a", jsoncontract.TypeText)))
	wantJSON := `{"algo_version":"fp1","fields":[{"name":"a","type":"STRING"}],"format":"ndjson","nesting":null,"parse_profile":{"delimiter":null,"encoding":"utf-8","has_header":null}}`
	if got := string(jsonObject.CanonicalBytes()); got != wantJSON {
		t.Errorf("ndjson canonical bytes:\n got %s\nwant %s", got, wantJSON)
	}

	bare := Object{AlgoVersion: AlgoVersion, Format: FormatXLSX, Fields: []Field{{Name: "x", Type: TypeString}}}
	wantBare := `{"algo_version":"fp1","fields":[{"name":"x","type":"STRING"}],"format":"xlsx","nesting":null,"parse_profile":null}`
	if got := string(bare.CanonicalBytes()); got != wantBare {
		t.Errorf("bare canonical bytes:\n got %s\nwant %s", got, wantBare)
	}
}

// Field names normalize to NFC with surrounding whitespace trimmed, so
// byte-level encoding noise is absorbed while real renames still miss.
func TestNameCanonicalisation(t *testing.T) {
	nfd := mustCSV(t, csvContract(csvField("café ", profile.TypeText)))
	nfc := mustCSV(t, csvContract(csvField("café", profile.TypeText)))
	if nfd.Hash() != nfc.Hash() {
		t.Errorf("NFC/trim noise changed hash")
	}

	upper := mustCSV(t, csvContract(csvField("CAFÉ", profile.TypeText)))
	if upper.Hash() == nfc.Hash() {
		t.Errorf("case change did not change hash; case must be preserved")
	}
}

// Duplicate canonical names degrade to positional identity: the name→type
// pairing stays in the hash, so two files with swapped column types can
// never share a fingerprint (the catastrophic false positive), and
// reordering ambiguous columns is a miss, like headerless CSV.
func TestDuplicateNamePositionalIdentity(t *testing.T) {
	textFirst := mustCSV(t, csvContract(
		csvField("a", profile.TypeText),
		csvField("a ", profile.TypeNumeric),
	))
	rebuilt := mustCSV(t, csvContract(
		csvField("a", profile.TypeText),
		csvField("a ", profile.TypeNumeric),
	))
	if textFirst.Hash() != rebuilt.Hash() {
		t.Errorf("duplicate-name fingerprint is not deterministic")
	}

	typesSwapped := mustCSV(t, csvContract(
		csvField("a", profile.TypeNumeric),
		csvField("a ", profile.TypeText),
	))
	if typesSwapped.Hash() == textFirst.Hash() {
		t.Errorf("swapped types behind duplicate names share a hash: false positive")
	}
	if Match(textFirst, typesSwapped) {
		t.Errorf("swapped types behind duplicate names still Match")
	}

	wantNames := []string{"a#1", "a#2"}
	for i, f := range textFirst.Fields {
		if f.Name != wantNames[i] {
			t.Errorf("field %d name = %q, want %q", i, f.Name, wantNames[i])
		}
	}
}

// Literal '#' in headers is escaped, so a real header can never collide
// with a synthesized disambiguation suffix.
func TestHashSuffixNamespaceIsPrivate(t *testing.T) {
	literal := mustCSV(t, csvContract(
		csvField("x#1", profile.TypeText),
		csvField("x#2", profile.TypeNumeric),
	))
	synthesized := mustCSV(t, csvContract(
		csvField("x", profile.TypeText),
		csvField("x", profile.TypeNumeric),
	))
	if literal.Hash() == synthesized.Hash() {
		t.Errorf("literal #-headers collide with synthesized duplicate suffixes")
	}
}

func TestErrors(t *testing.T) {
	if _, err := FromCSV(nil); err == nil {
		t.Errorf("nil CSV contract: expected error")
	}
	if _, err := FromJSON(nil); err == nil {
		t.Errorf("nil JSON contract: expected error")
	}
	if _, _, err := FromDataContract(nil); err == nil {
		t.Errorf("nil data contract: expected error")
	}
	if _, err := FromCSV(csvContract()); err == nil {
		t.Errorf("zero fields: expected error")
	}
	if _, err := FromJSON(jsonContract("xml", jsonField("a", jsoncontract.TypeText))); err == nil {
		t.Errorf("unsupported JSON source format: expected error")
	}
	if _, err := FromJSON(jsonContract("json")); err == nil {
		t.Errorf("JSON zero fields: expected error")
	}
	if _, err := FromCSV(csvContract(csvField("a", profile.DataType("mystery")))); err == nil {
		t.Errorf("unmapped type: expected error")
	}
	if _, _, err := FromDataContract(&contract.DataContract{Metadata: map[string]any{}}); err == nil {
		t.Errorf("undetectable format: expected error")
	}
	if _, _, err := FromDataContract(&contract.DataContract{Metadata: map[string]any{"source_format": "xlsx"}}); err == nil {
		t.Errorf("no schemas: expected error")
	}
	destination := &contract.DataContract{
		ContractType: "destination",
		Metadata:     map[string]any{"source": "openapi"},
		Schemas:      []contract.SchemaContract{{Name: "t", Fields: []contract.FieldDefinition{{Name: "x", DataType: "text"}}}},
	}
	if _, _, err := FromDataContract(destination); err == nil {
		t.Errorf("destination contract: expected error")
	}
	if _, err := FromCSV(csvContract(csvField("a", "array[text"))); err == nil {
		t.Errorf("malformed array type: expected error")
	}
	if _, err := FromCSV(csvContract(csvField("a", "array[]"))); err == nil {
		t.Errorf("empty array element: expected error")
	}
	if _, err := FromCSV(csvContract(csvField("a", "array[mystery]"))); err == nil {
		t.Errorf("unmapped array element: expected error")
	}
}

// One bad schema must not cost the other units their fingerprints: the
// failing sheet/endpoint is reported as skipped, the rest fan out normally.
func TestFanoutIsolatesBadSchemas(t *testing.T) {
	dc := &contract.DataContract{
		Metadata: map[string]any{"source": "openapi"},
		Schemas: []contract.SchemaContract{
			{Name: "GET /good", Fields: []contract.FieldDefinition{{Name: "id", DataType: "integer"}}},
			{Name: "GET /bad", Fields: []contract.FieldDefinition{{Name: "avatar", DataType: "file"}}},
			{Name: "GET /also-good", Fields: []contract.FieldDefinition{{Name: "name", DataType: "text"}}},
		},
	}
	units, skipped, err := FromDataContract(dc)
	if err != nil {
		t.Fatalf("FromDataContract: %v", err)
	}
	if len(units) != 2 {
		t.Fatalf("expected 2 units, got %d", len(units))
	}
	if len(skipped) != 1 || skipped[0].Locator != "GET /bad" || skipped[0].Err == nil {
		t.Fatalf("expected GET /bad skipped with error, got %+v", skipped)
	}
}

// Nested array element types stay identity-bearing as far as the analyzer
// exposes them.
func TestNestedArrayTypes(t *testing.T) {
	dc := &contract.DataContract{
		Metadata: map[string]any{"source": "openapi"},
		Schemas: []contract.SchemaContract{
			{Name: "GET /matrix", Fields: []contract.FieldDefinition{{Name: "grid", DataType: "array[array[integer]]"}}},
		},
	}
	units, skipped, err := FromDataContract(dc)
	if err != nil || len(skipped) != 0 {
		t.Fatalf("FromDataContract: err=%v skipped=%v", err, skipped)
	}
	if got := units[0].Object.Fields[0].Type; got != "ARRAY<ARRAY<NUMBER>>" {
		t.Errorf("nested array type = %q, want ARRAY<ARRAY<NUMBER>>", got)
	}

	stringArrays := &contract.DataContract{
		Metadata: map[string]any{"source": "openapi"},
		Schemas: []contract.SchemaContract{
			{Name: "GET /tags", Fields: []contract.FieldDefinition{{Name: "tags", DataType: "array[text]"}}},
		},
	}
	intArrays := &contract.DataContract{
		Metadata: map[string]any{"source": "openapi"},
		Schemas: []contract.SchemaContract{
			{Name: "GET /tags", Fields: []contract.FieldDefinition{{Name: "tags", DataType: "array[integer]"}}},
		},
	}
	su, _, _ := FromDataContract(stringArrays)
	iu, _, _ := FromDataContract(intArrays)
	if su[0].Object.Hash() == iu[0].Object.Hash() {
		t.Errorf("array element type change did not change hash")
	}
}
