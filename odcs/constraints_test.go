package odcs_test

import (
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
)

// TestUniqueConstraintsRoundTrip: a table's UNIQUE constraints written by
// UniqueConstraintsProperty read back identically through UniqueConstraints,
// and an empty list writes nothing (ok=false) so a constraint-free table
// carries no custom property.
func TestUniqueConstraintsRoundTrip(t *testing.T) {
	uniques := []odcs.UniqueConstraint{
		{Name: "u_isin", Columns: []string{"isin"}},
		{Name: "u_pair", Columns: []string{"account_id", "date"}},
	}
	prop, ok := odcs.UniqueConstraintsProperty(uniques)
	if !ok {
		t.Fatal("UniqueConstraintsProperty(non-empty) = ok=false, want true")
	}
	if prop.Property != odcs.CustomKeyUniqueConstraints {
		t.Errorf("key = %q, want %q", prop.Property, odcs.CustomKeyUniqueConstraints)
	}
	got := odcs.UniqueConstraints([]odcs.CustomProperty{prop})
	if !reflect.DeepEqual(got, uniques) {
		t.Errorf("round-trip = %+v, want %+v", got, uniques)
	}

	if _, ok := odcs.UniqueConstraintsProperty(nil); ok {
		t.Error("UniqueConstraintsProperty(nil) = ok=true, want false (no custom property for a constraint-free table)")
	}
}

// TestCheckConstraintsRoundTrip: CHECK constraints round-trip verbatim, and an
// empty list writes nothing.
func TestCheckConstraintsRoundTrip(t *testing.T) {
	checks := []odcs.CheckConstraint{
		{Name: "c_amount", Expression: "amount >= 0"},
		{Name: "c_type", Expression: "type IN ('buy', 'sell')"},
	}
	prop, ok := odcs.CheckConstraintsProperty(checks)
	if !ok {
		t.Fatal("CheckConstraintsProperty(non-empty) = ok=false, want true")
	}
	if prop.Property != odcs.CustomKeyCheckConstraints {
		t.Errorf("key = %q, want %q", prop.Property, odcs.CustomKeyCheckConstraints)
	}
	got := odcs.CheckConstraints([]odcs.CustomProperty{prop})
	if !reflect.DeepEqual(got, checks) {
		t.Errorf("round-trip = %+v, want %+v", got, checks)
	}

	if _, ok := odcs.CheckConstraintsProperty(nil); ok {
		t.Error("CheckConstraintsProperty(nil) = ok=true, want false")
	}
}

// TestForeignKeysRoundTrip: the generic structural triple round-trips, and an
// empty list writes nothing.
func TestForeignKeysRoundTrip(t *testing.T) {
	fks := []odcs.ForeignKey{
		{Column: "asset_id", ReferencedTable: "assets", ReferencedColumn: "id"},
	}
	prop, ok := odcs.ForeignKeysProperty(fks)
	if !ok {
		t.Fatal("ForeignKeysProperty(non-empty) = ok=false, want true")
	}
	if prop.Property != odcs.CustomKeyForeignKeys {
		t.Errorf("key = %q, want %q", prop.Property, odcs.CustomKeyForeignKeys)
	}
	got := odcs.ForeignKeys([]odcs.CustomProperty{prop})
	if !reflect.DeepEqual(got, fks) {
		t.Errorf("round-trip = %+v, want %+v", got, fks)
	}

	if _, ok := odcs.ForeignKeysProperty(nil); ok {
		t.Error("ForeignKeysProperty(nil) = ok=true, want false")
	}
}

// TestForeignKeysIgnoresLayeredKeys: a foreign-key map a delivery layer
// enriched with its own resolution keys (matchColumns, attested, constants)
// reads back as the generic triple only — the reader owns just the structural
// fields and ignores what another owner layered onto the same map.
func TestForeignKeysIgnoresLayeredKeys(t *testing.T) {
	enriched := odcs.CustomProperty{
		Property: odcs.CustomKeyForeignKeys,
		Value: []any{
			map[string]any{
				"column":           "asset_id",
				"referencedTable":  "assets",
				"referencedColumn": "id",
				"matchColumns":     []any{"isin"},
				"attested":         true,
				"constants":        []any{map[string]any{"column": "source", "value": "csn"}},
			},
		},
	}
	got := odcs.ForeignKeys([]odcs.CustomProperty{enriched})
	want := []odcs.ForeignKey{{Column: "asset_id", ReferencedTable: "assets", ReferencedColumn: "id"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ForeignKeys(enriched) = %+v, want %+v (structural triple only)", got, want)
	}
}

// TestConstraintReadersAbsent: reading from custom properties that carry no
// constraint key returns nil for every reader.
func TestConstraintReadersAbsent(t *testing.T) {
	props := []odcs.CustomProperty{{Property: "dcgSourceFormat", Value: "csv"}}
	if got := odcs.UniqueConstraints(props); got != nil {
		t.Errorf("UniqueConstraints(absent) = %+v, want nil", got)
	}
	if got := odcs.CheckConstraints(props); got != nil {
		t.Errorf("CheckConstraints(absent) = %+v, want nil", got)
	}
	if got := odcs.ForeignKeys(props); got != nil {
		t.Errorf("ForeignKeys(absent) = %+v, want nil", got)
	}
}

// TestConstraintReadersMalformed: a custom property whose value is not the
// expected list shape, and a list entry that is not a map, are both read
// defensively — a non-list value yields nil, a non-map entry is skipped.
func TestConstraintReadersMalformed(t *testing.T) {
	// Value is not a []any at all.
	scalar := func(key string) []odcs.CustomProperty {
		return []odcs.CustomProperty{{Property: key, Value: "not-a-list"}}
	}
	if got := odcs.UniqueConstraints(scalar(odcs.CustomKeyUniqueConstraints)); got != nil {
		t.Errorf("UniqueConstraints(non-list) = %+v, want nil", got)
	}
	if got := odcs.CheckConstraints(scalar(odcs.CustomKeyCheckConstraints)); got != nil {
		t.Errorf("CheckConstraints(non-list) = %+v, want nil", got)
	}
	if got := odcs.ForeignKeys(scalar(odcs.CustomKeyForeignKeys)); got != nil {
		t.Errorf("ForeignKeys(non-list) = %+v, want nil", got)
	}

	// A list whose single entry is not a map is skipped, leaving nil.
	badEntry := func(key string) []odcs.CustomProperty {
		return []odcs.CustomProperty{{Property: key, Value: []any{"not-a-map"}}}
	}
	if got := odcs.UniqueConstraints(badEntry(odcs.CustomKeyUniqueConstraints)); got != nil {
		t.Errorf("UniqueConstraints(bad entry) = %+v, want nil", got)
	}
	if got := odcs.CheckConstraints(badEntry(odcs.CustomKeyCheckConstraints)); got != nil {
		t.Errorf("CheckConstraints(bad entry) = %+v, want nil", got)
	}
	if got := odcs.ForeignKeys(badEntry(odcs.CustomKeyForeignKeys)); got != nil {
		t.Errorf("ForeignKeys(bad entry) = %+v, want nil", got)
	}
}

// TestUniqueConstraintsDecodedForm: a constraint decoded from YAML/JSON arrives
// as a map with the column list as []any of strings; the reader coerces it back
// to []string. A column list with a non-string element reads as nil columns
// (fail-closed on a malformed list).
func TestUniqueConstraintsDecodedForm(t *testing.T) {
	decoded := odcs.CustomProperty{
		Property: odcs.CustomKeyUniqueConstraints,
		Value: []any{
			map[string]any{"name": "u", "columns": []any{"a", "b"}},
			map[string]any{"name": "bad", "columns": []any{"a", 7}},
			// A columns list that is a native []string (the in-memory build,
			// not the decoded []any form) is coerced through unchanged.
			map[string]any{"name": "native", "columns": []string{"x"}},
			// A columns value that is not a list at all (a scalar) reads as nil.
			map[string]any{"name": "scalar", "columns": "oops"},
		},
	}
	got := odcs.UniqueConstraints([]odcs.CustomProperty{decoded})
	want := []odcs.UniqueConstraint{
		{Name: "u", Columns: []string{"a", "b"}},
		{Name: "bad", Columns: nil},
		{Name: "native", Columns: []string{"x"}},
		{Name: "scalar", Columns: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("UniqueConstraints(decoded) = %+v, want %+v", got, want)
	}
}
