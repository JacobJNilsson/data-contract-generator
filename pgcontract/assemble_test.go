package pgcontract

// Fast-tier unit tests for the PURE assembly core: the exclusion filters, the
// per-column mapping, and assemble itself all run on hand-built catalog facts
// with no database, so the fast tier exercises the contract-shaping logic.
// Generate and resolveEnumLabels (which issue live queries) are exercised in
// the integration tier.

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
	"github.com/JacobJNilsson/data-contract-generator/pgintrospect"
)

// col builds a minimal catalog Column with the fields the faithful mapping
// reads; unset fields default to their zero value (a non-array, non-numeric,
// unbounded scalar).
func col(table, name, dataType, nullable string) pgintrospect.Column {
	return pgintrospect.Column{Table: table, Name: name, DataType: dataType, Nullable: nullable, ElementTypeMod: -1}
}

func nullInt64(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }

func TestNullInt(t *testing.T) {
	if got := nullInt(sql.NullInt64{}); got != nil {
		t.Errorf("nullInt(NULL) = %v, want nil", got)
	}
	got := nullInt(nullInt64(12))
	if got == nil || *got != 12 {
		t.Errorf("nullInt(12) = %v, want *12", got)
	}
}

func TestToSetAndAnyExcluded(t *testing.T) {
	if len(toSet(nil)) != 0 {
		t.Errorf("toSet(nil) is not empty")
	}
	set := toSet([]string{"a", "b"})
	if !anyExcluded([]string{"x", "b"}, set) {
		t.Errorf("anyExcluded should report b excluded")
	}
	if anyExcluded([]string{"x", "y"}, set) {
		t.Errorf("anyExcluded should report none excluded")
	}
}

func TestIncludedTables(t *testing.T) {
	got := includedTables([]string{"a", "skip", "b"}, toSet([]string{"skip"}))
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("includedTables = %v, want [a b] in order", got)
	}
}

func TestIncludedPrimaryKeys(t *testing.T) {
	raw := map[string][]string{"keep": {"id"}, "drop": {"id", "ext"}}
	got := includedPrimaryKeys(raw, toSet([]string{"ext"}))
	if _, ok := got["drop"]; ok {
		t.Errorf("primary key over excluded column was kept: %v", got)
	}
	if want := []string{"id"}; len(got["keep"]) != 1 || got["keep"][0] != want[0] {
		t.Errorf("keep primary key = %v, want %v", got["keep"], want)
	}
}

func TestIncludedUniquesAndChecks(t *testing.T) {
	uniques := includedUniques([]pgintrospect.UniqueConstraint{
		{Table: "t", Name: "u_keep", Columns: []string{"a"}},
		{Table: "t", Name: "u_drop", Columns: []string{"a", "ext"}},
	}, toSet([]string{"ext"}))
	if len(uniques["t"]) != 1 || uniques["t"][0].Name != "u_keep" {
		t.Errorf("includedUniques = %+v, want only u_keep", uniques)
	}

	checks := includedChecks([]pgintrospect.CheckConstraint{
		{Table: "t", Name: "ck_keep", Expression: "a > 0", Columns: []string{"a"}},
		{Table: "t", Name: "ck_drop", Expression: "ext > 0", Columns: []string{"ext"}},
		{Table: "t", Name: "ck_nocol", Expression: "true"},
	}, toSet([]string{"ext"}))
	if len(checks["t"]) != 2 {
		t.Fatalf("includedChecks = %+v, want ck_keep and ck_nocol (ck_drop dropped)", checks)
	}
	if checks["t"][0].Name != "ck_keep" || checks["t"][0].Expression != "a > 0" || checks["t"][1].Name != "ck_nocol" {
		t.Errorf("includedChecks kept the wrong checks / lost the verbatim expression: %+v", checks["t"])
	}
}

func TestSingleColumnForeignKeys(t *testing.T) {
	got := singleColumnForeignKeys([]pgintrospect.ForeignKey{
		{Table: "orders", Name: "fk_single", NumColumns: 1, Columns: []string{"asset_id"}, ReferencedTable: "assets", ReferencedColumns: []string{"id"}},
		{Table: "orders", Name: "fk_composite", NumColumns: 2, Columns: []string{"a", "b"}, ReferencedTable: "refs", ReferencedColumns: []string{"x", "y"}},
		{Table: "orders", Name: "fk_excluded", NumColumns: 1, Columns: []string{"ext"}, ReferencedTable: "refs", ReferencedColumns: []string{"id"}},
	}, toSet([]string{"ext"}))
	if len(got["orders"]) != 1 {
		t.Fatalf("singleColumnForeignKeys = %+v, want only the single-column, non-excluded FK", got)
	}
	fk := got["orders"][0]
	if fk.Column != "asset_id" || fk.ReferencedTable != "assets" || fk.ReferencedColumn != "id" {
		t.Errorf("FK = %+v, want asset_id -> assets(id) (structural triple only)", fk)
	}
}

func TestMapColumnScalarAndWidths(t *testing.T) {
	cases := []struct {
		name     string
		column   pgintrospect.Column
		wantDDL  string
		nullable bool
	}{
		{"bigint not null", col("t", "id", "bigint", "NO"), "bigint", false},
		{"text nullable", col("t", "note", "text", "YES"), "text", true},
		{"boolean", col("t", "ok", "boolean", "YES"), "boolean", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mapColumn(tc.column, nil)
			if err != nil {
				t.Fatalf("mapColumn() = %v", err)
			}
			if ddl, ok := odcsdest.PostgresDDLType(got); !ok || ddl != tc.wantDDL {
				t.Errorf("DDL = (%q,%v), want %q", ddl, ok, tc.wantDDL)
			}
			if odcsdest.Nullable(got) != tc.nullable {
				t.Errorf("nullable = %v, want %v (RAW destination nullability)", odcsdest.Nullable(got), tc.nullable)
			}
		})
	}
}

func TestMapColumnVarcharCarriesLength(t *testing.T) {
	c := col("t", "code", "character varying", "NO")
	c.CharMaxLength = nullInt64(3)
	got, err := mapColumn(c, nil)
	if err != nil {
		t.Fatalf("mapColumn() = %v", err)
	}
	if ddl, ok := odcsdest.PostgresDDLType(got); !ok || ddl != "varchar(3)" {
		t.Errorf("DDL = (%q,%v), want varchar(3)", ddl, ok)
	}
	if w, ok := odcsdest.MaxCharacterLength(got); !ok || w != 3 {
		t.Errorf("width = (%d,%v), want 3", w, ok)
	}
}

func TestMapColumnNumericPrecisionScale(t *testing.T) {
	c := col("t", "amount", "numeric", "NO")
	c.NumericPrecision = nullInt64(12)
	c.NumericScale = nullInt64(2)
	got, err := mapColumn(c, nil)
	if err != nil {
		t.Fatalf("mapColumn() = %v", err)
	}
	if ddl, ok := odcsdest.PostgresDDLType(got); !ok || ddl != "numeric(12,2)" {
		t.Errorf("DDL = (%q,%v), want numeric(12,2)", ddl, ok)
	}
}

func TestMapColumnArray(t *testing.T) {
	c := col("t", "tags", "ARRAY", "YES")
	c.ElementDataType = "text"
	c.ArrayDims = 1
	got, err := mapColumn(c, nil)
	if err != nil {
		t.Fatalf("mapColumn() = %v", err)
	}
	if ddl, ok := odcsdest.PostgresDDLType(got); !ok || ddl != "text[]" {
		t.Errorf("DDL = (%q,%v), want text[]", ddl, ok)
	}
}

func TestMapColumnEnum(t *testing.T) {
	c := col("t", "status", "USER-DEFINED", "NO")
	c.UDTName = "order_status"
	c.UDTSchema = "public"
	enums := map[enumKey][]string{{"public", "order_status"}: {"pending", "shipped"}}
	got, err := mapColumn(c, enums)
	if err != nil {
		t.Fatalf("mapColumn(enum) = %v", err)
	}
	if !odcsdest.IsEnum(got) || odcsdest.EnumName(got) != "order_status" {
		t.Fatalf("mapColumn did not produce the enum: %+v", got)
	}
	if labels := odcsdest.EnumLabels(got); strings.Join(labels, ",") != "pending,shipped" {
		t.Errorf("enum labels = %v, want [pending shipped]", labels)
	}
}

func TestMapColumnFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		column pgintrospect.Column
		enums  map[enumKey][]string
		want   string
	}{
		{"unsupported scalar", col("t", "blob", "bytea", "NO"), nil, `unsupported type "bytea"`},
		{
			name:   "non-enum user-defined (empty labels)",
			column: pgintrospect.Column{Table: "t", Name: "where_at", DataType: "USER-DEFINED", UDTName: "addr", UDTSchema: "public", ElementTypeMod: -1},
			enums:  map[enumKey][]string{{"public", "addr"}: {}},
			want:   `unsupported USER-DEFINED type "addr"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mapColumn(tc.column, tc.enums)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("mapColumn() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

// TestAssembleHappy builds a two-table contract from hand-built facts and
// asserts the faithful shape round-trips through the accessors: exact widths,
// primary key, unique, verbatim check, and single-column FK (structural triple).
func TestAssembleHappy(t *testing.T) {
	f := facts{
		tables: []string{"assets", "orders"},
		columns: []pgintrospect.Column{
			col("assets", "id", "bigint", "NO"),
			withLen(col("assets", "isin", "character varying", "NO"), 12),
			col("orders", "order_id", "bigint", "NO"),
			col("orders", "asset_id", "bigint", "NO"),
			col("orders", "note", "text", "YES"),
		},
		pks: map[string][]string{"assets": {"id"}, "orders": {"order_id"}},
		uniques: map[string][]odcs.UniqueConstraint{
			"assets": {{Name: "assets_isin_unique", Columns: []string{"isin"}}},
		},
		checks: map[string][]odcs.CheckConstraint{
			"orders": {{Name: "orders_note_rule", Expression: "note IS NOT NULL"}},
		},
		fks: map[string][]odcs.ForeignKey{
			"orders": {{Column: "asset_id", ReferencedTable: "assets", ReferencedColumn: "id"}},
		},
	}
	c, err := assemble(f, nil)
	if err != nil {
		t.Fatalf("assemble() = %v", err)
	}

	assets := tableOf(t, c, "assets")
	if pk := odcsdest.PrimaryKeyColumns(assets); len(pk) != 1 || pk[0] != "id" {
		t.Errorf("assets PK = %v, want [id]", pk)
	}
	if u := odcs.UniqueConstraints(assets.CustomProperties); len(u) != 1 || u[0].Name != "assets_isin_unique" {
		t.Errorf("assets unique = %+v", u)
	}
	isin := propOf(t, assets, "isin")
	if w, ok := odcsdest.MaxCharacterLength(isin); !ok || w != 12 {
		t.Errorf("isin width = (%d,%v), want 12", w, ok)
	}

	orders := tableOf(t, c, "orders")
	// Column ordinal order is preserved.
	if len(orders.Properties) != 3 || orders.Properties[0].Name != "order_id" || orders.Properties[2].Name != "note" {
		t.Errorf("orders columns = %v, want [order_id asset_id note] in order", names(orders))
	}
	if ck := odcs.CheckConstraints(orders.CustomProperties); len(ck) != 1 || ck[0].Expression != "note IS NOT NULL" {
		t.Errorf("orders checks = %+v, want the verbatim expression", ck)
	}
	fks := odcs.ForeignKeys(orders.CustomProperties)
	if len(fks) != 1 || fks[0].Column != "asset_id" || fks[0].ReferencedTable != "assets" || fks[0].ReferencedColumn != "id" {
		t.Errorf("orders FK = %+v, want asset_id -> assets(id)", fks)
	}
}

func TestAssembleSkipsExcludedColumn(t *testing.T) {
	f := facts{
		tables: []string{"t"},
		columns: []pgintrospect.Column{
			col("t", "id", "integer", "NO"),
			col("t", "secret", "text", "YES"),
		},
		pks: map[string][]string{"t": {"id"}},
	}
	c, err := assemble(f, toSet([]string{"secret"}))
	if err != nil {
		t.Fatalf("assemble() = %v", err)
	}
	tbl := tableOf(t, c, "t")
	for _, p := range tbl.Properties {
		if p.Name == "secret" {
			t.Fatalf("assemble kept the excluded column secret")
		}
	}
}

func TestAssembleFailsClosedOnUnsupportedType(t *testing.T) {
	f := facts{
		tables: []string{"t"},
		columns: []pgintrospect.Column{
			col("t", "id", "integer", "NO"),
			col("t", "blob", "bytea", "NO"),
		},
	}
	_, err := assemble(f, nil)
	if err == nil {
		t.Fatal("assemble() = nil, want a fail-closed type error")
	}
	for _, want := range []string{"t", "blob", "bytea"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestAssembleEmptyIsInvalid(t *testing.T) {
	_, err := assemble(facts{}, nil)
	if err == nil || !strings.Contains(err.Error(), "at least one table") {
		t.Fatalf("assemble(no tables) = %v, want an invalid-contract error", err)
	}
}

func TestWrapRead(t *testing.T) {
	if wrapRead("columns", nil) != nil {
		t.Errorf("wrapRead(nil) should be nil")
	}
	err := wrapRead("columns", sql.ErrConnDone)
	if err == nil || !strings.Contains(err.Error(), "read columns") {
		t.Errorf("wrapRead(err) = %v, want it tagged with the read name", err)
	}
}

// withLen returns c carrying a declared character length.
func withLen(c pgintrospect.Column, n int64) pgintrospect.Column {
	c.CharMaxLength = nullInt64(n)
	return c
}

func tableOf(t *testing.T, c odcs.Contract, name string) odcs.SchemaObject {
	t.Helper()
	for _, tbl := range c.Schema {
		if tbl.Name == name {
			return tbl
		}
	}
	t.Fatalf("contract has no table %q", name)
	return odcs.SchemaObject{}
}

func propOf(t *testing.T, tbl odcs.SchemaObject, name string) odcs.Property {
	t.Helper()
	for _, p := range tbl.Properties {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("table %q has no column %q", tbl.Name, name)
	return odcs.Property{}
}

func names(tbl odcs.SchemaObject) []string {
	var out []string
	for _, p := range tbl.Properties {
		out = append(out, p.Name)
	}
	return out
}
