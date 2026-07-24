package odcsdest_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
)

// numericTypeMod packs a declared precision and scale into the atttypmod form
// Postgres reports for a numeric, so an array-element modifier case exercises
// the same decode path the mapper uses (precision in the high 16 bits, scale in
// the low 16, plus the 4-byte varlena header).
func numericTypeMod(precision, scale int) int {
	return ((precision << 16) | scale) + 4
}

// TestPostgresPropertyGoldens: the pure type mapper produces the SAME ODCS
// property the direct builder produces for each Postgres data_type, proving the
// mapping half B2 will drive is byte-identical to the builders' output — the
// exact-width, varchar(n)/char(n), numeric(p,s), enum, and array cases pinned
// against their builder counterparts.
func TestPostgresPropertyGoldens(t *testing.T) {
	cases := []struct {
		name string
		in   odcsdest.PostgresColumnType
		want odcs.Property
	}{
		{
			"smallint exact width",
			odcsdest.PostgresColumnType{Name: "a", Nullable: false, DataType: "smallint"},
			odcsdest.SmallintColumn("a", false),
		},
		{
			"integer exact width",
			odcsdest.PostgresColumnType{Name: "a", Nullable: true, DataType: "integer"},
			odcsdest.IntegerColumn("a", true),
		},
		{
			"bigint exact width",
			odcsdest.PostgresColumnType{Name: "a", Nullable: false, DataType: "bigint"},
			odcsdest.BigintColumn("a", false),
		},
		{
			"varchar(3) length (#82)",
			odcsdest.PostgresColumnType{Name: "code", Nullable: false, DataType: "character varying", CharMaxLength: intPtr(3)},
			odcsdest.VarcharColumn("code", false, intPtr(3)),
		},
		{
			"unbounded varchar",
			odcsdest.PostgresColumnType{Name: "memo", Nullable: true, DataType: "character varying"},
			odcsdest.VarcharColumn("memo", true, nil),
		},
		{
			"char(2) length (#82)",
			odcsdest.PostgresColumnType{Name: "pad", Nullable: false, DataType: "character", CharMaxLength: intPtr(2)},
			odcsdest.CharColumn("pad", false, intPtr(2)),
		},
		{
			"numeric(12,2)",
			odcsdest.PostgresColumnType{Name: "amount", Nullable: false, DataType: "numeric", NumericPrecision: intPtr(12), NumericScale: intPtr(2)},
			odcsdest.NumericColumn("amount", false, intPtr(12), intPtr(2)),
		},
		{
			"enum with labels",
			odcsdest.PostgresColumnType{Name: "status", Nullable: false, DataType: "USER-DEFINED", UDTName: "order_status", EnumLabels: []string{"a", "b"}},
			odcsdest.EnumColumn("status", false, "order_status", []string{"a", "b"}),
		},
		{
			"scalar via the vocabulary map (uuid keeps format)",
			odcsdest.PostgresColumnType{Name: "id", Nullable: false, DataType: "uuid"},
			odcsdest.UUIDColumn("id", false),
		},
		{
			"text array, scalar element",
			odcsdest.PostgresColumnType{Name: "tags", Nullable: true, DataType: "ARRAY", ElementDataType: "text", ElementTypeMod: -1, ArrayDims: 1},
			odcsdest.ArrayColumn("tags", true, odcsdest.TextColumn("", false)),
		},
		{
			"unbounded numeric array element",
			odcsdest.PostgresColumnType{Name: "amts", Nullable: true, DataType: "ARRAY", ElementDataType: "numeric", ElementTypeMod: -1, ArrayDims: 1},
			odcsdest.ArrayColumn("amts", true, odcsdest.NumericColumn("", false, nil, nil)),
		},
		{
			"unbounded varchar array element",
			odcsdest.PostgresColumnType{Name: "codes", Nullable: true, DataType: "ARRAY", ElementDataType: "character varying", ElementTypeMod: -1, ArrayDims: 1},
			odcsdest.ArrayColumn("codes", true, odcsdest.VarcharColumn("", false, nil)),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := odcsdest.PostgresProperty(tc.in)
			if err != nil {
				t.Fatalf("PostgresProperty(%s) errored: %v", tc.name, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("PostgresProperty(%s) =\n%#v\nwant\n%#v", tc.name, got, tc.want)
			}
		})
	}
}

// TestPostgresPropertyFailClosed: every type outside the reproducible
// vocabulary is an error naming the column, never a guessed property.
func TestPostgresPropertyFailClosed(t *testing.T) {
	cases := []struct {
		name    string
		in      odcsdest.PostgresColumnType
		wantSub string
	}{
		{"USER-DEFINED with no enum labels", odcsdest.PostgresColumnType{Name: "c", DataType: "USER-DEFINED", UDTName: "widget"}, "not an enum"},
		{"bare bpchar", odcsdest.PostgresColumnType{Name: "c", DataType: "character"}, "bare bpchar"},
		{"unknown scalar", odcsdest.PostgresColumnType{Name: "c", DataType: "money"}, `unsupported type "money"`},
		{"multi-dimensional array", odcsdest.PostgresColumnType{Name: "c", DataType: "ARRAY", ElementDataType: "text", ElementTypeMod: -1, ArrayDims: 2}, "multi-dimensional"},
		{"numeric array element with modifier", odcsdest.PostgresColumnType{Name: "c", DataType: "ARRAY", ElementDataType: "numeric", ElementTypeMod: numericTypeMod(12, 2), ArrayDims: 1}, "numeric(12,2)"},
		{"varchar array element with length", odcsdest.PostgresColumnType{Name: "c", DataType: "ARRAY", ElementDataType: "character varying", ElementTypeMod: 7, ArrayDims: 1}, "character varying(3)"},
		{"char array element with length", odcsdest.PostgresColumnType{Name: "c", DataType: "ARRAY", ElementDataType: "character", ElementTypeMod: 6, ArrayDims: 1}, "character(2)"},
		{"bare bpchar array element", odcsdest.PostgresColumnType{Name: "c", DataType: "ARRAY", ElementDataType: "character", ElementTypeMod: -1, ArrayDims: 1}, "bare bpchar array element"},
		{"unknown array element", odcsdest.PostgresColumnType{Name: "c", DataType: "ARRAY", ElementDataType: "money", ElementTypeMod: -1, ArrayDims: 1}, `unsupported array element type "money"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := odcsdest.PostgresProperty(tc.in)
			if err == nil {
				t.Fatalf("PostgresProperty(%s) = nil error, want a fail-closed error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("PostgresProperty(%s) error = %q, want substring %q", tc.name, err.Error(), tc.wantSub)
			}
		})
	}
}

// TestPostgresScalarBuilder: a known plain scalar resolves to its builder, and
// a specially handled or unknown type does not (the caller routes those through
// the full mapper).
func TestPostgresScalarBuilder(t *testing.T) {
	build, ok := odcsdest.PostgresScalarBuilder("bigint")
	if !ok {
		t.Fatal("PostgresScalarBuilder(bigint) = ok=false, want true")
	}
	if got := build("n", false); !reflect.DeepEqual(got, odcsdest.BigintColumn("n", false)) {
		t.Errorf("resolved builder produced %#v", got)
	}
	for _, dt := range []string{"numeric", "character varying", "character", "USER-DEFINED", "ARRAY", "money"} {
		if _, ok := odcsdest.PostgresScalarBuilder(dt); ok {
			t.Errorf("PostgresScalarBuilder(%q) = ok=true, want false", dt)
		}
	}
}
