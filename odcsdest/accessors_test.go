package odcsdest_test

import (
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
)

// TestPostgresDDLType exercises every logical branch of the reverse mapping
// from an ODCS property back to a Postgres DDL type: each scalar keyword, the
// exact-width integers and lengths, arrays, and the fail-closed shapes (an
// enum, a bad physicalType, an overflowing length/precision, a broken array).
func TestPostgresDDLType(t *testing.T) {
	p := func(l odcs.LogicalType, phys string) odcs.Property {
		return odcs.Property{Name: "c", LogicalType: l, PhysicalType: phys}
	}
	cases := []struct {
		name string
		col  odcs.Property
		ddl  string
		ok   bool
	}{
		{"text", odcsdest.TextColumn("c", true), "text", true},
		{"uuid", odcsdest.UUIDColumn("c", true), "uuid", true},
		{"varchar(3)", odcsdest.VarcharColumn("c", true, intPtr(3)), "varchar(3)", true},
		{"char(2)", odcsdest.CharColumn("c", true, intPtr(2)), "char(2)", true},
		{"smallint", odcsdest.SmallintColumn("c", true), "smallint", true},
		{"integer", odcsdest.IntegerColumn("c", true), "integer", true},
		{"bigint", odcsdest.BigintColumn("c", true), "bigint", true},
		{"boolean", odcsdest.BooleanColumn("c", true), "boolean", true},
		{"double", odcsdest.DoubleColumn("c", true), "double precision", true},
		{"real", odcsdest.RealColumn("c", true), "real", true},
		{"numeric(12,2)", odcsdest.NumericColumn("c", true, intPtr(12), intPtr(2)), "numeric(12,2)", true},
		{"bare numeric", odcsdest.NumericColumn("c", true, nil, nil), "numeric", true},
		{"timestamptz", odcsdest.TimestamptzColumn("c", true), "timestamptz", true},
		{"timestamp", odcsdest.TimestampColumn("c", true), "timestamp", true},
		{"date", odcsdest.DateColumn("c", true), "date", true},
		{"jsonb", odcsdest.JSONBColumn("c", true), "jsonb", true},
		{"text array", odcsdest.ArrayColumn("c", true, odcsdest.TextColumn("", false)), "text[]", true},

		{"enum is not placed here", odcsdest.EnumColumn("c", true, "st", []string{"a"}), "", false},
		{"unknown string physical", p(odcs.LogicalString, "weird"), "", false},
		{"overflowing varchar length", p(odcs.LogicalString, "varchar(99999999999999999999)"), "", false},
		{"unknown integer physical", p(odcs.LogicalInteger, "int4"), "", false},
		{"unknown boolean physical", p(odcs.LogicalBoolean, "bool"), "", false},
		{"unknown number physical", p(odcs.LogicalNumber, "decimal"), "", false},
		{"overflowing numeric precision", p(odcs.LogicalNumber, "numeric(99999999999999999999)"), "", false},
		{"overflowing numeric scale", p(odcs.LogicalNumber, "numeric(12,99999999999999999999)"), "", false},
		{"unknown timestamp physical", p(odcs.LogicalTimestamp, "timestamptz(6)"), "", false},
		{"unknown date physical", p(odcs.LogicalDate, "timestamp"), "", false},
		{"unknown object physical", p(odcs.LogicalObject, "json"), "", false},
		{"unhandled logical type", p(odcs.LogicalTime, "time"), "", false},
		{"array with no items", odcs.Property{LogicalType: odcs.LogicalArray, PhysicalType: "text[]"}, "", false},
		{"array with bad element", odcs.Property{LogicalType: odcs.LogicalArray, PhysicalType: "x[]", Items: &odcs.Property{LogicalType: odcs.LogicalInteger, PhysicalType: "int4"}}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ddl, ok := odcsdest.PostgresDDLType(tc.col)
			if ddl != tc.ddl || ok != tc.ok {
				t.Errorf("PostgresDDLType = (%q,%v), want (%q,%v)", ddl, ok, tc.ddl, tc.ok)
			}
		})
	}
}

// TestNullable: an unset Required reads nullable, required=true reads NOT NULL,
// and an explicit required=false reads nullable.
func TestNullable(t *testing.T) {
	if !odcsdest.Nullable(odcsdest.TextColumn("c", true)) {
		t.Error("nullable text read NOT NULL")
	}
	if odcsdest.Nullable(odcsdest.TextColumn("c", false)) {
		t.Error("NOT NULL text read nullable")
	}
	explicitFalse := odcs.Property{Name: "c", Required: boolPtr(false), LogicalType: odcs.LogicalString, PhysicalType: "text"}
	if !odcsdest.Nullable(explicitFalse) {
		t.Error("required=false read NOT NULL")
	}
}

// TestMaxCharacterLength: the width accessor reads back what WithCharacterWidth
// and the bounded-string constructors wrote (from both carriers), and reads
// ok=false for every shape carrying no width.
func TestMaxCharacterLength(t *testing.T) {
	// A hand-authored contract that declares only the physicalType length,
	// with no logicalTypeOptions carrier, is still read.
	physicalOnly := odcsdest.VarcharColumn("code", false, intPtr(3))
	physicalOnly.LogicalTypeOptions = nil

	cases := []struct {
		name  string
		col   odcs.Property
		width int
		ok    bool
	}{
		{"bounded text via WithCharacterWidth", odcsdest.WithCharacterWidth(odcsdest.TextColumn("currency", false), 3), 3, true},
		{"varchar(3)", odcsdest.VarcharColumn("code", false, intPtr(3)), 3, true},
		{"char(2)", odcsdest.CharColumn("pad", false, intPtr(2)), 2, true},
		{"physicalType-only varchar(3)", physicalOnly, 3, true},

		{"unbounded text", odcsdest.TextColumn("memo", true), 0, false},
		{"unbounded varchar", odcsdest.VarcharColumn("memo", true, nil), 0, false},
		{"uuid", odcsdest.UUIDColumn("id", false), 0, false},
		{"numeric", odcsdest.NumericColumn("amount", true, intPtr(12), intPtr(2)), 0, false},
		{"enum", odcsdest.EnumColumn("status", true, "st", []string{"a"}), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			width, ok := odcsdest.MaxCharacterLength(tc.col)
			if width != tc.width || ok != tc.ok {
				t.Errorf("MaxCharacterLength = (%d,%v), want (%d,%v)", width, ok, tc.width, tc.ok)
			}
		})
	}
}

// TestNumericPrecisionScale: the (p,s) accessor reads the declared bounds out
// of the physicalType, defaults an absent scale to 0, and reads ok=false for a
// bare numeric, a binary float, and a non-numeric column.
func TestNumericPrecisionScale(t *testing.T) {
	cases := []struct {
		name      string
		col       odcs.Property
		precision int
		scale     int
		ok        bool
	}{
		{"numeric(20,2)", odcsdest.NumericColumn("a", true, intPtr(20), intPtr(2)), 20, 2, true},
		{"numeric(12)", odcsdest.NumericColumn("a", true, intPtr(12), nil), 12, 0, true},
		{"bare numeric", odcsdest.NumericColumn("a", true, nil, nil), 0, 0, false},
		{"double", odcsdest.DoubleColumn("a", true), 0, 0, false},
		{"text", odcsdest.TextColumn("a", true), 0, 0, false},
	}
	for _, tc := range cases {
		p, s, ok := odcsdest.NumericPrecisionScale(tc.col)
		if p != tc.precision || s != tc.scale || ok != tc.ok {
			t.Errorf("NumericPrecisionScale(%s) = (%d,%d,%v), want (%d,%d,%v)", tc.name, p, s, ok, tc.precision, tc.scale, tc.ok)
		}
	}
}

// TestEnumAccessors: IsEnum recognises the enum encoding, EnumName reads the
// type name, EnumLabels reads the ordered labels, and a plain text column reads
// as a non-enum with no labels.
func TestEnumAccessors(t *testing.T) {
	enum := odcsdest.EnumColumn("status", false, "order_status", []string{"pending", "shipped"})
	if !odcsdest.IsEnum(enum) {
		t.Error("IsEnum(enum) = false")
	}
	if name := odcsdest.EnumName(enum); name != "order_status" {
		t.Errorf("EnumName = %q, want order_status", name)
	}
	if labels := odcsdest.EnumLabels(enum); !reflect.DeepEqual(labels, []string{"pending", "shipped"}) {
		t.Errorf("EnumLabels = %v", labels)
	}
	text := odcsdest.TextColumn("memo", true)
	if odcsdest.IsEnum(text) {
		t.Error("IsEnum(text) = true")
	}
	if labels := odcsdest.EnumLabels(text); labels != nil {
		t.Errorf("EnumLabels(text) = %v, want nil", labels)
	}
}

// TestPrimaryKeyColumns: the key columns read back in position order, a table
// with no key reads nil, and a key column carrying no explicit position sorts
// as position 0.
func TestPrimaryKeyColumns(t *testing.T) {
	obj := odcsdest.Table("t",
		[]odcs.Property{odcsdest.TextColumn("a", false), odcsdest.TextColumn("b", false), odcsdest.TextColumn("c", true)},
		[]string{"b", "a"}) // b=1, a=2
	if got := odcsdest.PrimaryKeyColumns(obj); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Errorf("PrimaryKeyColumns = %v, want [b a]", got)
	}

	none := odcsdest.Table("t", []odcs.Property{odcsdest.TextColumn("a", true)}, nil)
	if got := odcsdest.PrimaryKeyColumns(none); got != nil {
		t.Errorf("PrimaryKeyColumns(no key) = %v, want nil", got)
	}

	// A key column with PrimaryKey set but no explicit position defaults to 0.
	positionless := odcs.SchemaObject{Properties: []odcs.Property{
		{Name: "k", PrimaryKey: boolPtr(true)},
	}}
	if got := odcsdest.PrimaryKeyColumns(positionless); !reflect.DeepEqual(got, []string{"k"}) {
		t.Errorf("PrimaryKeyColumns(positionless) = %v, want [k]", got)
	}
}

// TestSortedTableNames returns the contract's table names sorted, independent
// of declared order.
func TestSortedTableNames(t *testing.T) {
	c := odcsdest.NewContract(
		odcsdest.Table("gamma", []odcs.Property{odcsdest.TextColumn("x", true)}, nil),
		odcsdest.Table("alpha", []odcs.Property{odcsdest.TextColumn("x", true)}, nil),
		odcsdest.Table("beta", []odcs.Property{odcsdest.TextColumn("x", true)}, nil),
	)
	if got := odcsdest.SortedTableNames(c); !reflect.DeepEqual(got, []string{"alpha", "beta", "gamma"}) {
		t.Errorf("SortedTableNames = %v", got)
	}
}
