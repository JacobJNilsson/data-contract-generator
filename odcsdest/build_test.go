package odcsdest_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
)

func intPtr(n int) *int    { return &n }
func boolPtr(b bool) *bool { return &b }

// TestColumnBuilderGoldens pins the exact ODCS Property value each column
// builder produces, the byte-identity contract a DB introspection (B2) and any
// other builder of a destination contract must match. The tricky fidelity
// cases are the point: EXACT integer widths (#124), varchar(n)/char(n) length
// (#82) carried in BOTH the physicalType and logicalTypeOptions.maxLength,
// numeric(p,s), an enum's ordered labels, and an array's element descriptor
// (including a uuid element's format, which must survive into Items).
func TestColumnBuilderGoldens(t *testing.T) {
	cases := []struct {
		name string
		got  odcs.Property
		want odcs.Property
	}{
		{
			"smallint exact width",
			odcsdest.SmallintColumn("a", false),
			odcs.Property{Name: "a", Required: boolPtr(true), LogicalType: odcs.LogicalInteger, PhysicalType: "smallint"},
		},
		{
			"integer exact width",
			odcsdest.IntegerColumn("a", false),
			odcs.Property{Name: "a", Required: boolPtr(true), LogicalType: odcs.LogicalInteger, PhysicalType: "integer"},
		},
		{
			"bigint exact width",
			odcsdest.BigintColumn("a", true),
			odcs.Property{Name: "a", LogicalType: odcs.LogicalInteger, PhysicalType: "bigint"},
		},
		{
			"varchar(3) carries width in both carriers (#82)",
			odcsdest.VarcharColumn("code", false, intPtr(3)),
			odcs.Property{
				Name: "code", Required: boolPtr(true),
				LogicalType: odcs.LogicalString, PhysicalType: "varchar(3)",
				LogicalTypeOptions: &odcs.LogicalTypeOptions{MaxLength: intPtr(3)},
			},
		},
		{
			"unbounded varchar carries no width",
			odcsdest.VarcharColumn("memo", true, nil),
			odcs.Property{Name: "memo", LogicalType: odcs.LogicalString, PhysicalType: "varchar"},
		},
		{
			"char(2) carries width in both carriers (#82)",
			odcsdest.CharColumn("pad", false, intPtr(2)),
			odcs.Property{
				Name: "pad", Required: boolPtr(true),
				LogicalType: odcs.LogicalString, PhysicalType: "char(2)",
				LogicalTypeOptions: &odcs.LogicalTypeOptions{MaxLength: intPtr(2)},
			},
		},
		{
			"numeric(12,2)",
			odcsdest.NumericColumn("amount", false, intPtr(12), intPtr(2)),
			odcs.Property{Name: "amount", Required: boolPtr(true), LogicalType: odcs.LogicalNumber, PhysicalType: "numeric(12,2)"},
		},
		{
			"numeric(12) scale-0 form",
			odcsdest.NumericColumn("amount", true, intPtr(12), nil),
			odcs.Property{Name: "amount", LogicalType: odcs.LogicalNumber, PhysicalType: "numeric(12)"},
		},
		{
			"bare numeric",
			odcsdest.NumericColumn("amount", true, nil, nil),
			odcs.Property{Name: "amount", LogicalType: odcs.LogicalNumber, PhysicalType: "numeric"},
		},
		{
			"text",
			odcsdest.TextColumn("memo", true),
			odcs.Property{Name: "memo", LogicalType: odcs.LogicalString, PhysicalType: "text"},
		},
		{
			"boolean",
			odcsdest.BooleanColumn("ok", false),
			odcs.Property{Name: "ok", Required: boolPtr(true), LogicalType: odcs.LogicalBoolean, PhysicalType: "boolean"},
		},
		{
			"uuid carries format option",
			odcsdest.UUIDColumn("id", false),
			odcs.Property{
				Name: "id", Required: boolPtr(true),
				LogicalType: odcs.LogicalString, PhysicalType: "uuid",
				LogicalTypeOptions: &odcs.LogicalTypeOptions{Format: "uuid"},
			},
		},
		{
			"date carries format option",
			odcsdest.DateColumn("d", true),
			odcs.Property{
				Name: "d", LogicalType: odcs.LogicalDate, PhysicalType: "date",
				LogicalTypeOptions: &odcs.LogicalTypeOptions{Format: "date"},
			},
		},
		{
			"timestamptz",
			odcsdest.TimestamptzColumn("t", true),
			odcs.Property{Name: "t", LogicalType: odcs.LogicalTimestamp, PhysicalType: "timestamptz"},
		},
		{
			"timestamp",
			odcsdest.TimestampColumn("t", true),
			odcs.Property{Name: "t", LogicalType: odcs.LogicalTimestamp, PhysicalType: "timestamp"},
		},
		{
			"jsonb",
			odcsdest.JSONBColumn("j", true),
			odcs.Property{Name: "j", LogicalType: odcs.LogicalObject, PhysicalType: "jsonb"},
		},
		{
			"double",
			odcsdest.DoubleColumn("d", true),
			odcs.Property{Name: "d", LogicalType: odcs.LogicalNumber, PhysicalType: "double"},
		},
		{
			"real",
			odcsdest.RealColumn("r", true),
			odcs.Property{Name: "r", LogicalType: odcs.LogicalNumber, PhysicalType: "real"},
		},
		{
			"enum carries ordered labels",
			odcsdest.EnumColumn("status", false, "order_status", []string{"pending", "shipped"}),
			odcs.Property{
				Name: "status", Required: boolPtr(true),
				LogicalType: odcs.LogicalString, PhysicalType: "order_status",
				Quality: []odcs.Quality{{
					ID:        "status_valid_values",
					Type:      odcs.QualityLibrary,
					Metric:    odcs.MetricInvalidValues,
					Arguments: map[string]any{"validValues": []any{"pending", "shipped"}},
					MustBe:    0,
					Unit:      "rows",
				}},
			},
		},
		{
			"text array element descriptor",
			odcsdest.ArrayColumn("tags", true, odcsdest.TextColumn("", false)),
			odcs.Property{
				Name: "tags", LogicalType: odcs.LogicalArray, PhysicalType: "text[]",
				Items: &odcs.Property{LogicalType: odcs.LogicalString, PhysicalType: "text"},
			},
		},
		{
			"uuid array element keeps its format in Items",
			odcsdest.ArrayColumn("ids", false, odcsdest.UUIDColumn("", false)),
			odcs.Property{
				Name: "ids", Required: boolPtr(true),
				LogicalType: odcs.LogicalArray, PhysicalType: "uuid[]",
				Items: &odcs.Property{
					LogicalType: odcs.LogicalString, PhysicalType: "uuid",
					LogicalTypeOptions: &odcs.LogicalTypeOptions{Format: "uuid"},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !reflect.DeepEqual(tc.got, tc.want) {
				t.Errorf("got  %#v\nwant %#v", tc.got, tc.want)
			}
		})
	}
}

// TestEnumJSONGolden pins the exact emitted JSON bytes of an enum property, so
// the enum encoding this reuses from odcsemit.EnumProperty is proven byte-
// stable through serialisation, not just structurally equal in memory.
func TestEnumJSONGolden(t *testing.T) {
	b, err := json.Marshal(odcsdest.EnumColumn("status", false, "order_status", []string{"a", "b"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"name":"status","logicalType":"string","physicalType":"order_status","required":true,"quality":[{"id":"status_valid_values","type":"library","metric":"invalidValues","arguments":{"validValues":["a","b"]},"mustBe":0,"unit":"rows"}]}`
	if string(b) != want {
		t.Errorf("enum JSON =\n%s\nwant\n%s", b, want)
	}
}

// TestWithCharacterWidthCopiesOptions: attaching a width copies any existing
// options block instead of mutating through the shared pointer, so deriving a
// second bounded column from the first never rewrites it.
func TestWithCharacterWidthCopiesOptions(t *testing.T) {
	first := odcsdest.WithCharacterWidth(odcsdest.TextColumn("a", false), 3)
	second := odcsdest.WithCharacterWidth(first, 8)
	if width, _ := odcsdest.MaxCharacterLength(first); width != 3 {
		t.Errorf("first width = %d after deriving second, want 3 (no aliasing)", width)
	}
	if width, _ := odcsdest.MaxCharacterLength(second); width != 8 {
		t.Errorf("second width = %d, want 8", width)
	}
}

// TestTableAppliesPrimaryKey: Table marks each primary-key column with its
// 1-based position in key order and leaves non-key columns untouched.
func TestTableAppliesPrimaryKey(t *testing.T) {
	obj := odcsdest.Table("accounts",
		[]odcs.Property{
			odcsdest.UUIDColumn("id", false),
			odcsdest.TextColumn("source", false),
			odcsdest.TextColumn("memo", true),
		},
		[]string{"source", "id"}, // key order: source=1, id=2
	)
	if obj.Name != "accounts" || obj.PhysicalName != "accounts" || obj.PhysicalType != "table" {
		t.Fatalf("table shell = %+v", obj)
	}
	want := map[string]int{"source": 1, "id": 2}
	for _, col := range obj.Properties {
		pos, isKey := want[col.Name]
		if !isKey {
			if col.PrimaryKey != nil {
				t.Errorf("column %q marked primaryKey, want unmarked", col.Name)
			}
			continue
		}
		if col.PrimaryKey == nil || !*col.PrimaryKey || col.PrimaryKeyPosition == nil || *col.PrimaryKeyPosition != pos {
			t.Errorf("column %q key mark = (%v,%v), want position %d", col.Name, col.PrimaryKey, col.PrimaryKeyPosition, pos)
		}
	}
}

// TestNewContractTopLevel: NewContract stamps the fixed ODCS top-level fields
// and derives a deterministic id from the SORTED table names, regardless of the
// order the tables are passed in.
func TestNewContractTopLevel(t *testing.T) {
	a := odcsdest.Table("beta", []odcs.Property{odcsdest.TextColumn("x", true)}, nil)
	b := odcsdest.Table("alpha", []odcs.Property{odcsdest.TextColumn("y", true)}, nil)
	c := odcsdest.NewContract(a, b)
	if c.APIVersion != odcs.APIVersion || c.Kind != odcs.KindDataContract || c.Status != odcs.StatusActive || c.Version != "1.0.0" {
		t.Errorf("top-level = %+v", c)
	}
	if c.ID != "destination-contract-alpha-beta" {
		t.Errorf("id = %q, want destination-contract-alpha-beta (sorted names)", c.ID)
	}
}

// TestConstraintBuildersGolden: the SchemaObject-level constraint builders
// attach exactly the dcg custom properties odcs owns, in attach order, and a
// table given no constraints carries none. This pins the single-column FK and
// multi-column value-set carriers B2's introspection must match.
func TestConstraintBuildersGolden(t *testing.T) {
	bare := odcsdest.Table("t", []odcs.Property{odcsdest.TextColumn("x", true)}, nil)
	if bare.CustomProperties != nil {
		t.Errorf("constraint-free table has custom properties %+v, want none", bare.CustomProperties)
	}

	obj := odcsdest.Table("transactions",
		[]odcs.Property{odcsdest.UUIDColumn("id", false), odcsdest.TextColumn("isin", false)},
		[]string{"id"})
	obj = odcsdest.WithUnique(obj, odcs.UniqueConstraint{Name: "u_isin", Columns: []string{"isin"}})
	obj = odcsdest.WithChecks(obj, odcs.CheckConstraint{Name: "c_type", Expression: "type IN ('buy', 'sell')"})
	obj = odcsdest.WithForeignKeys(obj, odcs.ForeignKey{Column: "isin", ReferencedTable: "assets", ReferencedColumn: "id"})

	// Read each fact back through the odcs accessors to prove the round-trip.
	if got := odcs.UniqueConstraints(obj.CustomProperties); !reflect.DeepEqual(got, []odcs.UniqueConstraint{{Name: "u_isin", Columns: []string{"isin"}}}) {
		t.Errorf("unique round-trip = %+v", got)
	}
	if got := odcs.CheckConstraints(obj.CustomProperties); !reflect.DeepEqual(got, []odcs.CheckConstraint{{Name: "c_type", Expression: "type IN ('buy', 'sell')"}}) {
		t.Errorf("check round-trip = %+v", got)
	}
	if got := odcs.ForeignKeys(obj.CustomProperties); !reflect.DeepEqual(got, []odcs.ForeignKey{{Column: "isin", ReferencedTable: "assets", ReferencedColumn: "id"}}) {
		t.Errorf("fk round-trip = %+v", got)
	}
	// The three facts attach in call order under their owned keys.
	keys := []string{obj.CustomProperties[0].Property, obj.CustomProperties[1].Property, obj.CustomProperties[2].Property}
	wantKeys := []string{odcs.CustomKeyUniqueConstraints, odcs.CustomKeyCheckConstraints, odcs.CustomKeyForeignKeys}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("attach order = %v, want %v", keys, wantKeys)
	}

	// A builder given no constraints leaves the table unchanged.
	unchanged := odcsdest.WithUnique(odcsdest.WithChecks(odcsdest.WithForeignKeys(bare)))
	if unchanged.CustomProperties != nil {
		t.Errorf("no-constraint builders added properties %+v", unchanged.CustomProperties)
	}
}
