package pgcontract

// Fast-tier unit tests for the pure faithful validator, driving each violation
// branch with a hand-built contract (no database).

import (
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
)

// okTable builds a minimal valid one-column table.
func okTable(name string) odcs.SchemaObject {
	return odcsdest.Table(name, []odcs.Property{odcsdest.IntegerColumn("id", false)}, []string{"id"})
}

func TestValidateHappy(t *testing.T) {
	c := odcsdest.NewContract(
		okTable("assets"),
		odcsdest.Table("orders",
			[]odcs.Property{odcsdest.BigintColumn("order_id", false), odcsdest.EnumColumn("status", false, "order_status", []string{"pending", "shipped"})},
			[]string{"order_id"}),
	)
	if err := validate(c); err != nil {
		t.Fatalf("validate(valid) = %v, want nil", err)
	}
}

func TestValidateEmpty(t *testing.T) {
	if err := validate(odcs.Contract{}); err == nil || !strings.Contains(err.Error(), "at least one table") {
		t.Fatalf("validate(empty) = %v, want an at-least-one-table error", err)
	}
}

func TestValidateStructuralViolations(t *testing.T) {
	cases := []struct {
		name     string
		contract odcs.Contract
		want     string
	}{
		{
			name:     "duplicate table",
			contract: odcs.Contract{Schema: []odcs.SchemaObject{okTable("dup"), okTable("dup")}},
			want:     `duplicate table name "dup"`,
		},
		{
			name:     "invalid table identifier",
			contract: odcs.Contract{Schema: []odcs.SchemaObject{okTable("bad name")}},
			want:     "table name",
		},
		{
			name:     "table with no columns",
			contract: odcs.Contract{Schema: []odcs.SchemaObject{{Name: "empty"}}},
			want:     `table "empty" must declare at least one column`,
		},
		{
			name: "duplicate column",
			contract: odcs.Contract{Schema: []odcs.SchemaObject{{Name: "t", Properties: []odcs.Property{
				odcsdest.IntegerColumn("id", false), odcsdest.TextColumn("id", true),
			}}}},
			want: `duplicate column name "id"`,
		},
		{
			name: "invalid column identifier",
			contract: odcs.Contract{Schema: []odcs.SchemaObject{{Name: "t", Properties: []odcs.Property{
				odcsdest.IntegerColumn("bad col", false),
			}}}},
			want: "column name",
		},
		{
			name: "unsupported column type",
			contract: odcs.Contract{Schema: []odcs.SchemaObject{{Name: "t", Properties: []odcs.Property{
				{Name: "weird", LogicalType: odcs.LogicalString, PhysicalType: "nonsense"},
			}}}},
			want: "unsupported column type",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validate(tc.contract)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("validate() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateEnumViolations(t *testing.T) {
	cases := []struct {
		name string
		col  odcs.Property
		want string
	}{
		{"invalid enum name", odcsdest.EnumColumn("s", false, "bad type", []string{"a"}), "enum type name"},
		{"enum with no labels", odcsdest.EnumColumn("s", false, "mood", nil), "at least one label"},
		{"enum with empty label", odcsdest.EnumColumn("s", false, "mood", []string{""}), "label must be non-empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := odcs.Contract{Schema: []odcs.SchemaObject{{Name: "t", Properties: []odcs.Property{tc.col}}}}
			err := validate(c)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("validate() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateEnumConsistencyConflict(t *testing.T) {
	c := odcs.Contract{Schema: []odcs.SchemaObject{
		{Name: "a", Properties: []odcs.Property{odcsdest.EnumColumn("s", false, "mood", []string{"happy", "sad"})}},
		{Name: "b", Properties: []odcs.Property{odcsdest.EnumColumn("s", false, "mood", []string{"up", "down"})}},
	}}
	err := validate(c)
	if err == nil || !strings.Contains(err.Error(), "conflicting with the same-named enum") {
		t.Fatalf("validate(enum conflict) = %v, want a conflict error", err)
	}
}

func TestValidateConstraintReferenceViolations(t *testing.T) {
	unique := odcsdest.WithUnique(okTable("t"), odcs.UniqueConstraint{Name: "u_ghost", Columns: []string{"ghost"}})
	if err := validate(odcs.Contract{Schema: []odcs.SchemaObject{unique}}); err == nil || !strings.Contains(err.Error(), `unique constraint "u_ghost" references unknown column "ghost"`) {
		t.Errorf("validate(unique over unknown column) = %v, want a reference error", err)
	}

	fk := odcsdest.WithForeignKeys(okTable("t"), odcs.ForeignKey{Column: "ghost", ReferencedTable: "other", ReferencedColumn: "id"})
	if err := validate(odcs.Contract{Schema: []odcs.SchemaObject{fk}}); err == nil || !strings.Contains(err.Error(), `foreign key references unknown column "ghost"`) {
		t.Errorf("validate(FK over unknown column) = %v, want a reference error", err)
	}
}

func TestValidIdentifier(t *testing.T) {
	if err := validIdentifier("table", ""); err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Errorf("validIdentifier(empty) = %v", err)
	}
	if err := validIdentifier("table", strings.Repeat("x", maxIdentifierLength+1)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("validIdentifier(too long) = %v", err)
	}
	if err := validIdentifier("table", "ok_name1"); err != nil {
		t.Errorf("validIdentifier(valid) = %v, want nil", err)
	}
}
