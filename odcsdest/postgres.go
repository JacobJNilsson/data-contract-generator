package odcsdest

import (
	"fmt"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
)

// The data_type strings information_schema.columns reports for the types the
// mapping treats specially (outside the plain scalar vocabulary).
const (
	// userDefinedDataType is what information_schema reports for an enum (and
	// other USER-DEFINED types). The mapping treats it as an enum; a
	// USER-DEFINED type carrying no enum labels fails closed rather than
	// reaching the contract as an empty enum.
	userDefinedDataType = "USER-DEFINED"

	// arrayDataType is what information_schema reports for any array column,
	// regardless of element type or declared dimensionality.
	arrayDataType = "ARRAY"

	// numericDataType is what Postgres reports for a numeric (its "decimal"
	// alias is normalised to "numeric").
	numericDataType = "numeric"

	// varcharDataType and charDataType are the two bounded string types. They
	// are handled outside the scalar vocabulary because their builders carry
	// the declared length a value overflowing it is rejected on (22001).
	varcharDataType = "character varying"
	charDataType    = "character"
)

// pgScalarBuilders maps the data_type strings Postgres reports to the column
// builder for that type. It is the only Postgres-to-ODCS mapping for plain
// scalars: a type Postgres reports that is absent here (and not one of the
// specially handled numeric/varchar/char/enum/array types) is one the contract
// cannot faithfully reproduce, so the mapping fails closed on it.
//
// The vocabulary is intentionally narrow and matches what a consumer can stand
// up. The integer family (smallint/integer/bigint) each map to their EXACT
// width, never widened, so a value the live column's narrower integer rejects
// (22003) is rejected downstream too. The textual JSON variant "json" and the
// exotic scalars (bytea, inet/cidr, bit, money, interval, time/timetz) are
// deliberately absent, so they fail closed for an operator to correct rather
// than being guessed.
var pgScalarBuilders = map[string]func(name string, nullable bool) odcs.Property{
	"smallint":                    SmallintColumn,
	"integer":                     IntegerColumn,
	"bigint":                      BigintColumn,
	"text":                        TextColumn,
	"boolean":                     BooleanColumn,
	"timestamp with time zone":    TimestamptzColumn,
	"uuid":                        UUIDColumn,
	"date":                        DateColumn,
	"jsonb":                       JSONBColumn,
	"timestamp without time zone": TimestampColumn,
	"double precision":            DoubleColumn,
	"real":                        RealColumn,
}

// PostgresScalarBuilder returns the column builder for a plain Postgres scalar
// data_type and whether one exists. A type handled specially (numeric,
// character varying, character, USER-DEFINED, ARRAY) or outside the vocabulary
// yields ok=false; PostgresProperty is the full mapper that dispatches those
// special cases and this scalar vocabulary together.
func PostgresScalarBuilder(dataType string) (func(name string, nullable bool) odcs.Property, bool) {
	build, ok := pgScalarBuilders[dataType]
	return build, ok
}

// PostgresColumnType is the pure, DB-free description of a Postgres column's
// TYPE: the catalog signals a mapper needs to choose an ODCS property, with
// none of the policy signals (identity/generated/default folding, column or
// table exclusions) a DB-introspecting caller layers on top. A caller reads
// these fields from the catalog and computes the effective Nullable, then calls
// PostgresProperty; the resulting property is byte-identical to one a builder
// produces directly.
type PostgresColumnType struct {
	// Name is the column name.
	Name string

	// Nullable is the EFFECTIVE nullability the property should encode. A
	// DB-introspecting caller folds a database-populated NOT NULL column
	// (identity/generated/defaulted) into this, so it is not always the live
	// column's raw nullability; the mapping treats it verbatim.
	Nullable bool

	// DataType is the information_schema.columns data_type string.
	DataType string

	// UDTName is the enum type's own name, used when DataType is
	// USER-DEFINED.
	UDTName string

	// EnumLabels are an enum's ordered labels (its enumsortorder), which a
	// caller resolves from pg_enum for a USER-DEFINED column. An empty slice on
	// a USER-DEFINED column fails closed (a USER-DEFINED type that is not an
	// enum, or a dropped type).
	EnumLabels []string

	// NumericPrecision and NumericScale are a numeric's declared precision and
	// scale, nil for an unconstrained numeric.
	NumericPrecision *int
	NumericScale     *int

	// CharMaxLength is a bounded string's declared character length, nil for an
	// unbounded varchar or a bare bpchar.
	CharMaxLength *int

	// ElementDataType, ElementTypeMod, and ArrayDims describe an array column's
	// element, used when DataType is ARRAY. ElementTypeMod is the element's
	// pg_attribute.atttypmod (-1 when the element carries no modifier);
	// ArrayDims is the declared dimension count.
	ElementDataType string
	ElementTypeMod  int
	ArrayDims       int
}

// PostgresProperty maps a Postgres column's type descriptor to its ODCS
// property, failing CLOSED (an error, never a guessed column) on any type
// outside the reproducible vocabulary. It is the pure half of destination
// introspection: a caller supplies the descriptor from a catalog read (and the
// enum labels from a second read), and this owns the type MAPPING — which
// builder each data_type routes to, and which shapes are unrepresentable.
//
// The unrepresentable shapes it rejects: a USER-DEFINED type with no enum
// labels (a composite or domain, not an enum); a bare bpchar (a blank-padded
// character with no declared length, which the DDL keyword "char" would
// silently narrow to char(1)); an array that is multi-dimensional, has a
// non-scalar or type-modified element, or a bare bpchar element; and any scalar
// outside pgScalarBuilders.
func PostgresProperty(c PostgresColumnType) (odcs.Property, error) {
	switch c.DataType {
	case userDefinedDataType:
		if len(c.EnumLabels) == 0 {
			return odcs.Property{}, fmt.Errorf("column %q has unsupported USER-DEFINED type %q (not an enum)", c.Name, c.UDTName)
		}
		return EnumColumn(c.Name, c.Nullable, c.UDTName, c.EnumLabels), nil
	case arrayDataType:
		element, err := arrayElement(c)
		if err != nil {
			return odcs.Property{}, err
		}
		return ArrayColumn(c.Name, c.Nullable, element), nil
	case numericDataType:
		return NumericColumn(c.Name, c.Nullable, c.NumericPrecision, c.NumericScale), nil
	case varcharDataType:
		// The declared length rides in the physicalType (varchar(3)) so a
		// consumer reproduces the exact bound an overlong value is rejected on
		// (22001); an unbounded varchar reports a nil length and renders bare.
		return VarcharColumn(c.Name, c.Nullable, c.CharMaxLength), nil
	case charDataType:
		// A char column normally always carries a length (Postgres normalises
		// bare "char" to char(1)); a nil length means a bare bpchar (unlimited
		// blank-padded), which the vocabulary cannot render faithfully — the
		// keyword "char" means char(1), a DIFFERENT type — so it fails closed.
		if c.CharMaxLength == nil {
			return odcs.Property{}, fmt.Errorf("column %q has unsupported bare bpchar type (a blank-padded character with no declared length cannot be reproduced faithfully)", c.Name)
		}
		return CharColumn(c.Name, c.Nullable, c.CharMaxLength), nil
	default:
		build, ok := pgScalarBuilders[c.DataType]
		if !ok {
			return odcs.Property{}, fmt.Errorf("column %q has unsupported type %q", c.Name, c.DataType)
		}
		return build(c.Name, c.Nullable), nil
	}
}

// arrayElement maps an ARRAY column to its faithful scalar element property (a
// nameless scalar built by the same builder a non-array column of that type
// uses, so it carries the element's logical/physical pair AND its format
// option), failing closed on the shapes fidelity forbids approximating: a
// multi-dimensional array; an element that is itself non-scalar or outside the
// scalar vocabulary; and an element carrying a type modifier — a declared
// numeric precision (numeric(12,2)[]) or string length (varchar(3)[]) — which
// an array element's physicalType cannot hold without silently widening.
func arrayElement(c PostgresColumnType) (odcs.Property, error) {
	if c.ArrayDims > 1 {
		return odcs.Property{}, fmt.Errorf("column %q has unsupported multi-dimensional array type (%d dimensions)", c.Name, c.ArrayDims)
	}
	if c.ElementDataType == numericDataType {
		if c.ElementTypeMod != -1 {
			precision, scale := numericTypeModParts(c.ElementTypeMod)
			return odcs.Property{}, fmt.Errorf("column %q has unsupported array element type numeric(%d,%d) (an array element's precision and scale cannot be reproduced faithfully)", c.Name, precision, scale)
		}
		// An unconstrained numeric[] element is faithful: the bare numeric pair.
		return NumericColumn("", false, nil, nil), nil
	}
	if c.ElementDataType == varcharDataType || c.ElementDataType == charDataType {
		if c.ElementTypeMod != -1 {
			// The string typmod packs the declared length plus the 4-byte
			// varlena header, the same header offset the numeric decoder
			// subtracts; naming the length keeps the message concrete.
			return odcs.Property{}, fmt.Errorf("column %q has unsupported array element type %s(%d) (an array element's declared length cannot be reproduced faithfully)", c.Name, c.ElementDataType, c.ElementTypeMod-4)
		}
		if c.ElementDataType == charDataType {
			// An unmodified "character" element is a bare bpchar[] (a declared
			// char[] normalises to char(1)[], caught by the length branch
			// above), which "char[]" would silently narrow to char(1)[].
			return odcs.Property{}, fmt.Errorf("column %q has unsupported bare bpchar array element type (a blank-padded character with no declared length cannot be reproduced faithfully)", c.Name)
		}
		// An unbounded varchar[] element is faithful: the bare varchar pair.
		return VarcharColumn("", false, nil), nil
	}
	build, ok := pgScalarBuilders[c.ElementDataType]
	if !ok {
		return odcs.Property{}, fmt.Errorf("column %q has unsupported array element type %q", c.Name, c.ElementDataType)
	}
	return build("", false), nil
}

// numericTypeModParts decodes the declared precision and scale from a numeric
// column's pg_attribute.atttypmod, the way Postgres packs it: the modifier
// minus the 4-byte varlena header holds the precision in its high 16 bits and
// the scale in its low 16 bits. It names the offending shape (numeric(p,s)) in
// the array fail-closed message; the caller has already established the
// modifier is present (not -1).
func numericTypeModParts(typeMod int) (precision, scale int) {
	return ((typeMod - 4) >> 16) & 0xFFFF, (typeMod - 4) & 0xFFFF
}
