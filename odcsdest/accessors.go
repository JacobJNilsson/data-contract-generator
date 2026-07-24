package odcsdest

import (
	"regexp"
	"sort"
	"strconv"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsemit"
)

// numericPhysicalPattern matches an ODCS numeric physicalType and captures its
// optional precision and scale: "numeric", "numeric(12)", or "numeric(12,2)".
// It is the reverse of the physicalType NumericColumn writes, so the accessors
// read a numeric's declared precision and scale back out of the document.
var numericPhysicalPattern = regexp.MustCompile(`^numeric(?:\((\d+)(?:,(\d+))?\))?$`)

// stringPhysicalPattern matches a bounded-string physicalType and captures its
// optional declared length: "varchar", "varchar(3)", "char", or "char(2)". It
// is the reverse of what VarcharColumn/CharColumn write, so the declared length
// a bounded string rejects overlong values on (SQLSTATE 22001) reads back out
// of the document exactly.
var stringPhysicalPattern = regexp.MustCompile(`^(?:varchar|char)(?:\((\d+)\))?$`)

// IsEnum reports whether a property encodes an enum: a string-logical column
// carrying the library invalidValues quality rule (the encoding EnumColumn /
// odcsemit.EnumProperty write). It reads through odcsemit.ReadEnumLabels, the
// library's single enum reader, so a destination enum and a source enum are
// recognised the same way.
func IsEnum(col odcs.Property) bool {
	_, _, ok := odcsemit.ReadEnumLabels(col)
	return col.LogicalType == odcs.LogicalString && ok
}

// EnumName returns an enum property's type name (its physicalType). For a
// non-enum property it returns the physicalType unchanged; callers gate on
// IsEnum first.
func EnumName(col odcs.Property) string {
	return col.PhysicalType
}

// EnumLabels returns an enum property's allowed labels in declared order, read
// through odcsemit.ReadEnumLabels. A property with no enum quality rule yields
// nil.
func EnumLabels(col odcs.Property) []string {
	_, labels, _ := odcsemit.ReadEnumLabels(col)
	return labels
}

// PostgresDDLType returns the Postgres type a column is created with and
// whether the column is one this can place. It is the single reverse mapping
// from the ODCS logical/physical pair to the concrete Postgres DDL type:
//
//   - a scalar renders its DDL keyword (text, bigint, boolean, timestamptz,
//     uuid, date, jsonb, "double precision", real), with a numeric carrying its
//     declared precision and scale (numeric(p,s)) so a money-shaped column is
//     reproduced exactly rather than widened;
//   - an array renders "<element>[]" from its faithful scalar element;
//   - an ENUM is NOT placed here (ok=false): its DDL type is the run-scoped enum
//     type name a consumer renders from EnumName, not a vocabulary keyword.
//
// ok=false means the column is an enum (the consumer renders it specially) OR
// an unrepresentable type.
func PostgresDDLType(col odcs.Property) (string, bool) {
	if IsEnum(col) {
		return "", false
	}
	if col.LogicalType == odcs.LogicalArray {
		element, ok := arrayElementDDLType(col)
		if !ok {
			return "", false
		}
		return element + "[]", true
	}
	return scalarDDLType(col)
}

// scalarDDLType maps a scalar property's logical/physical pair to its Postgres
// DDL keyword and whether it is a scalar this knows. The numeric case carries
// the declared precision and scale through from the physicalType, so a
// numeric(12,2) renders exactly. An enum (which shares logicalType string with
// text) is NOT a scalar here: it carries the invalidValues rule, so the caller
// gates on IsEnum before reaching this. The pair is matched (not just the
// physicalType) so a physicalType the encoding never writes fails closed.
func scalarDDLType(col odcs.Property) (string, bool) {
	switch col.LogicalType {
	case odcs.LogicalString:
		switch col.PhysicalType {
		case "text":
			return "text", true
		case "uuid":
			return "uuid", true
		default:
			// A bounded string carries its declared length in the physicalType
			// ("varchar(3)", "char(2)"), which is directly the DDL type, so a
			// consumer reproduces the length exactly and an overlong value
			// faults with the same 22001 the live column raises. Parsing (not
			// just pattern-matching) mirrors the numeric path: a hand-edited
			// length that overflows a machine int fails closed here.
			if _, ok := stringParts(col.PhysicalType); ok {
				return col.PhysicalType, true
			}
			return "", false
		}
	case odcs.LogicalInteger:
		switch col.PhysicalType {
		case "smallint", "integer", "bigint":
			// Each integer width renders EXACTLY, never widened, so a value the
			// live column's width rejects (22003) faults downstream rather than
			// at the live cutover.
			return col.PhysicalType, true
		}
		return "", false
	case odcs.LogicalBoolean:
		if col.PhysicalType == "boolean" {
			return "boolean", true
		}
		return "", false
	case odcs.LogicalNumber:
		switch col.PhysicalType {
		case "double":
			return "double precision", true
		case "real":
			return "real", true
		default:
			if _, _, ok := numericParts(col.PhysicalType); ok {
				return col.PhysicalType, true
			}
			return "", false
		}
	case odcs.LogicalTimestamp:
		switch col.PhysicalType {
		case "timestamptz":
			return "timestamptz", true
		case "timestamp":
			return "timestamp", true
		default:
			return "", false
		}
	case odcs.LogicalDate:
		if col.PhysicalType == "date" {
			return "date", true
		}
		return "", false
	case odcs.LogicalObject:
		if col.PhysicalType == "jsonb" {
			return "jsonb", true
		}
		return "", false
	default:
		return "", false
	}
}

// arrayElementDDLType resolves an array property's element to its Postgres DDL
// keyword and whether the element is a faithful scalar. The element type comes
// from the property's items descriptor; a missing items, a non-scalar element,
// or an enum/array element yields ok=false so the array fails closed (a
// consumer never approximates a multi-dimensional or non-scalar array element).
func arrayElementDDLType(col odcs.Property) (string, bool) {
	if col.Items == nil {
		return "", false
	}
	// The element is itself a scalar property: it carries no enum quality rule
	// and no nested array, so scalarDDLType places it directly.
	return scalarDDLType(*col.Items)
}

// Nullable reports whether a column accepts NULL. ODCS Required is nullability
// inverted (Required=true means NOT NULL), and an unspecified Required (nil)
// means the source never asserted NOT NULL, so the column is nullable.
func Nullable(col odcs.Property) bool {
	return col.Required == nil || !*col.Required
}

// MaxCharacterLength returns the DECLARED character width of a bounded string
// column (a live varchar(n)/char(n), or a hand-bounded text column) and whether
// the column declares one. It is the single READER of the width, the
// counterpart to the single writer (WithCharacterWidth): it reads the native
// ODCS carrier (logicalTypeOptions.maxLength) first and falls back to the
// declared length a varchar(n)/char(n) physicalType carries, so a hand-authored
// contract that declares only the physical length is still checked. An
// unbounded text/varchar/char column, a non-string column, and an enum all read
// ok=false: they carry no width, so a consumer applies no check rather than a
// guess.
func MaxCharacterLength(col odcs.Property) (int, bool) {
	if IsEnum(col) || col.LogicalType != odcs.LogicalString {
		return 0, false
	}
	length, bounded := stringParts(col.PhysicalType)
	if col.PhysicalType != "text" && !bounded {
		// Not a string encoding that can carry a width (uuid rides logicalType
		// string with a format).
		return 0, false
	}
	if col.LogicalTypeOptions != nil && col.LogicalTypeOptions.MaxLength != nil {
		return *col.LogicalTypeOptions.MaxLength, true
	}
	if length != nil {
		return *length, true
	}
	return 0, false
}

// NumericPrecisionScale returns a numeric column's declared precision and scale
// read back out of its physicalType ("numeric(12,2)"), and whether the column
// is a declared-precision numeric at all. A non-numeric column, a binary float,
// and an unconstrained numeric all read ok=false: they carry no (p,s) bound. A
// declared precision with no scale reads scale 0, Postgres's own semantics for
// numeric(p).
func NumericPrecisionScale(col odcs.Property) (precision, scale int, ok bool) {
	if col.LogicalType != odcs.LogicalNumber {
		return 0, 0, false
	}
	p, s, ok := numericParts(col.PhysicalType)
	if !ok || p == nil {
		return 0, 0, false
	}
	if s == nil {
		return *p, 0, true
	}
	return *p, *s, true
}

// PrimaryKeyColumns returns a table's primary-key column names in key order
// (primaryKeyPosition). A table with no key column returns nil. The order is
// the declared key order so a consumer's PRIMARY KEY clause reproduces it.
func PrimaryKeyColumns(t odcs.SchemaObject) []string {
	type keyed struct {
		name string
		pos  int
	}
	var keys []keyed
	for _, col := range t.Properties {
		if col.PrimaryKey != nil && *col.PrimaryKey {
			pos := 0
			if col.PrimaryKeyPosition != nil {
				pos = *col.PrimaryKeyPosition
			}
			keys = append(keys, keyed{col.Name, pos})
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.SliceStable(keys, func(i, j int) bool { return keys[i].pos < keys[j].pos })
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = k.name
	}
	return names
}

// SortedTableNames returns the contract's table names in sorted order, for
// where a deterministic order matters (rendering DDL, rendering instructions,
// asserting in tests) so the output does not depend on declared order.
func SortedTableNames(c odcs.Contract) []string {
	names := make([]string, 0, len(c.Schema))
	for _, t := range c.Schema {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	return names
}

// numericParts parses a numeric physicalType into its optional precision and
// scale and whether the string is a well-formed numeric. "numeric" yields
// (nil, nil, true); "numeric(12)" yields (12, nil, true); "numeric(12,2)"
// yields (12, 2, true). Anything else yields ok=false. It is the reverse of the
// physicalType NumericColumn writes.
//
// The pattern matches an arbitrary-length digit run, so a hand-edited contract
// can carry a precision/scale far larger than any Postgres numeric (or one that
// overflows a machine int). parseDigits fails closed on any such value rather
// than wrapping it into a garbage in-range int.
func numericParts(physical string) (precision, scale *int, ok bool) {
	m := numericPhysicalPattern.FindStringSubmatch(physical)
	if m == nil {
		return nil, nil, false
	}
	if m[1] != "" {
		precision, ok = parseDigits(m[1])
		if !ok {
			return nil, nil, false
		}
	}
	if m[2] != "" {
		scale, ok = parseDigits(m[2])
		if !ok {
			return nil, nil, false
		}
	}
	return precision, scale, true
}

// stringParts parses a bounded-string physicalType into its optional declared
// length and whether the string is a well-formed varchar/char. "varchar" yields
// (nil, true); "varchar(3)" yields (3, true); anything outside the grammar
// yields ok=false. It shares parseDigits' overflow fail-closing with the
// numeric grammar so a pathological hand-edited length is rejected rather than
// wrapped.
func stringParts(physical string) (length *int, ok bool) {
	m := stringPhysicalPattern.FindStringSubmatch(physical)
	if m == nil {
		return nil, false
	}
	if m[1] != "" {
		length, ok = parseDigits(m[1])
		if !ok {
			return nil, false
		}
	}
	return length, true
}

// parseDigits converts a digit run the pattern already matched into a pointer,
// failing closed (ok=false) when the value does not fit a machine int.
// strconv.Atoi reports both non-numeric input (which the pattern excludes) and
// out-of-range overflow, so a pathological hand-edited length or precision is
// rejected rather than wrapped into a garbage in-range value.
func parseDigits(s string) (*int, bool) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return nil, false
	}
	return &n, true
}
