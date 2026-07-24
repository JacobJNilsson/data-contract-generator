// Package odcsdest is the destination-encoding sibling of odcsemit: where
// odcsemit renders dcg's file-analyser results into ODCS, odcsdest builds and
// reads the ODCS encoding of a RELATIONAL destination's shape. It owns the
// column builders that WRITE a Postgres column's type, nullability, and
// width/precision as an ODCS logical/physical pair (build.go), the accessors
// that READ them back (accessors.go), and the pure Postgres-type MAPPING that
// turns a catalog type descriptor into one of those builders' output
// (postgres.go) — the DB introspection that produces the descriptor is a
// caller's concern, not this package's.
//
// The encoding is deliberately reversible: every builder here is the single
// WRITER of a fact and every accessor its single READER, so a property a
// builder produces round-trips through a saved ODCS document unchanged. Enum
// columns reuse the library's existing enum representation (odcsemit.
// EnumProperty / ReadEnumLabels) rather than a second copy, so a destination
// enum and a source enum are the same shape.
//
// Two fidelity rules are load-bearing and carried faithfully from the
// relational source:
//
//   - EXACT integer width (smallint/integer/bigint) and EXACT varchar(n)/
//     char(n) length: a value the live column's width rejects (an out-of-range
//     integer, 22003; an overlong string, 22001) must be rejected by anything
//     built from the contract too, so widths are never widened.
//   - EXACT numeric(p,s): a money-shaped column is reproduced with its declared
//     precision and scale rather than a bare numeric.
//
// A type outside the reproducible vocabulary fails CLOSED (an error, never a
// guessed column), so a contract only ever describes shapes a consumer can
// faithfully stand up.
package odcsdest

import (
	"fmt"
	"sort"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsemit"
)

const (
	// contractIDPrefix is the stable prefix the derived document id carries.
	// A destination contract has no externally assigned identifier, so the id
	// is derived from its sorted table names: a deterministic value so the
	// emitted artifact is byte-stable across regenerations of the same shape.
	contractIDPrefix = "destination-contract"

	// contractVersion is the semantic version stamped on the emitted document,
	// the contract artifact's own version (distinct from the ODCS standard
	// version, apiVersion). A fixed 1.0.0 keeps the emitted artifact byte-stable
	// for regen-and-diff guards downstream.
	contractVersion = "1.0.0"

	// tablePhysicalType is the physicalType every schema object (a table)
	// declares. It tells a reader the schema object maps to a relational table.
	tablePhysicalType = "table"
)

// TextColumn builds a text column.
func TextColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalString, "text", "")
}

// VarcharColumn builds a character varying column carrying its optional
// declared length in the physicalType: VarcharColumn(n, false, nil) is an
// unbounded varchar, VarcharColumn(n, false, &3) is varchar(3). The length is
// carried (and a consumer reproduces it) because a bounded varchar REJECTS an
// overlong value with SQLSTATE 22001, so widening it to text would accept
// values the live destination rejects. A bounded column also carries the same
// length in the native ODCS home for a string bound (logicalTypeOptions.
// maxLength, via WithCharacterWidth), so a validator reads the width without
// parsing the physicalType: one introspection read feeds both carriers, kept
// in sync by construction.
func VarcharColumn(name string, nullable bool, length *int) odcs.Property {
	return boundedStringColumn("varchar", name, nullable, length)
}

// CharColumn builds a blank-padded character column carrying its optional
// declared length in the physicalType, the char(n) sibling of VarcharColumn:
// CharColumn(n, false, &3) is char(3), CharColumn(n, false, nil) is a bare
// char (which Postgres defines as char(1)). Like varchar, char(n) rejects an
// overlong value with 22001, so the length is part of the self-contained type
// a consumer must reproduce; like varchar, a bounded char also carries the
// length as logicalTypeOptions.maxLength.
func CharColumn(name string, nullable bool, length *int) odcs.Property {
	return boundedStringColumn("char", name, nullable, length)
}

// boundedStringColumn builds the shared varchar/char shape: the declared
// length rides in the physicalType (the mirror-DDL carrier) and, when present,
// in logicalTypeOptions.maxLength (the validator's carrier). An unbounded
// column carries neither: no length to enforce, no width to check.
func boundedStringColumn(keyword, name string, nullable bool, length *int) odcs.Property {
	p := scalarColumn(name, nullable, odcs.LogicalString, numericTypeSQL(keyword, length, nil), "")
	if length != nil {
		p = WithCharacterWidth(p, *length)
	}
	return p
}

// WithCharacterWidth returns the string column carrying the DECLARED character
// width of the live varchar(n)/char(n) column it describes, in the native ODCS
// home for a string bound (logicalTypeOptions.maxLength). It is the single
// WRITER of the width, the counterpart to the single reader
// (MaxCharacterLength); the bounded-string constructors attach it alongside
// the length they carry in the physicalType, and a hand-authored contract may
// attach it to a bare text column to declare a bound the destination type does
// not.
//
// The width is CONSTRAINT knowledge, not a type change: the column's
// logicalType stays string, so an UNBOUNDED text column is byte-identical to
// one built before this marker existed, and only a genuinely bounded column
// carries it.
func WithCharacterWidth(p odcs.Property, maxLength int) odcs.Property {
	// Copy any existing options rather than mutating through the shared
	// pointer, so attaching a width to a column built from a reused property
	// value never reaches back into the original.
	opts := odcs.LogicalTypeOptions{}
	if p.LogicalTypeOptions != nil {
		opts = *p.LogicalTypeOptions
	}
	opts.MaxLength = &maxLength
	p.LogicalTypeOptions = &opts
	return p
}

// IntegerColumn builds a four-byte integer column. Its physicalType is
// "integer" and a consumer renders exactly that: each integer width carries
// through exactly (SmallintColumn / IntegerColumn / BigintColumn) so an
// out-of-range value the live integer rejects (22003) is rejected downstream
// rather than discovered at the live cutover.
func IntegerColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalInteger, "integer", "")
}

// SmallintColumn builds a two-byte integer column, reproduced exactly for the
// same out-of-range fidelity IntegerColumn's comment explains.
func SmallintColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalInteger, "smallint", "")
}

// BigintColumn builds an eight-byte integer column, reproduced exactly for the
// same out-of-range fidelity IntegerColumn's comment explains.
func BigintColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalInteger, "bigint", "")
}

// BooleanColumn builds a boolean column.
func BooleanColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalBoolean, "boolean", "")
}

// UUIDColumn builds a uuid column (a string logical type with a uuid format
// option, so the JSON Schema interchange turns it into a UUID).
func UUIDColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalString, "uuid", "uuid")
}

// DateColumn builds a date column (a date logical type with a date format
// option).
func DateColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalDate, "date", "date")
}

// TimestamptzColumn builds a time-zone-aware timestamp column.
func TimestamptzColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalTimestamp, "timestamptz", "")
}

// TimestampColumn builds a time-zone-naive timestamp column.
func TimestampColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalTimestamp, "timestamp", "")
}

// JSONBColumn builds a jsonb column.
func JSONBColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalObject, "jsonb", "")
}

// DoubleColumn builds a double-precision binary float column.
func DoubleColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalNumber, "double", "")
}

// RealColumn builds a single-precision binary float column.
func RealColumn(name string, nullable bool) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalNumber, "real", "")
}

// NumericColumn builds a numeric column carrying an optional declared precision
// and scale in its physicalType: NumericColumn(n, false, nil, nil) is a bare
// numeric, NumericColumn(n, false, p, nil) is numeric(p), NumericColumn(n,
// false, p, s) is numeric(p,s). A consumer reproduces the exact declared
// precision rather than widening it.
func NumericColumn(name string, nullable bool, precision, scale *int) odcs.Property {
	return scalarColumn(name, nullable, odcs.LogicalNumber, numericTypeSQL("numeric", precision, scale), "")
}

// EnumColumn builds an enum column: a string logical type whose physicalType is
// the enum type's own name and whose invalidValues quality rule carries the
// ordered labels. It reuses the library's enum encoding (odcsemit.EnumProperty)
// so a destination enum is the same shape as a source enum, adding only the
// column's nullability (which EnumProperty, built for the file-analyser path,
// does not carry).
func EnumColumn(name string, nullable bool, enumName string, labels []string) odcs.Property {
	p := odcsemit.EnumProperty(name, enumName, labels)
	p.Required = requiredFlag(nullable)
	return p
}

// ArrayColumn builds a one-dimensional array column over a scalar element. The
// element is one of the scalar builders above (its name is ignored); its
// logical/physical pair AND its format option become the array's items
// descriptor, and its physicalType the element of the "<element>[]" array
// physicalType.
//
// Carrying the element's LogicalTypeOptions (its format) into the items
// descriptor is load-bearing for element types whose JSON Schema format rides
// on the format option rather than on the logical type itself: a uuid element
// is LogicalString with format "uuid", so without the format the items
// descriptor degrades to a bare string and the interchange emits an untyped
// array element, silently losing the element's type.
func ArrayColumn(name string, nullable bool, element odcs.Property) odcs.Property {
	return odcs.Property{
		Name:         name,
		Required:     requiredFlag(nullable),
		LogicalType:  odcs.LogicalArray,
		PhysicalType: element.PhysicalType + "[]",
		Items: &odcs.Property{
			LogicalType:        element.LogicalType,
			PhysicalType:       element.PhysicalType,
			LogicalTypeOptions: element.LogicalTypeOptions,
		},
	}
}

// scalarColumn is the shared builder for a scalar column: name, nullability
// (carried as ODCS Required, true = NOT NULL), the logical/physical pair, and
// an optional format option (uuid, date). It is the one place the Required and
// format-option encoding lives.
func scalarColumn(name string, nullable bool, logical odcs.LogicalType, physical, format string) odcs.Property {
	p := odcs.Property{
		Name:         name,
		Required:     requiredFlag(nullable),
		LogicalType:  logical,
		PhysicalType: physical,
	}
	if format != "" {
		p.LogicalTypeOptions = &odcs.LogicalTypeOptions{Format: format}
	}
	return p
}

// requiredFlag encodes a column's nullability as the ODCS Required field: a NOT
// NULL column carries required=true, a nullable column carries nil so it
// marshals away, matching dcg's convention that an absent Required means the
// column is not asserted NOT NULL. The Nullable accessor reads nil as nullable,
// so the encoding round-trips.
func requiredFlag(nullable bool) *bool {
	if nullable {
		return nil
	}
	return boolPtr(true)
}

// boolPtr returns a pointer to b, for the ODCS Required field (true = NOT NULL).
func boolPtr(b bool) *bool { return &b }

// numericTypeSQL renders a numeric column's SQL type from its base keyword and
// optional precision and scale. An absent precision renders the bare keyword
// (an unconstrained numeric, which accepts any precision). A precision with no
// scale renders base(p) (Postgres's scale-0 form); a precision with a scale
// renders base(p,s). It is shared by the numeric and bounded-string builders.
func numericTypeSQL(base string, precision, scale *int) string {
	if precision == nil {
		return base
	}
	if scale == nil {
		return fmt.Sprintf("%s(%d)", base, *precision)
	}
	return fmt.Sprintf("%s(%d,%d)", base, *precision, *scale)
}

// Table builds a schema object (table) from its name, columns, and optional
// primary-key column names. The primary key marks each named column (in the
// given order) primaryKey with its 1-based position. Unique, check, and
// foreign-key constraints are attached separately (WithUnique / WithChecks /
// WithForeignKeys) so a constraint-free table carries no custom properties.
func Table(name string, columns []odcs.Property, primaryKey []string) odcs.SchemaObject {
	obj := odcs.SchemaObject{
		Name:         name,
		PhysicalName: name,
		PhysicalType: tablePhysicalType,
		Properties:   columns,
	}
	applyPrimaryKey(&obj, primaryKey)
	return obj
}

// applyPrimaryKey marks each named column primaryKey with its 1-based position
// on the schema object, in key order. A table with no primary key leaves every
// property's PrimaryKey nil (so it marshals away). The position carries the key
// order so PrimaryKeyColumns reproduces it.
func applyPrimaryKey(obj *odcs.SchemaObject, pk []string) {
	pos := make(map[string]int, len(pk))
	for i, name := range pk {
		pos[name] = i + 1
	}
	for i := range obj.Properties {
		if p, ok := pos[obj.Properties[i].Name]; ok {
			yes := true
			position := p
			obj.Properties[i].PrimaryKey = &yes
			obj.Properties[i].PrimaryKeyPosition = &position
		}
	}
}

// WithUnique returns the table with its UNIQUE constraints attached as a
// custom property. It delegates the value encoding to odcs (the owner of the
// dcg namespace); passing no constraints leaves the table unchanged.
func WithUnique(t odcs.SchemaObject, uniques ...odcs.UniqueConstraint) odcs.SchemaObject {
	if prop, ok := odcs.UniqueConstraintsProperty(uniques); ok {
		t.CustomProperties = append(t.CustomProperties, prop)
	}
	return t
}

// WithChecks returns the table with its CHECK constraints attached as a custom
// property, the check-constraint counterpart to WithUnique.
func WithChecks(t odcs.SchemaObject, checks ...odcs.CheckConstraint) odcs.SchemaObject {
	if prop, ok := odcs.CheckConstraintsProperty(checks); ok {
		t.CustomProperties = append(t.CustomProperties, prop)
	}
	return t
}

// WithForeignKeys returns the table with its single-column FOREIGN KEYS
// attached as a custom property, the foreign-key counterpart to WithUnique and
// WithChecks. Only the generic structural triple is carried; a delivery layer's
// natural-key resolution is its own concern.
func WithForeignKeys(t odcs.SchemaObject, fks ...odcs.ForeignKey) odcs.SchemaObject {
	if prop, ok := odcs.ForeignKeysProperty(fks); ok {
		t.CustomProperties = append(t.CustomProperties, prop)
	}
	return t
}

// NewContract builds a destination Contract from its schema objects, stamping
// the required ODCS top-level fields (apiVersion, kind, status, version) and a
// deterministic id derived from the sorted table names. Every built contract
// carries the same top-level shape a producer emits.
func NewContract(tables ...odcs.SchemaObject) odcs.Contract {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	return odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         contractID(names),
		Version:    contractVersion,
		Status:     odcs.StatusActive,
		Schema:     tables,
	}
}

// contractID derives the document's required id from the sorted table names: a
// stable, deterministic value so the emitted artifact is byte-stable across
// regenerations of the same shape. names must be sorted (NewContract sorts a
// copy).
func contractID(names []string) string {
	id := contractIDPrefix
	for _, n := range names {
		id += "-" + n
	}
	return id
}
