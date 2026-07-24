package odcs

// dcg constraint-fact custom-property keys. ODCS v3.1.0 has a first-class
// home for a column's primary-key membership and nullability, but NOT for a
// table's named, possibly multi-column UNIQUE constraints, its row-level
// CHECK constraints, or its cross-table FOREIGN KEYS. A destination contract
// derived from a relational source must carry these structural facts so a
// consumer reproducing the table (a mirror standing it up, an authoring
// surface describing it) sees the same constraints the live system enforces.
//
// They ride in a schema object's customProperties under the namespaced keys
// below, the same mechanism the odcs package already owns for the
// source-parse facts (CustomKeySourceFormat and the CSV delimiter/encoding/
// header keys). Defining them HERE makes the shared dcg namespace
// single-owned: a producer building a destination contract and a consumer
// reading one both stand on one definition of each key and its value shape,
// rather than each redefining them and risking a silent divergence.
//
// The values are STRUCTURED (a unique constraint is a name plus an ordered
// column list; a check is a name plus a verbatim expression; a foreign key is
// a local column plus the referenced table and column), carried as nested
// maps so they round-trip through YAML and JSON. They marshal away when a
// table declares none, so the emitted ODCS for a constraint-free table is
// unchanged.
//
// These keys carry only the GENERIC structural facts. A platform layering
// delivery or mirror conventions onto the same document (a merge-key
// designation, generated-column markers, or a foreign key's natural-key
// resolution columns) owns those keys and value fields itself; the foreign
// key reader here deliberately ignores any additional keys a producer wrote
// into the same map, reading only the structural triple this package owns.
const (
	// CustomKeyUniqueConstraints carries a table's non-primary UNIQUE
	// constraints as a list of {name, columns} maps.
	CustomKeyUniqueConstraints = "dcgUniqueConstraints"

	// CustomKeyCheckConstraints carries a table's CHECK constraints as a list
	// of {name, expression} maps, each expression verbatim as the source
	// rendered it.
	CustomKeyCheckConstraints = "dcgCheckConstraints"

	// CustomKeyForeignKeys carries a table's single-column FOREIGN KEYS as a
	// list of {column, referencedTable, referencedColumn} maps: the local
	// referencing column, the referenced table, and the referenced column it
	// targets (normally that table's surrogate primary key).
	CustomKeyForeignKeys = "dcgForeignKeys"
)

// UniqueConstraint is a uniqueness constraint over one or more columns of a
// table. A primary key is expressed natively on the properties (their
// primaryKey flag); this expresses a table's non-primary UNIQUE constraints,
// each of which a consumer reproduces so a duplicate row is rejected exactly
// as the live system would.
type UniqueConstraint struct {
	// Name is the constraint identifier.
	Name string

	// Columns are the column names the constraint spans, in declared order.
	Columns []string
}

// CheckConstraint is a row-level CHECK constraint on a table. The Expression
// is the source's own check text, carried verbatim so a consumer reproduces
// the exact predicate the live system enforces.
type CheckConstraint struct {
	// Name is the constraint identifier.
	Name string

	// Expression is the boolean SQL expression the CHECK enforces (e.g.
	// "amount >= 0"), verbatim as the source rendered it.
	Expression string
}

// ForeignKey is a single-column FOREIGN KEY on a table: the local
// (referencing) column points at a referenced table's column. It carries the
// generic structural reference only; any natural-key resolution a delivery
// layer needs is that layer's own concern, layered onto the same
// customProperty by its owner and ignored here.
type ForeignKey struct {
	// Column is the local (referencing) column name.
	Column string

	// ReferencedTable is the name of the table the foreign key points at.
	ReferencedTable string

	// ReferencedColumn is the referenced table's column the foreign key
	// targets, normally that table's surrogate primary key.
	ReferencedColumn string
}

// UniqueConstraintsProperty builds the customProperty carrying a table's
// UNIQUE constraints, or returns ok=false when there are none (so a
// constraint-free table emits no custom property and stays byte-identical to
// one built without this fact). The value is a list of {name, columns} maps so
// it round-trips through YAML and JSON unchanged.
func UniqueConstraintsProperty(uniques []UniqueConstraint) (CustomProperty, bool) {
	if len(uniques) == 0 {
		return CustomProperty{}, false
	}
	list := make([]any, len(uniques))
	for i, u := range uniques {
		cols := make([]any, len(u.Columns))
		for j, c := range u.Columns {
			cols[j] = c
		}
		list[i] = map[string]any{"name": u.Name, "columns": cols}
	}
	return CustomProperty{Property: CustomKeyUniqueConstraints, Value: list}, true
}

// CheckConstraintsProperty builds the customProperty carrying a table's CHECK
// constraints, or returns ok=false when there are none. The value is a list of
// {name, expression} maps.
func CheckConstraintsProperty(checks []CheckConstraint) (CustomProperty, bool) {
	if len(checks) == 0 {
		return CustomProperty{}, false
	}
	list := make([]any, len(checks))
	for i, c := range checks {
		list[i] = map[string]any{"name": c.Name, "expression": c.Expression}
	}
	return CustomProperty{Property: CustomKeyCheckConstraints, Value: list}, true
}

// ForeignKeysProperty builds the customProperty carrying a table's
// single-column foreign keys, or returns ok=false when there are none. The
// value is a list of {column, referencedTable, referencedColumn} maps so it
// round-trips through YAML and JSON unchanged.
func ForeignKeysProperty(fks []ForeignKey) (CustomProperty, bool) {
	if len(fks) == 0 {
		return CustomProperty{}, false
	}
	list := make([]any, len(fks))
	for i, fk := range fks {
		list[i] = map[string]any{
			"column":           fk.Column,
			"referencedTable":  fk.ReferencedTable,
			"referencedColumn": fk.ReferencedColumn,
		}
	}
	return CustomProperty{Property: CustomKeyForeignKeys, Value: list}, true
}

// UniqueConstraints reads a table's UNIQUE constraints back out of its custom
// properties. A list with none returns nil. Each entry arrives as a nested
// string-keyed map (whether decoded from YAML or JSON), which this coerces
// back to the typed form, skipping any malformed entry defensively.
func UniqueConstraints(props []CustomProperty) []UniqueConstraint {
	raw, ok := CustomProp(props, CustomKeyUniqueConstraints)
	if !ok {
		return nil
	}
	entries, ok := raw.([]any)
	if !ok {
		return nil
	}
	var uniques []UniqueConstraint
	for _, e := range entries {
		m, ok := asStringMap(e)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		uniques = append(uniques, UniqueConstraint{Name: name, Columns: asStringList(m["columns"])})
	}
	return uniques
}

// CheckConstraints reads a table's CHECK constraints back out of its custom
// properties, the mirror image of UniqueConstraints.
func CheckConstraints(props []CustomProperty) []CheckConstraint {
	raw, ok := CustomProp(props, CustomKeyCheckConstraints)
	if !ok {
		return nil
	}
	entries, ok := raw.([]any)
	if !ok {
		return nil
	}
	var checks []CheckConstraint
	for _, e := range entries {
		m, ok := asStringMap(e)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		expr, _ := m["expression"].(string)
		checks = append(checks, CheckConstraint{Name: name, Expression: expr})
	}
	return checks
}

// ForeignKeys reads a table's single-column foreign keys back out of its
// custom properties. A list with none returns nil. Each entry arrives as a
// nested string-keyed map; this reads only the generic structural triple
// (column, referencedTable, referencedColumn) and ignores any additional keys
// a delivery layer wrote into the same map, skipping any malformed entry
// defensively.
func ForeignKeys(props []CustomProperty) []ForeignKey {
	raw, ok := CustomProp(props, CustomKeyForeignKeys)
	if !ok {
		return nil
	}
	entries, ok := raw.([]any)
	if !ok {
		return nil
	}
	var fks []ForeignKey
	for _, e := range entries {
		m, ok := asStringMap(e)
		if !ok {
			continue
		}
		column, _ := m["column"].(string)
		referencedTable, _ := m["referencedTable"].(string)
		referencedColumn, _ := m["referencedColumn"].(string)
		fks = append(fks, ForeignKey{
			Column:           column,
			ReferencedTable:  referencedTable,
			ReferencedColumn: referencedColumn,
		})
	}
	return fks
}

// asStringMap coerces a customProperty entry to a string-keyed map. A value
// decoded from JSON is already map[string]any; one decoded from YAML through
// an any target is map[string]any too (yaml.v3 uses string keys for
// string-keyed mappings), so both forms are handled and anything else yields
// ok=false.
func asStringMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

// asStringList coerces a value to a string slice, accepting both []string (the
// in-memory build) and []any of strings (the decoded form). A non-list or a
// non-string element yields nil so a malformed column list is read as absent
// rather than partially.
func asStringList(v any) []string {
	switch list := v.(type) {
	case []string:
		return list
	case []any:
		out := make([]string, 0, len(list))
		for _, e := range list {
			s, ok := e.(string)
			if !ok {
				return nil
			}
			out = append(out, s)
		}
		return out
	default:
		return nil
	}
}
