package pgcontract

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
	"github.com/JacobJNilsson/data-contract-generator/pgintrospect"
)

// userDefinedDataType is the data_type string information_schema.columns
// reports for an enum (and other USER-DEFINED types). The mapping routes it to
// the enum path, resolving labels from pg_enum; a USER-DEFINED type carrying no
// enum labels fails closed in odcsdest.PostgresProperty rather than reaching
// the contract as an empty enum.
const userDefinedDataType = "USER-DEFINED"

// enumKey identifies one enum type by the (schema, name) pair its labels are
// resolved under, so a type used on many columns is read once and looked up by
// the same key the column carries.
type enumKey struct{ schema, name string }

// facts is the already-read catalog state assemble turns into a contract: the
// tables (filtered to the included set, sorted), every column (assemble skips
// an excluded one), and the per-table constraints with their exclusion policy
// already applied. enums maps each enum type to its ordered labels, resolved by
// a DB read the caller (Generate) performs; a USER-DEFINED column whose type is
// absent or empty here fails closed as a non-enum. Holding the facts as plain
// values keeps assemble a pure function the fast tier can drive without a
// database.
type facts struct {
	tables  []string
	columns []pgintrospect.Column
	pks     map[string][]string
	uniques map[string][]odcs.UniqueConstraint
	checks  map[string][]odcs.CheckConstraint
	fks     map[string][]odcs.ForeignKey
	enums   map[enumKey][]string
}

// assemble composes the read catalog facts into a FAITHFUL odcs.Contract: each
// included table becomes a schema object carrying its columns (in ordinal
// order, at their exact widths and RAW destination nullability), primary key,
// unique constraints, verbatim CHECK constraints, and single-column foreign
// keys (the structural triple only). It fails CLOSED, joining every column
// whose Postgres type is outside the reproducible vocabulary so an operator
// sees them all at once, and validates the assembled contract before returning
// it.
//
// It carries the faithful shape ONLY. The producer-optionality folding (MP-3),
// merge-key selection (MP-2), surrogate/generated markers, column defaults,
// destination-not-null markers, and foreign-key natural-key resolution are
// POLICY a caller layers on top afterwards; assemble never applies them, so the
// contract it returns describes exactly what the destination declares.
func assemble(f facts, excludedColumn map[string]struct{}) (odcs.Contract, error) {
	objs := make([]odcs.SchemaObject, 0, len(f.tables))
	var typeErrs []error
	for _, name := range f.tables {
		var props []odcs.Property
		for _, col := range f.columns {
			if col.Table != name {
				continue
			}
			if _, skip := excludedColumn[col.Name]; skip {
				continue
			}
			prop, err := mapColumn(col, f.enums)
			if err != nil {
				typeErrs = append(typeErrs, fmt.Errorf("table %q %w", name, err))
				continue
			}
			props = append(props, prop)
		}
		obj := odcsdest.Table(name, props, f.pks[name])
		obj = odcsdest.WithUnique(obj, f.uniques[name]...)
		obj = odcsdest.WithChecks(obj, f.checks[name]...)
		obj = odcsdest.WithForeignKeys(obj, f.fks[name]...)
		objs = append(objs, obj)
	}
	if len(typeErrs) > 0 {
		// Fail closed on any column the vocabulary cannot represent: a generated
		// contract that silently dropped or mistyped a column would mislead both
		// an agent and a mirror.
		return odcs.Contract{}, fmt.Errorf("pgcontract: generate: %w", errors.Join(typeErrs...))
	}
	contract := odcsdest.NewContract(objs...)
	if err := validate(contract); err != nil {
		return odcs.Contract{}, fmt.Errorf("pgcontract: generated contract is invalid: %w", err)
	}
	return contract, nil
}

// mapColumn turns one catalog column into its ODCS property through the pure
// B1 mapper (odcsdest.PostgresProperty), which owns the type vocabulary and the
// fail-closed rules. The nullability passed is the column's RAW destination
// nullability (is_nullable), NOT a producer-optionality fold: the faithful
// contract encodes exactly the destination's NOT NULL, and a policy layer folds
// database-populated columns to optional afterwards. An enum column's labels
// come from the pre-resolved enums map; an absent or empty entry makes the
// mapper fail closed on a non-enum USER-DEFINED type.
func mapColumn(col pgintrospect.Column, enums map[enumKey][]string) (odcs.Property, error) {
	desc := odcsdest.PostgresColumnType{
		Name:             col.Name,
		Nullable:         col.Nullable == "YES",
		DataType:         col.DataType,
		UDTName:          col.UDTName,
		NumericPrecision: nullInt(col.NumericPrecision),
		NumericScale:     nullInt(col.NumericScale),
		CharMaxLength:    nullInt(col.CharMaxLength),
		ElementDataType:  col.ElementDataType,
		ElementTypeMod:   col.ElementTypeMod,
		ArrayDims:        col.ArrayDims,
	}
	if col.DataType == userDefinedDataType {
		desc.EnumLabels = enums[enumKey{col.UDTSchema, col.UDTName}]
	}
	return odcsdest.PostgresProperty(desc)
}

// includedTables drops the excluded tables from the (sorted) base-table list,
// so the contract's table order stays deterministic.
func includedTables(raw []string, excluded map[string]struct{}) []string {
	var out []string
	for _, name := range raw {
		if _, skip := excluded[name]; !skip {
			out = append(out, name)
		}
	}
	return out
}

// includedPrimaryKeys drops a primary key whole when it includes an excluded
// column: the contract withholds that column, so it cannot declare a key that
// references it. A key over only included columns is kept in its key order.
func includedPrimaryKeys(raw map[string][]string, excluded map[string]struct{}) map[string][]string {
	out := map[string][]string{}
	for table, columns := range raw {
		if !anyExcluded(columns, excluded) {
			out[table] = columns
		}
	}
	return out
}

// includedUniques groups the non-primary UNIQUE constraints by table, dropping
// a constraint whole when it spans an excluded column. The gateway returns them
// in deterministic (table, constraint name) order, so appending per table
// preserves a stable constraint order.
func includedUniques(raw []pgintrospect.UniqueConstraint, excluded map[string]struct{}) map[string][]odcs.UniqueConstraint {
	out := map[string][]odcs.UniqueConstraint{}
	for _, u := range raw {
		if anyExcluded(u.Columns, excluded) {
			continue
		}
		out[u.Table] = append(out[u.Table], odcs.UniqueConstraint{Name: u.Name, Columns: u.Columns})
	}
	return out
}

// includedChecks groups the CHECK constraints by table, carrying each
// expression VERBATIM and dropping a check whole when it constrains an excluded
// column (conkey names exactly the constrained columns, so the drop is
// precise). Both single-column and table-level multi-column checks are kept
// (#155).
func includedChecks(raw []pgintrospect.CheckConstraint, excluded map[string]struct{}) map[string][]odcs.CheckConstraint {
	out := map[string][]odcs.CheckConstraint{}
	for _, ck := range raw {
		if anyExcluded(ck.Columns, excluded) {
			continue
		}
		out[ck.Table] = append(out[ck.Table], odcs.CheckConstraint{Name: ck.Name, Expression: ck.Expression})
	}
	return out
}

// singleColumnForeignKeys groups the SINGLE-COLUMN foreign keys by referencing
// table, carrying the structural triple only (local column, referenced table,
// referenced column). A multi-column (composite) foreign key is not corrupted
// into a single-column shape this cannot represent: it is skipped with a logged
// note so the omission is visible. A foreign key whose local column is excluded
// is dropped whole, the same fail-closed direction the unique and check
// exclusion take.
func singleColumnForeignKeys(raw []pgintrospect.ForeignKey, excluded map[string]struct{}) map[string][]odcs.ForeignKey {
	out := map[string][]odcs.ForeignKey{}
	for _, g := range raw {
		if g.NumColumns != 1 || len(g.Columns) != 1 {
			log.Printf("pgcontract: skipping multi-column foreign key %q on table %q (single-column foreign keys only)", g.Name, g.Table)
			continue
		}
		if _, skip := excluded[g.Columns[0]]; skip {
			continue
		}
		out[g.Table] = append(out[g.Table], odcs.ForeignKey{
			Column:           g.Columns[0],
			ReferencedTable:  g.ReferencedTable,
			ReferencedColumn: g.ReferencedColumns[0],
		})
	}
	return out
}

// nullInt converts a nullable integer scanned from a catalog column into a
// *int: a NULL (an unconstrained numeric, an unbounded string) yields nil so
// the column carries no bound, a present value yields a pointer to it.
func nullInt(n sql.NullInt64) *int {
	if !n.Valid {
		return nil
	}
	v := int(n.Int64)
	return &v
}

// toSet builds a lookup set from a name slice; nil yields an empty set.
func toSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return set
}

// anyExcluded reports whether any of a constraint's columns is excluded: the
// shared drop-whole rule the primary-key, unique, and check grouping apply to a
// constraint touching a withheld column.
func anyExcluded(columns []string, excluded map[string]struct{}) bool {
	for _, c := range columns {
		if _, skip := excluded[c]; skip {
			return true
		}
	}
	return false
}
