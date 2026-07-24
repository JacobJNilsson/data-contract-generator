package pgcontract

import (
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
)

// maxIdentifierLength is the byte budget for a single SQL identifier the
// faithful contract accepts. Postgres truncates identifiers at 63 bytes
// (NAMEDATALEN-1); the contract rejects anything longer rather than let two
// distinct names silently collapse to one, which would corrupt fidelity.
const maxIdentifierLength = 63

// identifierPattern is the conservative grammar for a contract identifier: an
// ASCII letter or underscore followed by ASCII letters, digits, or
// underscores. It is deliberately narrower than what Postgres would
// quote-accept — a generated contract has no need for spaces, punctuation, or
// Unicode in identifiers — and is a defence-in-depth barrier behind quoting.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validate checks the FAITHFUL contract fails closed, joining every violation
// so a malformed contract surfaces all its problems at once. It enforces the
// structural invariants a faithfully introspected contract must hold: at least
// one table; unique, valid table identifiers; per table at least one column,
// unique valid column identifiers, and a faithfully representable type; enum
// label consistency; and unique/foreign-key column references that resolve to
// real columns.
//
// It validates the faithful shape ONLY. The policy-layer markers a caller adds
// afterwards (merge key, generated columns, destination-not-null, foreign-key
// natural-key resolution) carry their own validation with that policy; they are
// out of scope here.
func validate(c odcs.Contract) error {
	var errs []error
	if len(c.Schema) == 0 {
		errs = append(errs, errors.New("contract must declare at least one table"))
	}
	seenTables := make(map[string]struct{}, len(c.Schema))
	for _, table := range c.Schema {
		if err := validIdentifier("table", table.Name); err != nil {
			errs = append(errs, err)
		} else if _, dup := seenTables[table.Name]; dup {
			errs = append(errs, fmt.Errorf("duplicate table name %q", table.Name))
		} else {
			seenTables[table.Name] = struct{}{}
		}
		errs = append(errs, validateTable(table)...)
	}
	errs = append(errs, validateEnumConsistency(c)...)
	return errors.Join(errs...)
}

// validateTable checks one schema object, returning every violation (not
// joined, so validate can join across tables): column presence and uniqueness,
// valid column identifiers, a faithfully representable type per column, and
// unique/foreign-key references that resolve to real columns.
func validateTable(t odcs.SchemaObject) []error {
	var errs []error
	if len(t.Properties) == 0 {
		errs = append(errs, fmt.Errorf("table %q must declare at least one column", t.Name))
	}
	cols := make(map[string]struct{}, len(t.Properties))
	for _, col := range t.Properties {
		if err := validIdentifier("column", col.Name); err != nil {
			errs = append(errs, fmt.Errorf("table %q: %w", t.Name, err))
			continue
		}
		if _, dup := cols[col.Name]; dup {
			errs = append(errs, fmt.Errorf("table %q: duplicate column name %q", t.Name, col.Name))
			continue
		}
		cols[col.Name] = struct{}{}
		if err := validateProperty(t.Name, col); err != nil {
			errs = append(errs, err)
		}
	}

	// A unique constraint must span real columns, so the mirror never renders a
	// UNIQUE over a column the table lacks. The introspected path always names
	// real columns (an excluded-column constraint is dropped whole), so this is
	// a defensive fail-closed guard against a hand-edited contract.
	for _, u := range odcs.UniqueConstraints(t.CustomProperties) {
		for _, c := range u.Columns {
			if _, ok := cols[c]; !ok {
				errs = append(errs, fmt.Errorf("table %q: unique constraint %q references unknown column %q", t.Name, u.Name, c))
			}
		}
	}

	// A foreign key's local (referencing) column must exist on the table. Only
	// the LOCAL column is reference-checked: the referenced table may
	// legitimately lie outside the public-schema scope, so its presence is not
	// required (an unresolvable FK is represented faithfully, not rejected).
	for _, fk := range odcs.ForeignKeys(t.CustomProperties) {
		if _, ok := cols[fk.Column]; !ok {
			errs = append(errs, fmt.Errorf("table %q: foreign key references unknown column %q", t.Name, fk.Column))
		}
	}
	return errs
}

// validateProperty checks one property's type is faithfully representable by
// the B1 accessors a consumer reads: an enum must name a valid type and carry
// at least one non-empty label; every other column must resolve to a Postgres
// DDL type. The check IS the accessor — odcsdest.PostgresDDLType returns
// ok=false for exactly the shapes a consumer cannot render — so a contract that
// validates is one every accessor can place.
func validateProperty(table string, col odcs.Property) error {
	if odcsdest.IsEnum(col) {
		if err := validIdentifier("enum type", odcsdest.EnumName(col)); err != nil {
			return fmt.Errorf("table %q column %q: %w", table, col.Name, err)
		}
		labels := odcsdest.EnumLabels(col)
		if len(labels) == 0 {
			return fmt.Errorf("table %q column %q: enum must declare at least one label", table, col.Name)
		}
		for _, label := range labels {
			if label == "" {
				return fmt.Errorf("table %q column %q: enum label must be non-empty", table, col.Name)
			}
		}
		return nil
	}
	if _, ok := odcsdest.PostgresDDLType(col); !ok {
		return fmt.Errorf("table %q column %q: unsupported column type (logicalType %q, physicalType %q)", table, col.Name, col.LogicalType, col.PhysicalType)
	}
	return nil
}

// validateEnumConsistency rejects a contract in which two enum columns share an
// enum type name but declare DIFFERENT ordered label sets: a consumer stands
// one type up per distinct enum NAME, so two columns naming the same enum must
// agree on its labels or the second would silently get the wrong values. A
// faithfully introspected public-schema contract never trips this (its enum
// names are unique); the guard fails a cross-schema-collision or hand-edited
// contract closed.
func validateEnumConsistency(c odcs.Contract) []error {
	labels := map[string][]string{}
	var errs []error
	for _, table := range c.Schema {
		for _, col := range table.Properties {
			if !odcsdest.IsEnum(col) {
				continue
			}
			name := odcsdest.EnumName(col)
			got := odcsdest.EnumLabels(col)
			if first, seen := labels[name]; seen {
				if !slices.Equal(first, got) {
					errs = append(errs, fmt.Errorf("table %q column %q: enum %q declares labels %v, conflicting with the same-named enum's labels %v elsewhere in the contract", table.Name, col.Name, name, got, first))
				}
				continue
			}
			labels[name] = got
		}
	}
	return errs
}

// validIdentifier checks that name is a usable contract identifier: non-empty,
// within the length budget, and matching the conservative grammar. It is the
// single gate every table, column, and enum name passes, so a name Postgres
// would truncate or one smuggling punctuation is rejected at validation.
func validIdentifier(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%s name must be non-empty", kind)
	}
	if len(name) > maxIdentifierLength {
		return fmt.Errorf("%s name %q exceeds %d bytes", kind, name, maxIdentifierLength)
	}
	if !identifierPattern.MatchString(name) {
		return fmt.Errorf("%s name %q must match %s", kind, name, identifierPattern.String())
	}
	return nil
}
