// Package pgcontract generates a FAITHFUL destination data contract from a live
// Postgres database: it introspects the public schema through pgintrospect and
// composes those catalog facts, via the odcsdest builders and the B1 type
// mapping, into an odcs.Contract that carries exactly the destination's shape —
// tables, exact-width columns (a bounded varchar/char keeps its length, each
// integer its width, a numeric its precision and scale), primary keys, unique
// constraints, verbatim CHECK constraints, single-column foreign keys (the
// structural triple), and enum labels.
//
// It is the FAITHFUL CORE only. The contract it returns describes what the
// destination DECLARES and nothing more; the delivery- and mirror-shaped POLICY
// a platform layers on a destination contract — producer-optionality folding
// (MP-3), merge-key selection (MP-2), surrogate/generated-column and
// destination-not-null markers, safe column defaults, and a foreign key's
// natural-key resolution columns — is applied by the caller as a post-pass over
// this faithful contract, never here. A column's producer-side signals
// (identity, generated, default) ride through on the pgintrospect.Column facts
// for that policy layer to read; this package reads only the destination's RAW
// nullability.
//
// It fails CLOSED: a column whose Postgres type is outside the reproducible
// vocabulary is an error naming the column, never a guessed or dropped column,
// and the assembled contract is validated before it is returned. It targets the
// "public" schema only, the deliberate choice that keeps a managed Postgres
// (e.g. Supabase) safe to point at.
package pgcontract

import (
	"context"
	"errors"
	"fmt"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/pgintrospect"
)

// Options carries the caller's exclusion policy. ExcludeTables names
// platform-internal tables to withhold from the contract by name (a shared
// control-plane database passes its own bookkeeping tables here; a dedicated
// destination passes none). ExcludeColumns names columns to withhold across
// every table, dropping whole any key or constraint that spans one. Both are
// values the caller supplies, never hardcoded here, so the same generator
// serves a shared database and a dedicated one.
type Options struct {
	ExcludeTables  []string
	ExcludeColumns []string
}

// Generate introspects a Postgres destination through q and returns the
// faithful destination contract (see the package doc). q is the combined
// read/exec surface a *sql.DB satisfies; Generate performs catalog READS only
// (the exec surface is part of the signature because the pgintrospect gateway a
// caller shares exposes it, but the faithful generation never writes). Every
// introspection query is scoped to the public schema.
//
// It runs each introspection query and JOINS their errors, so a database that
// has gone away surfaces every query failure at once rather than just the
// first. An unsupported column type is reported separately, as a fail-closed
// contract error listing every offending column, because it is a contract
// problem an operator corrects rather than a database failure. The returned
// contract is validated before it is returned, so a caller never receives one
// that would be rejected downstream.
func Generate(ctx context.Context, q pgintrospect.ExecQuerier, opts Options) (odcs.Contract, error) {
	if q == nil {
		return odcs.Contract{}, errors.New("pgcontract: querier must be non-nil")
	}
	excludedTable := toSet(opts.ExcludeTables)
	excludedColumn := toSet(opts.ExcludeColumns)

	// Run every introspection query and JOIN their errors (each tagged with the
	// read it came from), so a database that has gone away surfaces every query
	// failure at once rather than just the first.
	rawTables, tablesErr := pgintrospect.BaseTables(ctx, q, "public")
	columns, columnsErr := pgintrospect.Columns(ctx, q)
	rawPKs, pkErr := pgintrospect.PrimaryKeys(ctx, q)
	rawUniques, uniqueErr := pgintrospect.UniqueConstraints(ctx, q)
	rawChecks, checkErr := pgintrospect.CheckConstraints(ctx, q)
	rawFKs, fkErr := pgintrospect.ForeignKeys(ctx, q)
	if err := errors.Join(
		wrapRead("base tables", tablesErr),
		wrapRead("columns", columnsErr),
		wrapRead("primary keys", pkErr),
		wrapRead("unique constraints", uniqueErr),
		wrapRead("check constraints", checkErr),
		wrapRead("foreign keys", fkErr),
	); err != nil {
		return odcs.Contract{}, err
	}

	// Resolve each enum column's labels with a second, DB-touching pass (the
	// labels are not in information_schema.columns). A query failure here is a
	// database error, distinct from the fail-closed "USER-DEFINED type is not an
	// enum" the empty-label set drives inside assemble.
	enums, err := resolveEnumLabels(ctx, q, columns, excludedTable, excludedColumn)
	if err != nil {
		return odcs.Contract{}, err
	}

	return assemble(facts{
		tables:  includedTables(rawTables, excludedTable),
		columns: columns,
		pks:     includedPrimaryKeys(rawPKs, excludedColumn),
		uniques: includedUniques(rawUniques, excludedColumn),
		checks:  includedChecks(rawChecks, excludedColumn),
		fks:     singleColumnForeignKeys(rawFKs, excludedColumn),
		enums:   enums,
	}, excludedColumn)
}

// resolveEnumLabels reads the ordered labels of every enum column, keyed by the
// enum's (schema, name) so a type used on many columns is read once. It is
// schema-qualified (see pgintrospect.EnumValues) so a same-named enum in
// another schema never interleaves its labels. An excluded table's or column's
// enum is not read (it never reaches the contract). A USER-DEFINED type that is
// not an enum resolves to an empty set, which assemble turns into a fail-closed
// type error; a genuine query failure is returned as an error here.
func resolveEnumLabels(ctx context.Context, q pgintrospect.Querier, columns []pgintrospect.Column, excludedTable, excludedColumn map[string]struct{}) (map[enumKey][]string, error) {
	out := map[enumKey][]string{}
	for _, col := range columns {
		if col.DataType != userDefinedDataType {
			continue
		}
		if _, skip := excludedTable[col.Table]; skip {
			continue
		}
		if _, skip := excludedColumn[col.Name]; skip {
			continue
		}
		key := enumKey{col.UDTSchema, col.UDTName}
		if _, done := out[key]; done {
			continue
		}
		labels, err := pgintrospect.EnumValues(ctx, q, key.schema, key.name)
		if err != nil {
			return nil, fmt.Errorf("pgcontract: enum labels for %q.%q: %w", key.schema, key.name, err)
		}
		out[key] = labels
	}
	return out, nil
}

// wrapRead tags an introspection read's error with the read it came from,
// keeping Generate's error prefixes uniform, and passes a nil error through so
// errors.Join drops the successful reads.
func wrapRead(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("pgcontract: read %s: %w", name, err)
}
