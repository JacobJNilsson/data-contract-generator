// Package pgintrospect is the live-Postgres catalog gateway: every
// information_schema and pg_constraint read the destination-contract path
// performs against a relational database, plus the identifier quoting that
// renders catalog-sourced names into SQL, live here and nowhere else.
//
// Each function returns the catalog's FACTS and nothing more. What a fact
// MEANS for a generated contract — which tables to exclude, which foreign keys
// a contract represents, how a column's producer-side signals fold into its
// nullability — is POLICY that lives with the caller (pgcontract and, beyond
// it, an orchestrator layering delivery/mirror conventions), never here. The
// queries were consolidated from a single relational consumer verbatim: each
// method's SQL is the exact text that consumer carried.
//
// Why one home: catalog reads encode information_schema quirks and identifier
// quoting, and quoting bugs are injection bugs. A single package makes the next
// tightening land everywhere at once rather than leaving a duplicated copy
// exploitable.
//
// The read surface is an interface (Querier) satisfied by *sql.DB and *sql.Tx
// alike, so a caller reading on a pool and a caller reading inside a
// transaction share one gateway. NormalizeCheckExpression additionally writes
// (a savepoint-scoped scratch constraint), so it takes the combined
// ExecQuerier and needs a live server — it cannot be faithfully faked.
package pgintrospect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Querier is the read surface every catalog query runs through, satisfied by
// *sql.DB and *sql.Tx alike: a fidelity check reads inside transactions while
// the contract generator reads on a pool.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Execer is the statement surface the non-read catalog operations
// (PinSearchPath and NormalizeCheckExpression's savepoint-scoped scratch
// constraint) run through, satisfied by *sql.DB and *sql.Tx. It is separate
// from Querier because almost every catalog operation is a pure read.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ExecQuerier is the combined surface NormalizeCheckExpression needs: it
// executes the savepoint-scoped scratch DDL AND reads the resulting
// pg_constraint row on the SAME transaction (a transaction sees its own DDL),
// so the two surfaces must be one handle. *sql.Tx satisfies it.
type ExecQuerier interface {
	Execer
	Querier
}

// QuoteIdentifier renders a SQL identifier safely by wrapping it in double
// quotes and doubling any embedded double quote, the Postgres rule for a
// quoted identifier. It is the single choke point through which every
// catalog-sourced schema, table, column, and constraint name passes before it
// is concatenated into SQL, so an untrusted identifier can never break out of
// its quotes into executable SQL. Identifiers are still grammar-validated by
// the contract layer; quoting is the belt-and-braces second barrier.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteLiteral renders a string as a SQL string literal, doubling embedded
// single quotes. It quotes VALUES (never identifiers); every NAME is quoted
// with QuoteIdentifier instead.
//
// It doubles single quotes ONLY and so relies on standard_conforming_strings
// = on (the Postgres default since 9.1), under which a backslash in a regular
// '...' literal is an ordinary character with no escape meaning. That GUC is
// the default and is not changed anywhere this code runs, so this is safe
// today. If a future deployment ever turned standard_conforming_strings off,
// this would need to emit an E'...' literal with backslashes doubled too; it
// is left as plain single-quote doubling to avoid the added rendering surface
// while the GUC dependency holds.
func QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// BaseTables returns one schema's BASE TABLE names, sorted. The schema name is
// a bound parameter (a value comparison, never an identifier position), so the
// catalog query carries no injection surface.
func BaseTables(ctx context.Context, q Querier, schema string) ([]string, error) {
	const query = `SELECT table_name FROM information_schema.tables
	                WHERE table_schema = $1 AND table_type = 'BASE TABLE'
	                ORDER BY table_name`
	var tables []string
	err := forEachRow(ctx, q, query, []any{schema}, func(rows *sql.Rows) error {
		var name string
		err := rows.Scan(&name)
		if err == nil {
			tables = append(tables, name)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return tables, nil
}

// SchemaExists reports whether a schema is present. The schema name is a bound
// parameter, so the lookup carries no injection surface.
func SchemaExists(ctx context.Context, q Querier, schema string) (bool, error) {
	const query = `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.schemata WHERE schema_name = $1
		)`
	exists := false
	err := forEachRow(ctx, q, query, []any{schema}, func(rows *sql.Rows) error {
		return rows.Scan(&exists)
	})
	return exists, err
}

// Column is one row of the contract generator's column introspection: the
// column's identity and nullability plus the signals compound-type resolution
// needs (the enum type name via UDTName, the array element's friendly
// data_type and the declared array dimension count) and the producer-side
// signals that a POLICY layer reads to decide whether a NOT NULL column is
// populated by the pipeline or by the database (IsIdentity, IsGenerated,
// HasDefault). What the signals MEAN is the caller's policy; this struct only
// carries the catalog's facts.
type Column struct {
	// Table and Name identify the column.
	Table string
	Name  string

	// DataType is information_schema.columns.data_type.
	DataType string

	// Nullable is is_nullable ("YES"/"NO").
	Nullable string

	// IsIdentity is is_identity ("YES" for an identity column), IsGenerated
	// is is_generated ("ALWAYS" for a generated column), and HasDefault is
	// (column_default IS NOT NULL) scanned as a bool.
	IsIdentity  string
	IsGenerated string
	HasDefault  bool

	// IsSequenceDefault is true when column_default is a nextval(...) call,
	// the shape a SERIAL/BIGSERIAL column carries.
	IsSequenceDefault bool

	// Default is the column's raw normalised DEFAULT expression
	// (information_schema.columns.column_default), NULL when the column
	// declares none.
	Default sql.NullString

	// UDTName and UDTSchema name the column's underlying type and the schema
	// it lives in (an enum column's own type).
	UDTName   string
	UDTSchema string

	// ElementDataType is the array element's friendly data_type (empty for a
	// non-array column) and ArrayDims the declared dimension count
	// (pg_attribute.attndims, 0 for a non-array column).
	ElementDataType string
	ArrayDims       int

	// CharMaxLength carries a bounded string column's declared length
	// (character_maximum_length); Postgres reports it NULL for text, for an
	// unbounded varchar, and for every non-string column.
	CharMaxLength sql.NullInt64

	// NumericPrecision and NumericScale carry a numeric column's declared
	// precision and scale; NULL for an unconstrained numeric and for every
	// non-numeric column.
	NumericPrecision sql.NullInt64
	NumericScale     sql.NullInt64

	// ElementTypeMod is the type modifier (pg_attribute.atttypmod) of the
	// column's own pg_attribute row: -1 when the type is unconstrained, the
	// packed precision/scale when it is constrained. For an ARRAY column the
	// array's atttypmod carries the ELEMENT's modifier.
	ElementTypeMod int
}

// Columns returns every public-schema column in (table, ordinal) order.
//
// The query joins two extra catalogs onto information_schema.columns so an
// array column is resolved in one pass: element_types gives the element's
// friendly data_type (the same vocabulary the scalar map keys on),
// pg_attribute.attndims gives the declared dimension count so a
// multi-dimensional array (attndims > 1) fails closed, and
// pg_attribute.atttypmod carries the array element's type modifier so a
// precision-bearing numeric element (numeric(p,s)[]) fails closed rather than
// being silently widened. A non-array column's element data_type is NULL, its
// attndims is 0, and its scalar numeric precision is read from
// c.numeric_precision instead, so the array joins are inert for scalars and
// enums.
//
// It targets the "public" schema ONLY, the deliberate choice that keeps a
// managed Postgres safe to point at: a provider's internal schemas hold types
// outside the vocabulary, so introspecting only public returns only the
// customer's own tables.
func Columns(ctx context.Context, q Querier) ([]Column, error) {
	const query = `SELECT c.table_name, c.column_name, c.data_type, c.is_nullable,
	                      c.is_identity, c.is_generated, (c.column_default IS NOT NULL) AS has_default,
	                      COALESCE(c.column_default LIKE 'nextval(%', false) AS is_sequence_default,
	                      c.column_default,
	                      c.udt_name, c.udt_schema,
	                      COALESCE(et.data_type, '') AS element_data_type,
	                      COALESCE(at.attndims, 0) AS array_dims,
	                      c.character_maximum_length,
	                      c.numeric_precision, c.numeric_scale,
	                      COALESCE(at.atttypmod, -1) AS element_type_mod
	                 FROM information_schema.columns c
	                 LEFT JOIN information_schema.element_types et
	                   ON c.table_catalog = et.object_catalog
	                  AND c.table_schema = et.object_schema
	                  AND c.table_name = et.object_name
	                  AND et.object_type = 'TABLE'
	                  AND c.dtd_identifier = et.collection_type_identifier
	                 LEFT JOIN pg_namespace ns ON ns.nspname = c.table_schema
	                 LEFT JOIN pg_class cl ON cl.relname = c.table_name AND cl.relnamespace = ns.oid
	                 LEFT JOIN pg_attribute at ON at.attrelid = cl.oid AND at.attname = c.column_name
	                WHERE c.table_schema = 'public'
	                ORDER BY c.table_name, c.ordinal_position`
	var out []Column
	err := forEachRow(ctx, q, query, nil, func(rows *sql.Rows) error {
		var c Column
		err := rows.Scan(&c.Table, &c.Name, &c.DataType, &c.Nullable, &c.IsIdentity, &c.IsGenerated, &c.HasDefault, &c.IsSequenceDefault, &c.Default, &c.UDTName, &c.UDTSchema, &c.ElementDataType, &c.ArrayDims, &c.CharMaxLength, &c.NumericPrecision, &c.NumericScale, &c.ElementTypeMod)
		if err == nil {
			out = append(out, c)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EnumValues returns an enum type's allowed labels in declared order
// (enumsortorder), the order Postgres uses for the enum's comparisons. A
// (schema, name) with no rows in pg_enum (a USER-DEFINED type that is not an
// enum, or a dropped type) returns an empty slice, which the contract
// generator treats as a fail-closed unsupported type rather than an empty
// enum.
//
// The lookup is SCHEMA-QUALIFIED on purpose. An enum name alone is not unique
// across a database: a managed Postgres (e.g. Supabase) defines enums in its
// own auth/storage/realtime schemas, so a same-named enum in another schema
// would, with a name-only filter, make pg_enum return the interleaved union of
// both enums' labels (ordered by enumsortorder across types) and corrupt the
// contract. Joining pg_namespace and filtering on the column's own udt_schema
// resolves the EXACT type the column references.
func EnumValues(ctx context.Context, q Querier, schema, typeName string) ([]string, error) {
	const query = `SELECT e.enumlabel
	                 FROM pg_enum e
	                 JOIN pg_type t ON t.oid = e.enumtypid
	                 JOIN pg_namespace n ON n.oid = t.typnamespace
	                WHERE t.typname = $1 AND n.nspname = $2
	                ORDER BY e.enumsortorder`
	var labels []string
	err := forEachRow(ctx, q, query, []any{typeName, schema}, func(rows *sql.Rows) error {
		var label string
		err := rows.Scan(&label)
		if err == nil {
			labels = append(labels, label)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return labels, nil
}

// PrimaryKeys returns each public-schema table's primary-key column names in
// key order, keyed by table name. A table with no primary key is simply
// absent.
func PrimaryKeys(ctx context.Context, q Querier) (map[string][]string, error) {
	const query = `SELECT tc.table_name, kcu.column_name
	                 FROM information_schema.table_constraints tc
	                 JOIN information_schema.key_column_usage kcu
	                   ON kcu.constraint_name = tc.constraint_name
	                  AND kcu.constraint_schema = tc.constraint_schema
	                WHERE tc.table_schema = 'public' AND tc.constraint_type = 'PRIMARY KEY'
	                ORDER BY tc.table_name, kcu.ordinal_position`
	pks := map[string][]string{}
	err := forEachRow(ctx, q, query, nil, func(rows *sql.Rows) error {
		var table, column string
		err := rows.Scan(&table, &column)
		if err == nil {
			pks[table] = append(pks[table], column)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return pks, nil
}

// UniqueConstraint is one public-schema UNIQUE constraint: its table, name,
// and spanned columns in key order.
type UniqueConstraint struct {
	Table   string
	Name    string
	Columns []string
}

// UniqueConstraints returns every public-schema non-primary UNIQUE constraint
// in deterministic (table, constraint name, ordinal) encounter order.
func UniqueConstraints(ctx context.Context, q Querier) ([]UniqueConstraint, error) {
	const query = `SELECT tc.table_name, tc.constraint_name, kcu.column_name
	                 FROM information_schema.table_constraints tc
	                 JOIN information_schema.key_column_usage kcu
	                   ON kcu.constraint_name = tc.constraint_name
	                  AND kcu.constraint_schema = tc.constraint_schema
	                WHERE tc.table_schema = 'public' AND tc.constraint_type = 'UNIQUE'
	                ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position`
	var out []UniqueConstraint
	idx := map[[2]string]int{}
	err := forEachRow(ctx, q, query, nil, func(rows *sql.Rows) error {
		var table, constraint, column string
		err := rows.Scan(&table, &constraint, &column)
		if err != nil {
			return err
		}
		k := [2]string{table, constraint}
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, UniqueConstraint{Table: table, Name: constraint})
		}
		out[i].Columns = append(out[i].Columns, column)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ForeignKey is one public-schema FOREIGN KEY constraint: its referencing
// table and columns, the referenced table and columns (pairwise, in
// constraint order), and the constraint's declared column count so a consumer
// can tell a composite key from a single-column one.
type ForeignKey struct {
	Table             string
	Name              string
	NumColumns        int
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}

// ForeignKeys returns every public-schema FOREIGN KEY in deterministic
// (table, constraint name, ordinal) encounter order. It reads pg_constraint
// (contype = 'f') and unnests conkey/confkey against pg_attribute to recover
// the local referencing columns and the referenced columns, scoping BOTH the
// referencing and the referenced table to the public schema so a cross-schema
// FK (a reference into a provider's internal schema on a managed Postgres)
// never enters the public-only contract.
func ForeignKeys(ctx context.Context, q Querier) ([]ForeignKey, error) {
	const query = `SELECT rel.relname AS table_name,
	                      con.conname,
	                      att.attname AS column_name,
	                      frel.relname AS referenced_table,
	                      fatt.attname AS referenced_column,
	                      array_length(con.conkey, 1) AS num_cols
	                 FROM pg_constraint con
	                 JOIN pg_class rel ON rel.oid = con.conrelid
	                 JOIN pg_namespace ns ON ns.oid = rel.relnamespace
	                 JOIN pg_class frel ON frel.oid = con.confrelid
	                 JOIN pg_namespace fns ON fns.oid = frel.relnamespace
	                 JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY
	                        AS k(local_attnum, ref_attnum, ord) ON true
	                 JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.local_attnum
	                 JOIN pg_attribute fatt ON fatt.attrelid = con.confrelid AND fatt.attnum = k.ref_attnum
	                WHERE con.contype = 'f'
	                  AND ns.nspname = 'public'
	                  AND fns.nspname = 'public'
	                ORDER BY rel.relname, con.conname, k.ord`
	var out []ForeignKey
	idx := map[[2]string]int{}
	err := forEachRow(ctx, q, query, nil, func(rows *sql.Rows) error {
		var table, constraint, column, refTable, refColumn string
		var numCols int
		if err := rows.Scan(&table, &constraint, &column, &refTable, &refColumn, &numCols); err != nil {
			return err
		}
		k := [2]string{table, constraint}
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, ForeignKey{Table: table, Name: constraint, ReferencedTable: refTable, NumColumns: numCols})
		}
		out[i].Columns = append(out[i].Columns, column)
		out[i].ReferencedColumns = append(out[i].ReferencedColumns, refColumn)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CheckConstraint is one public-schema CHECK constraint: its table, name, the
// verbatim expression as pg_get_expr renders it, and the columns it
// constrains (empty when the check constrains no column).
type CheckConstraint struct {
	Table      string
	Name       string
	Expression string
	Columns    []string
}

// CheckConstraints returns every public-schema CHECK constraint in
// deterministic (table, constraint name) encounter order, capturing BOTH
// single-column and table-level multi-column checks. It reads pg_constraint
// (contype = 'c') directly, taking the expression from pg_get_expr(conbin,
// conrelid) and the constrained columns from conkey unnested against
// pg_attribute.
//
// Reading pg_constraint rather than information_schema.check_constraints is
// what makes a multi-column check faithful: information_schema fabricates
// SYNTHETIC per-column NOT NULL checks that would need a lossy substring
// filter, while pg_constraint carries only real CHECK constraints under
// contype = 'c' (a column's NOT NULL is attnotnull, not a 'c' row).
func CheckConstraints(ctx context.Context, q Querier) ([]CheckConstraint, error) {
	// One row per (constraint, constrained column): the conkey columns are
	// LEFT-unnested so a check that constrains no column still yields one row
	// (with a NULL column). The expression is identical on every row of a
	// constraint.
	const query = `SELECT rel.relname AS table_name,
	                      con.conname,
	                      pg_get_expr(con.conbin, con.conrelid) AS expr,
	                      att.attname AS column_name
	                 FROM pg_constraint con
	                 JOIN pg_class rel ON rel.oid = con.conrelid
	                 JOIN pg_namespace ns ON ns.oid = rel.relnamespace
	                 LEFT JOIN LATERAL unnest(con.conkey) AS k(attnum) ON true
	                 LEFT JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
	                WHERE con.contype = 'c' AND ns.nspname = 'public'
	                ORDER BY rel.relname, con.conname`
	var out []CheckConstraint
	idx := map[[2]string]int{}
	err := forEachRow(ctx, q, query, nil, func(rows *sql.Rows) error {
		var table, constraint string
		var expr, column sql.NullString
		if err := rows.Scan(&table, &constraint, &expr, &column); err != nil {
			return err
		}
		k := [2]string{table, constraint}
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, CheckConstraint{Table: table, Name: constraint, Expression: expr.String})
		}
		if column.Valid {
			out[i].Columns = append(out[i].Columns, column.String)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PinSearchPath pins the current transaction's search_path to one schema, so
// pg_get_expr renders a schema-local type (a run-scoped enum in a CHECK cast)
// UNQUALIFIED, exactly as the destination renders its own public-schema types.
// set_config(..., true) is transaction-local, so the pin dies with the
// caller's transaction and never leaks into a pooled session.
func PinSearchPath(ctx context.Context, e Execer, schema string) error {
	_, err := e.ExecContext(ctx, `SELECT set_config('search_path', $1, true)`, schema)
	return err
}

// ColumnFacts is one column's rejection-deciding catalog facts, a fidelity
// snapshot unit: exactly the self-contained facts that decide whether a value
// is REJECTED (data type, declared length, numeric precision and scale,
// nullability, and the underlying type name that distinguishes one enum or
// array element from another). Absent numeric facts are normalised to -1 (the
// catalog reports them NULL) so the struct stays comparable.
type ColumnFacts struct {
	Table            string
	Column           string
	DataType         string
	CharMax          int64
	NumericPrecision int64
	NumericScale     int64
	Nullable         string
	UDTName          string
}

// ColumnFactsOf reads every column's rejection-deciding facts for one schema
// from information_schema, in (table, ordinal) order. The absent-fact
// COALESCEs keep the scan NULL-free so ColumnFacts stays a plain comparable
// struct.
func ColumnFactsOf(ctx context.Context, q Querier, schema string) ([]ColumnFacts, error) {
	const query = `SELECT table_name, column_name, data_type,
	       COALESCE(character_maximum_length, -1),
	       COALESCE(numeric_precision, -1),
	       COALESCE(numeric_scale, -1),
	       is_nullable, udt_name
	  FROM information_schema.columns
	 WHERE table_schema = $1
	 ORDER BY table_name, ordinal_position`
	var out []ColumnFacts
	err := forEachRow(ctx, q, query, []any{schema}, func(rows *sql.Rows) error {
		var f ColumnFacts
		if err := rows.Scan(&f.Table, &f.Column, &f.DataType, &f.CharMax, &f.NumericPrecision, &f.NumericScale, &f.Nullable, &f.UDTName); err != nil {
			return err
		}
		out = append(out, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ConstraintFacts is one constraint's catalog facts for the fidelity
// snapshot: its table, its contype ('c'/'u'/'p'), the expression text for a
// CHECK (pg_get_expr, which re-renders the parsed tree, so byte-equal text on
// both sides means semantically equal constraints over equally typed
// columns), and the ordered constrained column names for a key.
type ConstraintFacts struct {
	Table      string
	Type       string
	Expression string
	Columns    string
}

// ConstraintFactsOf reads one schema's CHECK, UNIQUE, and PRIMARY KEY
// constraints from pg_constraint, in (table, constraint name) order. contype
// 'f' (FOREIGN KEY) is deliberately outside the filter: a fidelity check that
// keeps FKs out of scope must not see them.
func ConstraintFactsOf(ctx context.Context, q Querier, schema string) ([]ConstraintFacts, error) {
	const query = `SELECT rel.relname,
	       con.contype::text,
	       COALESCE(pg_get_expr(con.conbin, con.conrelid), ''),
	       COALESCE((SELECT string_agg(att.attname, ', ' ORDER BY k.ord)
	          FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
	          JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum), '')
	  FROM pg_constraint con
	  JOIN pg_class rel ON rel.oid = con.conrelid
	  JOIN pg_namespace ns ON ns.oid = rel.relnamespace
	 WHERE ns.nspname = $1 AND con.contype IN ('c', 'u', 'p')
	 ORDER BY rel.relname, con.conname`
	var out []ConstraintFacts
	err := forEachRow(ctx, q, query, []any{schema}, func(rows *sql.Rows) error {
		var f ConstraintFacts
		if err := rows.Scan(&f.Table, &f.Type, &f.Expression, &f.Columns); err != nil {
			return err
		}
		out = append(out, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// normCheckConstraint is the scratch constraint name NormalizeCheckExpression
// applies and rolls back. The leading underscore is an identifier the contract
// grammar never produces, so the scratch name can never collide with a real
// constraint on the relation it is briefly attached to.
const normCheckConstraint = "_pgintrospect_check_norm"

// NormalizeCheckExpression parses one CHECK expression on the EXECUTING server
// and returns that server's own deparse of it (issue #192): the expression is
// applied to the named relation as a savepoint-scoped scratch constraint, its
// stored tree is read back through pg_get_expr, and the savepoint is rolled
// back so the caller's transaction keeps exactly the state it had.
//
// Why this exists: pg_get_expr's deparse does not round-trip through the
// parser textually. The destination deparses an IN-authored varchar CHECK with
// a WHOLE-ARRAY cast ("(ARRAY['buy'::character varying, ...])::text[]");
// re-parsing that text folds the cast onto each element, so a copy created
// FROM the deparsed text deparses differently ("ARRAY[('buy'::character
// varying)::text, ...]") — same constraint, two renderings, no version skew.
// Comparing deparsed strings from TWO servers is therefore unsound; pushing
// both sides through ONE server's parse-and-deparse first makes string
// equality mean tree equality on that server.
//
// The expression is rendered verbatim into the scratch DDL. It is pg_get_expr
// output read from a trusted catalog (the destination's or the mirror's own);
// schema and table are quoted identifiers.
//
// An expression the server cannot apply to the relation (a column the relation
// lacks, an unknown function or type) is returned VERBATIM with no error: the
// caller's textual comparison then still sees the two sides differ and fails
// closed, naming the expression — the same fail-open-parse, fail-closed-compare
// posture the membership reduction takes. Plumbing failures a healthy server
// never produces (a failed savepoint, a failed read-back) are errors.
func NormalizeCheckExpression(ctx context.Context, q ExecQuerier, schema, table, expr string) (string, error) {
	// The savepoint scopes the scratch constraint: rolling back to it removes
	// the constraint whether the ALTER succeeded or not (and clears the
	// aborted-subtransaction state a failed ALTER leaves), so the caller's
	// transaction commits none of this.
	if _, err := q.ExecContext(ctx, "SAVEPOINT "+normCheckConstraint); err != nil {
		return "", fmt.Errorf("savepoint for CHECK normalization: %w", err)
	}
	// NOT VALID skips validating existing rows: only the parsed tree is
	// wanted, never a data scan.
	alter := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		QuoteIdentifier(schema), QuoteIdentifier(table), QuoteIdentifier(normCheckConstraint), expr)
	_, alterErr := q.ExecContext(ctx, alter)
	normalized, readErr := "", error(nil)
	if alterErr == nil {
		normalized, readErr = normalizedCheckOf(ctx, q, schema, table)
	}
	if _, err := q.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+normCheckConstraint); err != nil {
		return "", fmt.Errorf("roll back CHECK normalization savepoint: %w", err)
	}
	if alterErr != nil {
		return expr, nil
	}
	if readErr != nil {
		return "", readErr
	}
	return normalized, nil
}

// normalizedCheckOf reads the scratch constraint's deparse back from
// pg_constraint on the same transaction that just applied it. A missing row is
// an error, never an empty pass: the caller only reads after a successful
// ALTER, so an absent constraint means the read went to the wrong place.
func normalizedCheckOf(ctx context.Context, q Querier, schema, table string) (string, error) {
	const query = `SELECT pg_get_expr(con.conbin, con.conrelid)
	  FROM pg_constraint con
	  JOIN pg_class rel ON rel.oid = con.conrelid
	  JOIN pg_namespace ns ON ns.oid = rel.relnamespace
	 WHERE ns.nspname = $1 AND rel.relname = $2 AND con.conname = $3`
	expr, found := "", false
	err := forEachRow(ctx, q, query, []any{schema, table, normCheckConstraint}, func(rows *sql.Rows) error {
		found = true
		return rows.Scan(&expr)
	})
	if err != nil {
		return "", fmt.Errorf("read back normalized CHECK: %w", err)
	}
	if !found {
		return "", errors.New("read back normalized CHECK: scratch constraint not found in pg_constraint")
	}
	return expr, nil
}

// forEachRow runs one query with its bind arguments and feeds every row
// through scan: the single query-iterate-scan choke point every catalog read
// shares, so the error paths are exercised once. The scan error short-circuits
// the loop and wins over rows.Err, because the first failure is the one worth
// reporting.
func forEachRow(ctx context.Context, q Querier, query string, args []any, scan func(rows *sql.Rows) error) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	scanErr := error(nil)
	for scanErr == nil && rows.Next() {
		scanErr = scan(rows)
	}
	if scanErr == nil {
		scanErr = rows.Err()
	}
	return scanErr
}
