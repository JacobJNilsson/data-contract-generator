//go:build integration

// These tests run every pgintrospect query against a REAL Postgres (via the
// internal/pgtest harness), so the SQL's meaning is asserted where only a live
// server can prove it: that the catalog joins resolve real widths, arrays, and
// enums; and — the fidelity-critical case — that NormalizeCheckExpression
// (issue #192) really collapses two textually-different deparse renderings of
// ONE constraint to a single string on the executing server. #192 cannot be
// faked: the round-trip depends on the server's own parser folding a
// whole-array cast onto its elements, so this is the only place it is proven.
package pgintrospect_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/internal/pgtest"
	"github.com/JacobJNilsson/data-contract-generator/pgintrospect"
)

// pg is the shared Postgres for this test binary, started in TestMain.
var pg *pgtest.Postgres

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	got, err := pgtest.Start(ctx)
	if errors.Is(err, pgtest.ErrNoDocker) {
		fmt.Fprintln(os.Stderr, "pgintrospect: skipping integration tier:", err)
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgintrospect: start Postgres:", err)
		return 1
	}
	defer func() { _ = got.Close(ctx) }()
	pg = got
	return m.Run()
}

// execAll runs each statement in order, failing the test on the first error.
func execAll(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// TestCatalogReadsAgainstRealPostgres exercises the shape-and-constraint reads
// against a live server on one rich schema: exact widths and an array (Columns),
// a primary key, a unique constraint, a single-column and a composite foreign
// key, and a multi-column CHECK (#155). It asserts the raw catalog facts the
// contract layer builds on, not the contract itself.
func TestCatalogReadsAgainstRealPostgres(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	execAll(t, db,
		`CREATE TABLE cat_assets (
			id   bigint PRIMARY KEY,
			isin varchar(12) NOT NULL,
			CONSTRAINT cat_assets_isin_unique UNIQUE (isin)
		)`,
		`CREATE TABLE cat_orders (
			order_id   bigint PRIMARY KEY,
			asset_id   bigint NOT NULL REFERENCES cat_assets(id),
			code       varchar(3) NOT NULL,
			tags       text[],
			amount     numeric(12,2) NOT NULL,
			note       text,
			CONSTRAINT cat_orders_amount_nonneg CHECK (amount >= 0),
			CONSTRAINT cat_orders_note_rule CHECK ((code IS NULL) OR (note IS NOT NULL))
		)`,
	)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS cat_orders, cat_assets`)
	})

	tables, err := pgintrospect.BaseTables(ctx, db, "public")
	if err != nil {
		t.Fatalf("BaseTables() = %v", err)
	}
	if !contains(tables, "cat_assets") || !contains(tables, "cat_orders") {
		t.Fatalf("BaseTables() = %v, want it to include cat_assets and cat_orders", tables)
	}

	cols, err := pgintrospect.Columns(ctx, db)
	if err != nil {
		t.Fatalf("Columns() = %v", err)
	}
	byName := map[string]pgintrospect.Column{}
	for _, c := range cols {
		if c.Table == "cat_orders" {
			byName[c.Name] = c
		}
	}
	if got := byName["code"]; !got.CharMaxLength.Valid || got.CharMaxLength.Int64 != 3 || got.DataType != "character varying" || got.Nullable != "NO" {
		t.Errorf("cat_orders.code = %+v, want varchar(3) NOT NULL", got)
	}
	if got := byName["amount"]; got.DataType != "numeric" || !got.NumericPrecision.Valid || got.NumericPrecision.Int64 != 12 || !got.NumericScale.Valid || got.NumericScale.Int64 != 2 {
		t.Errorf("cat_orders.amount = %+v, want numeric(12,2)", got)
	}
	if got := byName["tags"]; got.DataType != "ARRAY" || got.ElementDataType != "text" || got.ArrayDims != 1 {
		t.Errorf("cat_orders.tags = %+v, want a one-dimensional text[]", got)
	}

	pks, err := pgintrospect.PrimaryKeys(ctx, db)
	if err != nil {
		t.Fatalf("PrimaryKeys() = %v", err)
	}
	if want := []string{"order_id"}; !reflect.DeepEqual(pks["cat_orders"], want) {
		t.Errorf("cat_orders primary key = %v, want %v", pks["cat_orders"], want)
	}

	uniques, err := pgintrospect.UniqueConstraints(ctx, db)
	if err != nil {
		t.Fatalf("UniqueConstraints() = %v", err)
	}
	foundUnique := false
	for _, u := range uniques {
		if u.Table == "cat_assets" && u.Name == "cat_assets_isin_unique" && reflect.DeepEqual(u.Columns, []string{"isin"}) {
			foundUnique = true
		}
	}
	if !foundUnique {
		t.Errorf("UniqueConstraints() = %+v, want cat_assets_isin_unique over [isin]", uniques)
	}

	fks, err := pgintrospect.ForeignKeys(ctx, db)
	if err != nil {
		t.Fatalf("ForeignKeys() = %v", err)
	}
	foundFK := false
	for _, fk := range fks {
		if fk.Table == "cat_orders" && fk.NumColumns == 1 && reflect.DeepEqual(fk.Columns, []string{"asset_id"}) &&
			fk.ReferencedTable == "cat_assets" && reflect.DeepEqual(fk.ReferencedColumns, []string{"id"}) {
			foundFK = true
		}
	}
	if !foundFK {
		t.Errorf("ForeignKeys() = %+v, want cat_orders.asset_id -> cat_assets(id)", fks)
	}

	checks, err := pgintrospect.CheckConstraints(ctx, db)
	if err != nil {
		t.Fatalf("CheckConstraints() = %v", err)
	}
	// The multi-column check (#155) is captured with BOTH its columns, proving
	// pg_constraint (not information_schema's synthetic per-column NOT NULL
	// rows) is the source.
	var multi pgintrospect.CheckConstraint
	for _, ck := range checks {
		if ck.Name == "cat_orders_note_rule" {
			multi = ck
		}
	}
	if multi.Name == "" || multi.Expression == "" {
		t.Fatalf("CheckConstraints() dropped the multi-column check cat_orders_note_rule: %+v", checks)
	}
	if !reflect.DeepEqual(multi.Columns, []string{"code", "note"}) {
		t.Errorf("cat_orders_note_rule columns = %v, want [code note]", multi.Columns)
	}
}

// TestEnumValuesSchemaQualified proves EnumValues resolves the EXACT type the
// column references: a same-named enum in another schema must not interleave
// its labels into the public enum's.
func TestEnumValuesSchemaQualified(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	execAll(t, db,
		`CREATE SCHEMA enum_decoy`,
		`CREATE TYPE enum_decoy.mood AS ENUM ('aaa_decoy', 'zzz_decoy')`,
		`CREATE TYPE public.mood AS ENUM ('happy', 'sad')`,
	)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `DROP TYPE IF EXISTS public.mood`)
		_, _ = db.ExecContext(ctx, `DROP SCHEMA IF EXISTS enum_decoy CASCADE`)
	})

	got, err := pgintrospect.EnumValues(ctx, db, "public", "mood")
	if err != nil {
		t.Fatalf("EnumValues() = %v", err)
	}
	if want := []string{"happy", "sad"}; !reflect.DeepEqual(got, want) {
		t.Errorf("EnumValues(public, mood) = %v, want ONLY the public enum's labels %v", got, want)
	}
	// A USER-DEFINED type that is not an enum resolves to an empty set (the
	// contract layer treats that as a fail-closed unsupported type).
	empty, err := pgintrospect.EnumValues(ctx, db, "public", "does_not_exist")
	if err != nil || len(empty) != 0 {
		t.Errorf("EnumValues(missing) = %v, %v; want an empty set and no error", empty, err)
	}
}

// TestSchemaExistsAndFacts proves SchemaExists and the fidelity-snapshot facts
// reads (ColumnFactsOf, ConstraintFactsOf) run against a real server.
func TestSchemaExistsAndFacts(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	if ok, err := pgintrospect.SchemaExists(ctx, db, "public"); err != nil || !ok {
		t.Fatalf("SchemaExists(public) = %v, %v; want true, nil", ok, err)
	}
	if ok, err := pgintrospect.SchemaExists(ctx, db, "no_such_schema"); err != nil || ok {
		t.Fatalf("SchemaExists(absent) = %v, %v; want false, nil", ok, err)
	}

	execAll(t, db, `CREATE TABLE facts_t (
		id     integer PRIMARY KEY,
		code   varchar(3) NOT NULL,
		CONSTRAINT facts_t_code_len CHECK (length((code)::text) > 0)
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS facts_t`) })

	colFacts, err := pgintrospect.ColumnFactsOf(ctx, db, "public")
	if err != nil {
		t.Fatalf("ColumnFactsOf() = %v", err)
	}
	var codeFact pgintrospect.ColumnFacts
	for _, f := range colFacts {
		if f.Table == "facts_t" && f.Column == "code" {
			codeFact = f
		}
	}
	if codeFact.DataType != "character varying" || codeFact.CharMax != 3 || codeFact.Nullable != "NO" {
		t.Errorf("facts_t.code facts = %+v, want character varying, CharMax 3, NOT NULL", codeFact)
	}

	conFacts, err := pgintrospect.ConstraintFactsOf(ctx, db, "public")
	if err != nil {
		t.Fatalf("ConstraintFactsOf() = %v", err)
	}
	sawCheck := false
	for _, f := range conFacts {
		if f.Table == "facts_t" && f.Type == "c" && strings.Contains(f.Expression, "length") {
			sawCheck = true
		}
	}
	if !sawCheck {
		t.Errorf("ConstraintFactsOf() = %+v, want the facts_t CHECK expression", conFacts)
	}
}

// TestNormalizeCheckExpressionRoundTrip is the #192 fidelity proof against a
// LIVE server. A varchar membership CHECK authored as IN(...) deparses with a
// WHOLE-ARRAY cast; a copy created FROM that deparsed text re-parses with the
// cast folded onto each element, so the two renderings are byte-different yet
// the same constraint. NormalizeCheckExpression must collapse both to ONE
// string on the executing server, so string equality of the normalized forms
// means tree equality. This cannot be faked — it depends on the server's own
// parser folding the cast.
func TestNormalizeCheckExpressionRoundTrip(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	execAll(t, db, `CREATE TABLE norm_src (
		side varchar(8) NOT NULL,
		CONSTRAINT norm_src_side_in CHECK (side IN ('buy', 'sell'))
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS norm_src, norm_copy`) })

	// D1: the destination's own deparse of the IN check — a whole-array cast.
	d1 := checkExpr(t, db, "norm_src", "norm_src_side_in")
	if !strings.Contains(d1, "::text[]") {
		t.Fatalf("norm_src deparse %q does not carry the whole-array cast the round-trip needs", d1)
	}

	// A copy created FROM D1 re-parses it: Postgres folds the whole-array cast
	// onto each element, so its deparse D2 differs textually from D1.
	execAll(t, db, fmt.Sprintf(`CREATE TABLE norm_copy (
		side varchar(8) NOT NULL,
		CONSTRAINT norm_copy_side_in CHECK (%s)
	)`, d1))
	d2 := checkExpr(t, db, "norm_copy", "norm_copy_side_in")
	if d1 == d2 {
		t.Fatalf("the fixture no longer isolates the round-trip: D1 and D2 are byte-equal (%q)", d1)
	}
	if strings.Contains(d2, "::text[]") {
		t.Fatalf("D2 %q still carries the whole-array cast; the copy did not fold it", d2)
	}

	// Normalizing each rendering on ONE server collapses them to one string.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	n1, err := pgintrospect.NormalizeCheckExpression(ctx, tx, "public", "norm_src", d1)
	if err != nil {
		t.Fatalf("NormalizeCheckExpression(D1) = %v", err)
	}
	n2, err := pgintrospect.NormalizeCheckExpression(ctx, tx, "public", "norm_copy", d2)
	if err != nil {
		t.Fatalf("NormalizeCheckExpression(D2) = %v", err)
	}
	if n1 != n2 {
		t.Errorf("normalized forms differ: N1 = %q, N2 = %q; #192 requires them equal", n1, n2)
	}
	if n1 == d1 {
		t.Errorf("normalization was a no-op on D1 (%q); the fixture is not exercising #192", d1)
	}

	// The savepoint rolled back: norm_src still carries only its original
	// constraint, no scratch constraint leaked into the transaction.
	checks, err := pgintrospect.CheckConstraints(ctx, tx)
	if err != nil {
		t.Fatalf("CheckConstraints() after normalize = %v", err)
	}
	for _, ck := range checks {
		if strings.Contains(ck.Name, "_pgintrospect_check_norm") {
			t.Errorf("scratch constraint leaked: %+v", ck)
		}
	}
}

// TestNormalizeCheckExpressionInapplicableLive proves the fail-open-parse
// posture against a real server: an expression the relation cannot satisfy (a
// column it lacks) returns VERBATIM with no error, so a caller's comparison
// still fails closed on it, and the transaction stays usable afterwards.
func TestNormalizeCheckExpressionInapplicableLive(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	execAll(t, db, `CREATE TABLE norm_inapplicable (side text NOT NULL)`)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS norm_inapplicable`) })

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	const expr = `(missing_column > 0)`
	got, err := pgintrospect.NormalizeCheckExpression(ctx, tx, "public", "norm_inapplicable", expr)
	if err != nil {
		t.Fatalf("NormalizeCheckExpression(inapplicable) = %v, want nil (fail-open parse)", err)
	}
	if got != expr {
		t.Errorf("NormalizeCheckExpression(inapplicable) = %q, want the verbatim %q", got, expr)
	}
	// The savepoint absorbed the failed ALTER: the transaction is still usable.
	var one int
	if err := tx.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("transaction unusable after inapplicable normalize: %v", err)
	}
}

// TestPinSearchPathRendersUnqualified proves PinSearchPath makes pg_get_expr
// render a schema-local type UNQUALIFIED: without the pin the destination
// deparses a run-scoped enum cast schema-qualified; with the pin it renders
// bare, exactly as the destination renders its own public-schema types.
func TestPinSearchPathRendersUnqualified(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	execAll(t, db,
		`CREATE SCHEMA run_pin`,
		`CREATE TYPE run_pin.color AS ENUM ('r', 'g')`,
		`CREATE TABLE run_pin.t (
			c run_pin.color,
			CONSTRAINT t_color_rule CHECK (c = 'r'::run_pin.color)
		)`,
	)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DROP SCHEMA IF EXISTS run_pin CASCADE`) })

	// Unpinned (default search_path): the enum cast renders schema-qualified.
	unpinned := runPinCheckExpr(t, db, ctx, false)
	if !strings.Contains(unpinned, "run_pin.color") {
		t.Fatalf("unpinned deparse %q, want the schema-qualified run_pin.color", unpinned)
	}

	// Pinned to run_pin: the same cast renders bare.
	pinned := runPinCheckExpr(t, db, ctx, true)
	if strings.Contains(pinned, "run_pin.") {
		t.Errorf("pinned deparse %q still schema-qualifies the type; the pin did not take", pinned)
	}
	if !strings.Contains(pinned, "color") {
		t.Errorf("pinned deparse %q dropped the type name entirely", pinned)
	}
}

// runPinCheckExpr reads run_pin.t's CHECK expression inside a transaction,
// optionally pinning search_path to run_pin first, so the two calls differ only
// in whether PinSearchPath ran.
func runPinCheckExpr(t *testing.T, db *sql.DB, ctx context.Context, pin bool) string {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if pin {
		if err := pgintrospect.PinSearchPath(ctx, tx, "run_pin"); err != nil {
			t.Fatalf("PinSearchPath() = %v", err)
		}
	}
	facts, err := pgintrospect.ConstraintFactsOf(ctx, tx, "run_pin")
	if err != nil {
		t.Fatalf("ConstraintFactsOf() = %v", err)
	}
	for _, f := range facts {
		if f.Table == "t" && f.Type == "c" {
			return f.Expression
		}
	}
	t.Fatalf("ConstraintFactsOf(run_pin) has no CHECK on t: %+v", facts)
	return ""
}

// checkExpr reads one table's named CHECK expression as the server deparses it.
func checkExpr(t *testing.T, db *sql.DB, table, constraint string) string {
	t.Helper()
	checks, err := pgintrospect.CheckConstraints(context.Background(), db)
	if err != nil {
		t.Fatalf("CheckConstraints() = %v", err)
	}
	for _, ck := range checks {
		if ck.Table == table && ck.Name == constraint {
			return ck.Expression
		}
	}
	t.Fatalf("no CHECK %q on table %q: %+v", constraint, table, checks)
	return ""
}

// contains reports whether s holds v.
func contains(s []string, v string) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}
