package pgintrospect_test

// The gateway's own fast-tier tests drive every method over an in-process
// scripted driver: a happy path per method pins the row folding and grouping,
// and two generic sweeps pin the shared error paths (a failed query, a
// malformed row a healthy Postgres never produces). Conformance against a REAL
// Postgres — that the SQL means what these tests assume, and that
// NormalizeCheckExpression's deparse really is the server's own — lives in the
// integration tier (pgintrospect_integration_test.go), so the SQL's meaning is
// asserted where only a live server can prove it.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/pgintrospect"
)

// script scripts the fake connection: whether every ExecContext fails, and the
// one result set every QueryContext returns (or its error). execErrs scripts
// SUCCESSIVE ExecContext results in call order (nil is a success) for the
// multi-statement NormalizeCheckExpression flow; once exhausted, execErr
// applies to every further call, so the single-statement tests keep their
// one-knob scripting.
type script struct {
	execErr  error
	execErrs []error
	queryErr error
	cols     []string
	rows     [][]driver.Value
	// then scripts the result sets of the queries after the first, in call
	// order, for methods that issue more than one query.
	then  []resultSet
	calls int
}

type resultSet struct {
	cols []string
	rows [][]driver.Value
	err  error
}

type scriptedRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

func (r *scriptedRows) Columns() []string { return r.cols }
func (r *scriptedRows) Close() error      { return nil }
func (r *scriptedRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

type scriptedConn struct{ s *script }

func (c *scriptedConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("pgintrospect test: scripted conn does not prepare")
}
func (c *scriptedConn) Close() error              { return nil }
func (c *scriptedConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }
func (c *scriptedConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if len(c.s.execErrs) > 0 {
		err := c.s.execErrs[0]
		c.s.execErrs = c.s.execErrs[1:]
		if err != nil {
			return nil, err
		}
		return driver.RowsAffected(0), nil
	}
	if c.s.execErr != nil {
		return nil, c.s.execErr
	}
	return driver.RowsAffected(0), nil
}
func (c *scriptedConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.s.queryErr != nil {
		return nil, c.s.queryErr
	}
	rows := &scriptedRows{cols: c.s.cols, rows: c.s.rows}
	if c.s.calls++; c.s.calls > 1 && len(c.s.then) > 0 {
		next := c.s.then[0]
		c.s.then = c.s.then[1:]
		if next.err != nil {
			return nil, next.err
		}
		rows = &scriptedRows{cols: next.cols, rows: next.rows}
	}
	return rows, nil
}

type scriptedDriver struct{}

func (scriptedDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("pgintrospect test: use sql.OpenDB with scriptedConnector")
}

type scriptedConnector struct{ s *script }

func (c scriptedConnector) Connect(context.Context) (driver.Conn, error) {
	return &scriptedConn{s: c.s}, nil
}
func (scriptedConnector) Driver() driver.Driver { return scriptedDriver{} }

// scriptDB opens an in-process *sql.DB over one scripted connection.
func scriptDB(t *testing.T, s *script) *sql.DB {
	t.Helper()
	db := sql.OpenDB(scriptedConnector{s: s})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestQuoteIdentifier(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", `"plain"`},
		{"MiXed", `"MiXed"`},
		{`emb"edded`, `"emb""edded"`},
		{`"; DROP TABLE x; --`, `"""; DROP TABLE x; --"`},
		{"", `""`},
	}
	for _, tc := range cases {
		if got := pgintrospect.QuoteIdentifier(tc.in); got != tc.want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestQuoteLiteral(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", `'plain'`},
		{"it's", `'it''s'`},
		{`'; DROP TABLE x; --`, `'''; DROP TABLE x; --'`},
		{"", `''`},
	}
	for _, tc := range cases {
		if got := pgintrospect.QuoteLiteral(tc.in); got != tc.want {
			t.Errorf("QuoteLiteral(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestBaseTables(t *testing.T) {
	db := scriptDB(t, &script{cols: []string{"table_name"}, rows: [][]driver.Value{{"accounts"}, {"transactions"}}})
	got, err := pgintrospect.BaseTables(t.Context(), db, "public")
	if err != nil {
		t.Fatalf("BaseTables() = %v, want nil", err)
	}
	if want := []string{"accounts", "transactions"}; !reflect.DeepEqual(got, want) {
		t.Errorf("BaseTables() = %v, want %v", got, want)
	}
}

func TestSchemaExists(t *testing.T) {
	for _, want := range []bool{true, false} {
		db := scriptDB(t, &script{cols: []string{"exists"}, rows: [][]driver.Value{{want}}})
		got, err := pgintrospect.SchemaExists(t.Context(), db, "run_x")
		if err != nil {
			t.Fatalf("SchemaExists() = %v, want nil", err)
		}
		if got != want {
			t.Errorf("SchemaExists() = %v, want %v", got, want)
		}
	}
}

// columnsCols is the 17-column shape of the contract generator's column
// introspection result set, for scripting rows.
var columnsCols = []string{
	"table_name", "column_name", "data_type", "is_nullable",
	"is_identity", "is_generated", "has_default", "is_sequence_default",
	"column_default", "udt_name", "udt_schema", "element_data_type",
	"array_dims", "character_maximum_length", "numeric_precision",
	"numeric_scale", "element_type_mod",
}

var triggerCols = []string{"relname", "tgname", "prosrc"}

func TestColumns(t *testing.T) {
	db := scriptDB(t, &script{cols: columnsCols, rows: [][]driver.Value{
		{"t", "id", "bigint", "NO", "YES", "NEVER", true, false, "identity", "int8", "public", "", int64(0), nil, int64(64), int64(0), int64(-1)},
		{"t", "code", "character varying", "YES", "NO", "NEVER", false, false, nil, "varchar", "public", "", int64(0), int64(3), nil, nil, int64(-1)},
		{"accounts", "source_id", "bigint", "NO", "NO", "NEVER", false, false, nil, "int8", "public", "", int64(0), nil, int64(64), int64(0), int64(-1)},
		{"accounts", "display_name", "character varying", "NO", "NO", "NEVER", false, false, nil, "varchar", "public", "", int64(0), int64(100), nil, nil, int64(-1)},
		{"accounts", "name", "text", "NO", "NO", "NEVER", false, false, nil, "text", "public", "", int64(0), nil, nil, nil, int64(-1)},
		{"accounts", "note", "text", "YES", "NO", "NEVER", false, false, nil, "text", "public", "", int64(0), nil, nil, nil, int64(-1)},
		{"accounts", "currency", "text", "NO", "NO", "NEVER", true, false, "'SEK'::text", "text", "public", "", int64(0), nil, nil, nil, int64(-1)},
		{"other", "source_id", "bigint", "NO", "NO", "NEVER", false, false, nil, "int8", "public", "", int64(0), nil, int64(64), int64(0), int64(-1)},
	}, then: []resultSet{{cols: triggerCols, rows: [][]driver.Value{
		{"accounts", "accounts_a_first", "begin new.name = 'x'; return new; end;"},
		{"accounts", "accounts_resolve_source", accountsResolveSourceBody},
	}}}})
	got, err := pgintrospect.Columns(t.Context(), db)
	if err != nil {
		t.Fatalf("Columns() = %v, want nil", err)
	}
	if len(got) != 8 {
		t.Fatalf("Columns() returned %d rows, want 8", len(got))
	}
	id := got[0]
	if id.Table != "t" || id.Name != "id" || id.DataType != "bigint" || id.Nullable != "NO" ||
		id.IsIdentity != "YES" || !id.HasDefault || id.IsSequenceDefault ||
		!id.Default.Valid || id.Default.String != "identity" ||
		id.UDTName != "int8" || id.UDTSchema != "public" ||
		id.CharMaxLength.Valid || !id.NumericPrecision.Valid || id.NumericPrecision.Int64 != 64 ||
		id.ElementTypeMod != -1 {
		t.Errorf("Columns()[0] = %+v, want the scripted identity column's facts", id)
	}
	code := got[1]
	if code.Name != "code" || code.Default.Valid || !code.CharMaxLength.Valid || code.CharMaxLength.Int64 != 3 || code.NumericPrecision.Valid {
		t.Errorf("Columns()[1] = %+v, want the scripted varchar(3) column's facts", code)
	}
	wantTrigger := map[string]string{
		"t.id": "", "t.code": "",
		"accounts.source_id": "accounts_resolve_source", "accounts.display_name": "accounts_resolve_source",
		"accounts.name": "accounts_a_first", "accounts.note": "", "accounts.currency": "",
		"other.source_id": "",
	}
	for _, c := range got {
		if want := wantTrigger[c.Table+"."+c.Name]; c.FillingTrigger != want {
			t.Errorf("Columns() %s.%s FillingTrigger = %q, want %q", c.Table, c.Name, c.FillingTrigger, want)
		}
	}
}

func TestColumnsReportTriggerQueryFailure(t *testing.T) {
	boom := errors.New("trigger query failed")
	db := scriptDB(t, &script{cols: columnsCols, then: []resultSet{{err: boom}}})
	if _, err := pgintrospect.Columns(t.Context(), db); !errors.Is(err, boom) {
		t.Errorf("Columns(trigger query failure) = %v, want %v", err, boom)
	}
}

// accountsResolveSourceBody is the prosrc of wealth-tracker's
// accounts_resolve_source trigger function (migration
// 20260919120000_sources_and_display_names.sql).
const accountsResolveSourceBody = `
begin
  if new.source_id is null or (tg_op = 'UPDATE' and new.source is distinct from old.source) then
    insert into sources (user_id, key, display_name, default_currency)
    values (new.user_id, new.source, new.source, coalesce(new.currency, 'SEK'))
    on conflict (user_id, key) do nothing;
    select id into new.source_id from sources
     where user_id = new.user_id and key = new.source;
  end if;
  if new.display_name is null
     or (tg_op = 'UPDATE' and new.name is distinct from old.name and new.display_name = old.name) then
    new.display_name := new.name;
  end if;
  return new;
end;
`

func TestAssignsNewColumn(t *testing.T) {
	cases := []struct {
		name, source, column string
		want                 bool
	}{
		{"into form in the wealth-tracker trigger", accountsResolveSourceBody, "source_id", true},
		{"walrus form in the wealth-tracker trigger", accountsResolveSourceBody, "display_name", true},
		{"read-only column in the wealth-tracker trigger", accountsResolveSourceBody, "user_id", false},
		{"column only compared with is distinct from", accountsResolveSourceBody, "name", false},
		{"equals form", "new.slug = lower(new.name);", "slug", true},
		{"no space before operator", "new.slug:=1;", "slug", true},
		{"tabs and newlines around operator", "NEW.slug\n\t:=\n\t1;", "slug", true},
		{"if comparison", "IF new.x = 5 THEN return new; END IF;", "x", false},
		{"multi-line if comparison", "IF\n  new.x = 5\nTHEN return new; END IF;", "x", false},
		{"comparison on the right", "select 1 from t where k = new.col;", "col", false},
		{"assignment after then", "if a then new.x := 5; end if;", "x", true},
		{"assignment after begin", "begin new.x = 5; end;", "x", true},
		{"assignment after else", "if a then null; else new.x := 5; end if;", "x", true},
		{"assignment in loop", "loop new.x := 5; exit; end loop;", "x", true},
		{"assignment after a statement", "a := 1;\n  new.x := 5;", "x", true},
		{"line comment", "-- new.x := 5;\nreturn new;", "x", false},
		{"block comment", "/* new.x := 5; */ return new;", "x", false},
		{"unterminated block comment", "return new; /* new.x := 5;", "x", false},
		{"line comment without newline", "return new; -- new.x := 5;", "x", false},
		{"string literal", "raise notice 'new.x := 5; it''s'; return new;", "x", false},
		{"unterminated string literal", "raise notice 'new.x := 5;", "x", false},
		{"assignment after a comment", "/* c */ new.x := 5;", "x", true},
		{"quoted column is case-sensitive", `new."Slug" := 1;`, "slug", false},
		{"upper case source", "NEW.SLUG := 1;", "slug", true},
		{"upper case into", "SELECT 1 INTO NEW.slug;", "slug", true},
		{"quoted column", `new."Slug" := 1;`, "Slug", true},
		{"longer name with suffix", "new.source_id_old := 1;", "source_id", false},
		{"longer name with suffix in into form", "select 1 into new.source_id_old;", "source_id", false},
		{"longer name with prefix", "new.old_source_id := 1;", "source_id", false},
		{"other record named like new", "renew.slug := 1;", "slug", false},
		{"comparison with less-or-equal", "if new.slug <= 3 then", "slug", false},
		{"comparison with not-equal", "if new.slug != 3 then", "slug", false},
		{"read in expression", "x := new.slug;", "slug", false},
		{"empty column", "new. := 1;", "", false},
		{"empty source", "", "slug", false},
		{"column with regexp metacharacters", "new.a.b := 1;", "a.b", true},
		{"regexp metacharacter does not match any character", "new.axb := 1;", "a.b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pgintrospect.AssignsNewColumn(tc.source, tc.column); got != tc.want {
				t.Errorf("AssignsNewColumn(%q, %q) = %v, want %v", tc.source, tc.column, got, tc.want)
			}
		})
	}
}

func TestEnumValues(t *testing.T) {
	db := scriptDB(t, &script{cols: []string{"enumlabel"}, rows: [][]driver.Value{{"open"}, {"closed"}}})
	got, err := pgintrospect.EnumValues(t.Context(), db, "public", "status")
	if err != nil {
		t.Fatalf("EnumValues() = %v, want nil", err)
	}
	if want := []string{"open", "closed"}; !reflect.DeepEqual(got, want) {
		t.Errorf("EnumValues() = %v, want %v (declared order)", got, want)
	}
}

func TestPrimaryKeys(t *testing.T) {
	db := scriptDB(t, &script{cols: []string{"table_name", "column_name"}, rows: [][]driver.Value{
		{"t", "user_id"}, {"t", "source"}, {"u", "id"},
	}})
	got, err := pgintrospect.PrimaryKeys(t.Context(), db)
	if err != nil {
		t.Fatalf("PrimaryKeys() = %v, want nil", err)
	}
	want := map[string][]string{"t": {"user_id", "source"}, "u": {"id"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PrimaryKeys() = %v, want %v (key order preserved)", got, want)
	}
}

func TestUniqueConstraints(t *testing.T) {
	db := scriptDB(t, &script{cols: []string{"table_name", "constraint_name", "column_name"}, rows: [][]driver.Value{
		{"t", "t_key", "account_id"}, {"t", "t_key", "date"}, {"u", "u_isin", "isin"},
	}})
	got, err := pgintrospect.UniqueConstraints(t.Context(), db)
	if err != nil {
		t.Fatalf("UniqueConstraints() = %v, want nil", err)
	}
	want := []pgintrospect.UniqueConstraint{
		{Table: "t", Name: "t_key", Columns: []string{"account_id", "date"}},
		{Table: "u", Name: "u_isin", Columns: []string{"isin"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("UniqueConstraints() = %v, want %v (grouped in encounter order)", got, want)
	}
}

func TestForeignKeys(t *testing.T) {
	db := scriptDB(t, &script{
		cols: []string{"table_name", "conname", "column_name", "referenced_table", "referenced_column", "num_cols"},
		rows: [][]driver.Value{
			{"orders", "orders_pair_fk", "a", "refs", "x", int64(2)},
			{"orders", "orders_pair_fk", "b", "refs", "y", int64(2)},
			{"orders", "orders_ref_fk", "ref_id", "refs", "id", int64(1)},
		},
	})
	got, err := pgintrospect.ForeignKeys(t.Context(), db)
	if err != nil {
		t.Fatalf("ForeignKeys() = %v, want nil", err)
	}
	want := []pgintrospect.ForeignKey{
		{Table: "orders", Name: "orders_pair_fk", NumColumns: 2, Columns: []string{"a", "b"}, ReferencedTable: "refs", ReferencedColumns: []string{"x", "y"}},
		{Table: "orders", Name: "orders_ref_fk", NumColumns: 1, Columns: []string{"ref_id"}, ReferencedTable: "refs", ReferencedColumns: []string{"id"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ForeignKeys() = %v, want %v (pairwise columns, grouped per constraint)", got, want)
	}
}

func TestCheckConstraints(t *testing.T) {
	db := scriptDB(t, &script{
		cols: []string{"table_name", "conname", "expr", "column_name"},
		rows: [][]driver.Value{
			{"t", "t_multi_check", "((a IS NULL) OR (b IS NOT NULL))", "a"},
			{"t", "t_multi_check", "((a IS NULL) OR (b IS NOT NULL))", "b"},
			{"t", "t_no_column_check", nil, nil},
		},
	})
	got, err := pgintrospect.CheckConstraints(t.Context(), db)
	if err != nil {
		t.Fatalf("CheckConstraints() = %v, want nil", err)
	}
	want := []pgintrospect.CheckConstraint{
		{Table: "t", Name: "t_multi_check", Expression: "((a IS NULL) OR (b IS NOT NULL))", Columns: []string{"a", "b"}},
		{Table: "t", Name: "t_no_column_check", Expression: ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CheckConstraints() = %v, want %v (columns gathered, NULL column skipped)", got, want)
	}
}

func TestColumnFactsOf(t *testing.T) {
	db := scriptDB(t, &script{
		cols: []string{"table_name", "column_name", "data_type", "char_max", "num_prec", "num_scale", "is_nullable", "udt_name"},
		rows: [][]driver.Value{{"t", "code", "character varying", int64(3), int64(-1), int64(-1), "NO", "varchar"}},
	})
	got, err := pgintrospect.ColumnFactsOf(t.Context(), db, "public")
	if err != nil {
		t.Fatalf("ColumnFactsOf() = %v, want nil", err)
	}
	want := []pgintrospect.ColumnFacts{{Table: "t", Column: "code", DataType: "character varying", CharMax: 3, NumericPrecision: -1, NumericScale: -1, Nullable: "NO", UDTName: "varchar"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ColumnFactsOf() = %v, want %v", got, want)
	}
}

func TestConstraintFactsOf(t *testing.T) {
	db := scriptDB(t, &script{
		cols: []string{"relname", "contype", "expr", "cols"},
		rows: [][]driver.Value{{"t", "u", "", "account_id, date"}},
	})
	got, err := pgintrospect.ConstraintFactsOf(t.Context(), db, "public")
	if err != nil {
		t.Fatalf("ConstraintFactsOf() = %v, want nil", err)
	}
	want := []pgintrospect.ConstraintFacts{{Table: "t", Type: "u", Expression: "", Columns: "account_id, date"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ConstraintFactsOf() = %v, want %v", got, want)
	}
}

func TestPinSearchPath(t *testing.T) {
	if err := pgintrospect.PinSearchPath(t.Context(), scriptDB(t, &script{}), "run_x"); err != nil {
		t.Errorf("PinSearchPath() = %v, want nil", err)
	}
	wantErr := errors.New("pin refused")
	err := pgintrospect.PinSearchPath(t.Context(), scriptDB(t, &script{execErr: wantErr}), "run_x")
	if !errors.Is(err, wantErr) {
		t.Errorf("PinSearchPath(exec failure) = %v, want the driver's error", err)
	}
}

// TestNormalizeCheckExpression pins the savepoint-scoped normalization flow
// (issue #192) over the scripted driver: the happy path returns the server's
// read-back deparse (not the caller's text), an inapplicable expression falls
// back to its VERBATIM text with no error (the caller's comparison then fails
// closed on it), and every plumbing failure a healthy server never produces (a
// failed savepoint or rollback, a failed, empty, or malformed read-back)
// surfaces as an error. The conformance half — that the returned deparse really
// is the one the server's own catalog renders — lives in the integration tier
// against a real Postgres, since it cannot be faked.
func TestNormalizeCheckExpression(t *testing.T) {
	boom := errors.New("boom")
	const verbatim = `((c)::text = ANY ((ARRAY['a'::character varying])::text[]))`
	const normalized = `((c)::text = ANY (ARRAY[('a'::character varying)::text]))`
	exprCols := []string{"pg_get_expr"}
	oneRow := [][]driver.Value{{normalized}}

	t.Run("returns the server's own deparse", func(t *testing.T) {
		db := scriptDB(t, &script{cols: exprCols, rows: oneRow})
		got, err := pgintrospect.NormalizeCheckExpression(t.Context(), db, "run_x", "t", verbatim)
		if err != nil || got != normalized {
			t.Fatalf("NormalizeCheckExpression() = %q, %v; want the read-back deparse %q, nil", got, err, normalized)
		}
	})

	t.Run("inapplicable expression keeps its verbatim text", func(t *testing.T) {
		// The savepoint succeeds, the ALTER fails (a column the relation
		// lacks), the rollback succeeds: the caller gets the verbatim text back
		// so its own comparison still fails closed on the divergence.
		db := scriptDB(t, &script{execErrs: []error{nil, boom}})
		got, err := pgintrospect.NormalizeCheckExpression(t.Context(), db, "run_x", "t", verbatim)
		if err != nil || got != verbatim {
			t.Fatalf("NormalizeCheckExpression(inapplicable) = %q, %v; want the verbatim text back, nil", got, err)
		}
	})

	plumbing := []struct {
		name   string
		script *script
		want   string
	}{
		{"savepoint fails", &script{execErrs: []error{boom}}, "savepoint for CHECK normalization"},
		{"rollback fails", &script{execErrs: []error{nil, nil, boom}, cols: exprCols, rows: oneRow}, "roll back CHECK normalization savepoint"},
		{"read-back query fails", &script{queryErr: boom}, "read back normalized CHECK"},
		{"read-back finds no constraint", &script{cols: exprCols}, "scratch constraint not found"},
		{"read-back row malformed", &script{cols: exprCols, rows: [][]driver.Value{{nil}}}, "read back normalized CHECK"},
	}
	for _, tc := range plumbing {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pgintrospect.NormalizeCheckExpression(t.Context(), scriptDB(t, tc.script), "run_x", "t", verbatim)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NormalizeCheckExpression(%s) = %v, want an error containing %q", tc.name, err, tc.want)
			}
		})
	}
}

// methods enumerates every row-reading gateway method with the column shape of
// its result set, so the two generic error sweeps below (a failed query, a
// malformed row) cover each one without repeating the scaffolding.
var methods = []struct {
	name string
	cols []string
	call func(ctx context.Context, db *sql.DB) error
}{
	{"BaseTables", []string{"table_name"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.BaseTables(ctx, db, "public")
		return err
	}},
	{"SchemaExists", []string{"exists"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.SchemaExists(ctx, db, "run_x")
		return err
	}},
	{"Columns", columnsCols, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.Columns(ctx, db)
		return err
	}},
	{"EnumValues", []string{"enumlabel"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.EnumValues(ctx, db, "public", "status")
		return err
	}},
	{"PrimaryKeys", []string{"table_name", "column_name"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.PrimaryKeys(ctx, db)
		return err
	}},
	{"UniqueConstraints", []string{"table_name", "constraint_name", "column_name"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.UniqueConstraints(ctx, db)
		return err
	}},
	{"ForeignKeys", []string{"table_name", "conname", "column_name", "referenced_table", "referenced_column", "num_cols"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.ForeignKeys(ctx, db)
		return err
	}},
	{"CheckConstraints", []string{"table_name", "conname", "expr", "column_name"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.CheckConstraints(ctx, db)
		return err
	}},
	{"ColumnFactsOf", []string{"table_name", "column_name", "data_type", "char_max", "num_prec", "num_scale", "is_nullable", "udt_name"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.ColumnFactsOf(ctx, db, "public")
		return err
	}},
	{"ConstraintFactsOf", []string{"relname", "contype", "expr", "cols"}, func(ctx context.Context, db *sql.DB) error {
		_, err := pgintrospect.ConstraintFactsOf(ctx, db, "public")
		return err
	}},
}

// TestMethodsReportQueryFailure: every gateway method surfaces its query's
// failure rather than swallowing it, so a destination that has gone away is a
// loud error at every consumer.
func TestMethodsReportQueryFailure(t *testing.T) {
	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			wantErr := errors.New("connection refused")
			err := m.call(t.Context(), scriptDB(t, &script{queryErr: wantErr}))
			if !errors.Is(err, wantErr) {
				t.Errorf("%s(query failure) = %v, want the driver's error", m.name, err)
			}
		})
	}
}

// TestMethodsReportMalformedRow: every gateway method surfaces a row-scan
// failure rather than swallowing it. The real catalog queries can never
// produce such a row (a NULL in a NOT NULL catalog position), so the fake row
// is the only way to exercise the defensive scan guard.
func TestMethodsReportMalformedRow(t *testing.T) {
	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			// A row of the right width whose every value is NULL: the scan into
			// the method's non-nullable string/bool/int fields fails.
			row := make([]driver.Value, len(m.cols))
			err := m.call(t.Context(), scriptDB(t, &script{cols: m.cols, rows: [][]driver.Value{row}}))
			if err == nil {
				t.Errorf("%s(malformed row) = nil, want a scan error", m.name)
			}
		})
	}
}
