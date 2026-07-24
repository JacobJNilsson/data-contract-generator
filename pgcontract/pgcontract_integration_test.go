//go:build integration

// Fidelity fixtures for the faithful generator, run against a REAL Postgres via
// the internal/pgtest harness. They reproduce the exact cases the move must not
// regress — exact integer/varchar/char widths (#124/#82), a multi-column CHECK
// (#155), enums (schema-qualified), arrays (DC-4 fail-closed), and single-column
// foreign keys — and assert the generated odcs.Contract through the B1
// accessors. They also PIN THE SCOPE LINE: a NOT NULL identity/defaulted column
// is encoded NOT NULL here (the faithful shape), never folded to optional, which
// is the policy a caller layers on afterwards.
package pgcontract_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/internal/pgtest"
	"github.com/JacobJNilsson/data-contract-generator/odcs"
	"github.com/JacobJNilsson/data-contract-generator/odcsdest"
	"github.com/JacobJNilsson/data-contract-generator/pgcontract"
)

var pg *pgtest.Postgres

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	got, err := pgtest.Start(ctx)
	if errors.Is(err, pgtest.ErrNoDocker) {
		fmt.Fprintln(os.Stderr, "pgcontract: skipping integration tier:", err)
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgcontract: start Postgres:", err)
		return 1
	}
	defer func() { _ = got.Close(ctx) }()
	pg = got
	return m.Run()
}

func exec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func generate(t *testing.T, db *sql.DB, opts pgcontract.Options) odcs.Contract {
	t.Helper()
	c, err := pgcontract.Generate(context.Background(), db, opts)
	if err != nil {
		t.Fatalf("Generate() = %v, want nil", err)
	}
	return c
}

func tableByName(t *testing.T, c odcs.Contract, name string) odcs.SchemaObject {
	t.Helper()
	for _, tbl := range c.Schema {
		if tbl.Name == name {
			return tbl
		}
	}
	t.Fatalf("contract has no table %q (tables: %v)", name, odcsdest.SortedTableNames(c))
	return odcs.SchemaObject{}
}

func columnByName(t *testing.T, tbl odcs.SchemaObject, name string) odcs.Property {
	t.Helper()
	for _, col := range tbl.Properties {
		if col.Name == name {
			return col
		}
	}
	t.Fatalf("table %q has no column %q", tbl.Name, name)
	return odcs.Property{}
}

// TestGenerateCapturesShapeAndConstraints: a known schema with every constraint
// shape and every scalar type round-trips into a faithful contract that
// captures its columns (exact DDL types), widths, primary key, unique
// constraint, and verbatim check.
func TestGenerateCapturesShapeAndConstraints(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE gen_orders (
		order_id      bigint PRIMARY KEY,
		sku           varchar(64) NOT NULL,
		currency      char(3),
		unbounded     varchar,
		descr         text,
		amount        numeric NOT NULL,
		quantity      integer NOT NULL,
		shipped       boolean,
		placed_at     timestamptz NOT NULL,
		external_id   uuid,
		ordered_on    date,
		metadata      jsonb,
		captured_at   timestamp,
		rating        double precision,
		discount      real,
		CONSTRAINT gen_orders_sku_unique UNIQUE (sku),
		CONSTRAINT gen_orders_amount_nonneg CHECK (amount >= 0)
	)`)
	exec(t, db, `CREATE TABLE gen_customers (customer_id integer PRIMARY KEY, name text NOT NULL)`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS gen_orders, gen_customers`) })

	c := generate(t, db, pgcontract.Options{})
	orders := tableByName(t, c, "gen_orders")

	wantCols := []struct {
		name     string
		ddl      string
		nullable bool
	}{
		{"order_id", "bigint", false},
		{"sku", "varchar(64)", false},
		{"currency", "char(3)", true},
		{"unbounded", "varchar", true},
		{"descr", "text", true},
		{"amount", "numeric", false},
		{"quantity", "integer", false},
		{"shipped", "boolean", true},
		{"placed_at", "timestamptz", false},
		{"external_id", "uuid", true},
		{"ordered_on", "date", true},
		{"metadata", "jsonb", true},
		{"captured_at", "timestamp", true},
		{"rating", "double precision", true},
		{"discount", "real", true},
	}
	if len(orders.Properties) != len(wantCols) {
		t.Fatalf("gen_orders has %d columns, want %d", len(orders.Properties), len(wantCols))
	}
	for i, want := range wantCols {
		col := orders.Properties[i]
		if col.Name != want.name {
			t.Errorf("gen_orders column %d = %q, want %q", i, col.Name, want.name)
			continue
		}
		if ddl, ok := odcsdest.PostgresDDLType(col); !ok || ddl != want.ddl {
			t.Errorf("gen_orders %q DDL = (%q,%v), want %q", col.Name, ddl, ok, want.ddl)
		}
		if odcsdest.Nullable(col) != want.nullable {
			t.Errorf("gen_orders %q nullable = %v, want %v", col.Name, odcsdest.Nullable(col), want.nullable)
		}
	}

	wantWidths := map[string]int{"sku": 64, "currency": 3}
	for _, col := range orders.Properties {
		width, ok := odcsdest.MaxCharacterLength(col)
		want, bounded := wantWidths[col.Name]
		if ok != bounded || (bounded && width != want) {
			t.Errorf("gen_orders %q width = (%d,%v), want (%d,%v)", col.Name, width, ok, want, bounded)
		}
	}

	if pk := odcsdest.PrimaryKeyColumns(orders); len(pk) != 1 || pk[0] != "order_id" {
		t.Errorf("gen_orders PK = %v, want [order_id]", pk)
	}
	uniques := odcs.UniqueConstraints(orders.CustomProperties)
	if len(uniques) != 1 || uniques[0].Name != "gen_orders_sku_unique" || len(uniques[0].Columns) != 1 || uniques[0].Columns[0] != "sku" {
		t.Errorf("gen_orders unique = %+v", uniques)
	}
	checks := odcs.CheckConstraints(orders.CustomProperties)
	if len(checks) != 1 || checks[0].Name != "gen_orders_amount_nonneg" || checks[0].Expression == "" {
		t.Errorf("gen_orders checks = %+v", checks)
	}
	if pk := odcsdest.PrimaryKeyColumns(tableByName(t, c, "gen_customers")); len(pk) != 1 || pk[0] != "customer_id" {
		t.Errorf("gen_customers PK = %v, want [customer_id]", pk)
	}
}

// TestGenerateFaithfulNullabilityNotFolded PINS THE SCOPE LINE (MP-3 policy is
// NOT applied here): a NOT NULL identity primary key and a NOT NULL DEFAULT
// column are encoded NOT NULL in the FAITHFUL contract, exactly as the
// destination declares them. The orchestrator folds these to producer-optional
// as a post-pass in a later stage; the library must not.
func TestGenerateFaithfulNullabilityNotFolded(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE faithful_null (
		id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		created_at timestamptz NOT NULL DEFAULT now(),
		plain      text NOT NULL,
		note       text
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS faithful_null`) })

	tbl := tableByName(t, generate(t, db, pgcontract.Options{}), "faithful_null")
	wantNullable := map[string]bool{
		"id":         false, // identity NOT NULL — faithful contract keeps NOT NULL
		"created_at": false, // NOT NULL DEFAULT now() — kept NOT NULL, not folded
		"plain":      false, // plain NOT NULL
		"note":       true,  // genuinely nullable
	}
	for name, want := range wantNullable {
		if got := odcsdest.Nullable(columnByName(t, tbl, name)); got != want {
			t.Errorf("%q nullable = %v, want %v (faithful raw nullability, MP-3 folding is a later policy pass)", name, got, want)
		}
	}
	// No policy custom-properties leaked in: the faithful contract carries no
	// merge-key, generated-column, or destination-not-null markers.
	for _, key := range []string{"dcgMergeKey", "dcgGeneratedColumns", "dcgDestinationNotNull"} {
		if _, ok := odcs.CustomProp(tbl.CustomProperties, key); ok {
			t.Errorf("faithful contract leaked the policy marker %q", key)
		}
	}
}

// TestGenerateCapturesSelfContainedTypesExactly (#124/#82): each bounded
// string's declared length and each integer's exact width survive, so a
// consumer reproduces the type that REJECTS values rather than a widened one.
func TestGenerateCapturesSelfContainedTypesExactly(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE mf1cap_types (
		code    varchar(3) NOT NULL,
		free    varchar,
		padded  char(2) NOT NULL,
		tiny    smallint NOT NULL,
		normal  integer NOT NULL,
		big     bigint NOT NULL,
		codes   varchar[]
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS mf1cap_types`) })

	tbl := tableByName(t, generate(t, db, pgcontract.Options{}), "mf1cap_types")
	wantDDL := map[string]string{
		"code": "varchar(3)", "free": "varchar", "padded": "char(2)",
		"tiny": "smallint", "normal": "integer", "big": "bigint", "codes": "varchar[]",
	}
	for name, want := range wantDDL {
		col := columnByName(t, tbl, name)
		if ddl, ok := odcsdest.PostgresDDLType(col); !ok || ddl != want {
			t.Errorf("%q DDL = (%q,%v), want %q (exact, never widened)", name, ddl, ok, want)
		}
	}
}

// TestGenerateFailsClosedOnUnreproducibleStringShapes (#82): the string shapes
// the vocabulary cannot reproduce fail CLOSED, naming the column.
func TestGenerateFailsClosedOnUnreproducibleStringShapes(t *testing.T) {
	cases := []struct{ name, ddl, want string }{
		{"bounded varchar array element", `CREATE TABLE mf1cap_bad (codes varchar(3)[])`, `column "codes" has unsupported array element type character varying(3)`},
		{"char array element", `CREATE TABLE mf1cap_bad (pads char[])`, `column "pads" has unsupported array element type character(1)`},
		{"bare bpchar scalar", `CREATE TABLE mf1cap_bad (pad bpchar)`, `column "pad" has unsupported bare bpchar type`},
		{"bare bpchar array element", `CREATE TABLE mf1cap_bad (pads bpchar[])`, `column "pads" has unsupported bare bpchar array element type`},
	}
	db := pg.Open(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec(t, db, tc.ddl)
			t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS mf1cap_bad`) })
			_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Generate() = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestGenerateCapturesMultiColumnCheck (#155): a table-level multi-column CHECK
// (a conditional NOT NULL) is captured, and the synthetic per-column NOT NULL
// checks never appear.
func TestGenerateCapturesMultiColumnCheck(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE checked_rows (
		id                  integer PRIMARY KEY,
		asset_id            integer,
		instrument_currency text,
		amount              numeric NOT NULL,
		CONSTRAINT checked_amount_nonneg CHECK (amount >= 0),
		CONSTRAINT checked_currency_present CHECK ((asset_id IS NULL) OR (instrument_currency IS NOT NULL))
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS checked_rows`) })

	tbl := tableByName(t, generate(t, db, pgcontract.Options{}), "checked_rows")
	byName := map[string]string{}
	for _, ck := range odcs.CheckConstraints(tbl.CustomProperties) {
		byName[ck.Name] = ck.Expression
	}
	if len(byName) != 2 {
		t.Fatalf("checks = %+v, want exactly two (no synthetic NOT NULL)", byName)
	}
	multi, ok := byName["checked_currency_present"]
	if !ok {
		t.Fatalf("multi-column check dropped: %+v", byName)
	}
	for _, want := range []string{"asset_id", "instrument_currency", "IS NOT NULL"} {
		if !strings.Contains(multi, want) {
			t.Errorf("multi-column check %q does not mention %q", multi, want)
		}
	}
}

// TestGenerateCapturesEnum: a USER-DEFINED enum column is captured with its type
// name and labels in declared order.
func TestGenerateCapturesEnum(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TYPE order_status AS ENUM ('pending', 'shipped', 'delivered')`)
	exec(t, db, `CREATE TABLE enum_orders (id integer PRIMARY KEY, status order_status NOT NULL)`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS enum_orders`)
		_, _ = db.ExecContext(context.Background(), `DROP TYPE IF EXISTS order_status`)
	})

	status := columnByName(t, tableByName(t, generate(t, db, pgcontract.Options{}), "enum_orders"), "status")
	if !odcsdest.IsEnum(status) || odcsdest.EnumName(status) != "order_status" {
		t.Fatalf("status is not the expected enum: %+v", status)
	}
	if got := strings.Join(odcsdest.EnumLabels(status), ","); got != "pending,shipped,delivered" {
		t.Errorf("enum labels = %q, want the declared order", got)
	}
	if odcsdest.Nullable(status) {
		t.Errorf("status should be NOT NULL")
	}
}

// TestGenerateResolvesEnumByItsOwnSchema: the enum-label lookup is
// schema-qualified, so a same-named enum in another schema never interleaves
// its labels.
func TestGenerateResolvesEnumByItsOwnSchema(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE SCHEMA decoy`)
	exec(t, db, `CREATE TYPE decoy.mood AS ENUM ('aaa_decoy1', 'aaa_decoy2', 'zzz_decoy3')`)
	exec(t, db, `CREATE TYPE public.mood AS ENUM ('happy', 'sad')`)
	exec(t, db, `CREATE TABLE mood_rows (id integer PRIMARY KEY, feel public.mood NOT NULL)`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS mood_rows`)
		_, _ = db.ExecContext(context.Background(), `DROP TYPE IF EXISTS public.mood`)
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS decoy CASCADE`)
	})

	feel := columnByName(t, tableByName(t, generate(t, db, pgcontract.Options{}), "mood_rows"), "feel")
	if got := strings.Join(odcsdest.EnumLabels(feel), ","); got != "happy,sad" {
		t.Errorf("feel labels = %q, want ONLY the public enum [happy sad]", got)
	}
}

// TestGenerateFailsClosedOnNonEnumUserDefined and its shadowed sibling: a
// USER-DEFINED type that is not an enum fails closed, even when a same-named
// enum exists in another schema.
func TestGenerateFailsClosedOnNonEnumUserDefined(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TYPE addr AS (street text, city text)`)
	exec(t, db, `CREATE TABLE has_composite (id integer PRIMARY KEY, where_at addr)`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS has_composite`)
		_, _ = db.ExecContext(context.Background(), `DROP TYPE IF EXISTS addr`)
	})
	_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{})
	if err == nil {
		t.Fatal("Generate() = nil, want fail-closed for a non-enum USER-DEFINED type")
	}
	for _, want := range []string{"has_composite", "where_at", "addr"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestGenerateFailsClosedOnCompositeShadowedBySameNamedEnum(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE SCHEMA decoy`)
	exec(t, db, `CREATE TYPE decoy.mood AS ENUM ('aaa_decoy1', 'zzz_decoy2')`)
	exec(t, db, `CREATE TYPE public.mood AS (a text, b text)`)
	exec(t, db, `CREATE TABLE shadowed (id integer PRIMARY KEY, feel public.mood)`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS shadowed`)
		_, _ = db.ExecContext(context.Background(), `DROP TYPE IF EXISTS public.mood`)
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS decoy CASCADE`)
	})
	_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{})
	if err == nil {
		t.Fatal("Generate() = nil, want fail-closed for a composite shadowed by a same-named enum")
	}
	for _, want := range []string{"shadowed", "feel", "mood"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestGenerateCapturesNumericPrecisionScale: a numeric(12,2) money column keeps
// its precision and scale; an unconstrained numeric renders bare.
func TestGenerateCapturesNumericPrecisionScale(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE numeric_ledger (
		id integer PRIMARY KEY, amount numeric(12,2) NOT NULL, whole numeric(8), freeform numeric
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS numeric_ledger`) })

	ledger := tableByName(t, generate(t, db, pgcontract.Options{}), "numeric_ledger")
	for _, tc := range []struct{ col, ddl string }{
		{"amount", "numeric(12,2)"}, {"whole", "numeric(8,0)"}, {"freeform", "numeric"},
	} {
		if ddl, ok := odcsdest.PostgresDDLType(columnByName(t, ledger, tc.col)); !ok || ddl != tc.ddl {
			t.Errorf("%s DDL = (%q,%v), want %q", tc.col, ddl, ok, tc.ddl)
		}
	}
}

// TestGenerateCapturesArray and its fail-closed sibling (DC-4): a
// one-dimensional array of a supported scalar is captured; the array shapes the
// contract cannot faithfully reproduce fail closed.
func TestGenerateCapturesArray(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE array_orders (id integer PRIMARY KEY, tags text[] NOT NULL, costs numeric[])`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS array_orders`) })

	orders := tableByName(t, generate(t, db, pgcontract.Options{}), "array_orders")
	if ddl, ok := odcsdest.PostgresDDLType(columnByName(t, orders, "tags")); !ok || ddl != "text[]" {
		t.Errorf("tags DDL = (%q,%v), want text[]", ddl, ok)
	}
	if ddl, ok := odcsdest.PostgresDDLType(columnByName(t, orders, "costs")); !ok || ddl != "numeric[]" {
		t.Errorf("costs DDL = (%q,%v), want numeric[]", ddl, ok)
	}
}

func TestGenerateFailsClosedOnUnfaithfulArrays(t *testing.T) {
	cases := []struct{ name, ddl, wantCol, wantMsg string }{
		{"multi-dimensional", `CREATE TABLE bad_arr (id integer PRIMARY KEY, grid int[][] NOT NULL)`, "grid", "multi-dimensional"},
		{"array of enum", `CREATE TYPE palette AS ENUM ('r','g','b'); CREATE TABLE bad_arr (id integer PRIMARY KEY, colors palette[] NOT NULL)`, "colors", "array element type"},
		{"array of unsupported scalar", `CREATE TABLE bad_arr (id integer PRIMARY KEY, blobs bytea[] NOT NULL)`, "blobs", "array element type"},
		{"array of numeric with precision", `CREATE TABLE bad_arr (id integer PRIMARY KEY, amounts numeric(12,2)[] NOT NULL)`, "amounts", "numeric(12,2)"},
	}
	db := pg.Open(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec(t, db, tc.ddl)
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS bad_arr`)
				_, _ = db.ExecContext(context.Background(), `DROP TYPE IF EXISTS palette`)
			})
			_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{})
			if err == nil {
				t.Fatalf("Generate() = nil, want fail-closed for %s", tc.name)
			}
			for _, want := range []string{tc.wantCol, tc.wantMsg} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// TestGenerateFailsClosedOnUnsupportedTypes: a type outside the vocabulary
// (bytea) and a deliberately-excluded common type (json) both fail closed.
func TestGenerateFailsClosedOnUnsupportedTypes(t *testing.T) {
	cases := []struct {
		name, ddl, drop string
		want            []string
	}{
		{"bytea", `CREATE TABLE unsupported (id integer PRIMARY KEY, payload bytea NOT NULL)`, `DROP TABLE IF EXISTS unsupported`, []string{"unsupported", "payload", "bytea"}},
		{"json", `CREATE TABLE has_json (id integer PRIMARY KEY, doc json NOT NULL)`, `DROP TABLE IF EXISTS has_json`, []string{"has_json", "doc", "json"}},
	}
	db := pg.Open(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec(t, db, tc.ddl)
			t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), tc.drop) })
			_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{})
			if err == nil {
				t.Fatalf("Generate() = nil, want fail-closed for %s", tc.name)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// TestGenerateForeignKeysStructuralTripleOnly: a single-column FK is captured as
// the structural triple (local column, referenced table, referenced column) and
// carries NO policy extras (no natural-key match columns — that resolution is a
// later policy pass). A composite FK is skipped; a control table is FK-free.
func TestGenerateForeignKeysStructuralTripleOnly(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE assets (id bigserial PRIMARY KEY, isin text UNIQUE NOT NULL, name text NOT NULL)`)
	exec(t, db, `CREATE TABLE venues (id bigserial PRIMARY KEY, note text)`)
	exec(t, db, `CREATE TABLE transactions (
		id bigserial PRIMARY KEY,
		asset_id bigint NOT NULL REFERENCES assets(id),
		venue_id bigint REFERENCES venues(id),
		amount numeric NOT NULL
	)`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS transactions, assets, venues`)
	})

	c := generate(t, db, pgcontract.Options{})
	tx := tableByName(t, c, "transactions")
	fks := odcs.ForeignKeys(tx.CustomProperties)
	byColumn := map[string]odcs.ForeignKey{}
	for _, fk := range fks {
		byColumn[fk.Column] = fk
	}
	if len(fks) != 2 {
		t.Fatalf("transactions FKs = %+v, want two", fks)
	}
	if a := byColumn["asset_id"]; a.ReferencedTable != "assets" || a.ReferencedColumn != "id" {
		t.Errorf("asset_id FK = %+v, want -> assets(id)", a)
	}
	if v := byColumn["venue_id"]; v.ReferencedTable != "venues" || v.ReferencedColumn != "id" {
		t.Errorf("venue_id FK = %+v, want -> venues(id)", v)
	}
	// The faithful FK carries the structural triple ONLY: odcs.ForeignKey has no
	// natural-key/attested/constant fields at all, so a policy extra is
	// unrepresentable here by construction (a later policy pass re-adds them).
	if got := odcs.ForeignKeys(tableByName(t, c, "assets").CustomProperties); got != nil {
		t.Errorf("assets FKs = %+v, want nil", got)
	}
}

func TestGenerateSkipsMultiColumnForeignKeys(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE TABLE parts (maker text NOT NULL, code text NOT NULL, isin text UNIQUE NOT NULL, PRIMARY KEY (maker, code))`)
	exec(t, db, `CREATE TABLE assemblies (
		id bigserial PRIMARY KEY, part_maker text NOT NULL, part_code text NOT NULL,
		FOREIGN KEY (part_maker, part_code) REFERENCES parts(maker, code)
	)`)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS assemblies, parts`) })

	asm := tableByName(t, generate(t, db, pgcontract.Options{}), "assemblies")
	if got := odcs.ForeignKeys(asm.CustomProperties); len(got) != 0 {
		t.Errorf("assemblies FKs = %+v, want none (composite skipped)", got)
	}
}

// TestGenerateExclusions covers the Options exclusion policy end to end: an
// excluded table, and an excluded column that drops the constraints spanning it
// (unique, primary key, check, foreign key) whole.
func TestGenerateExclusions(t *testing.T) {
	t.Run("excluded table (a value, not a hardcode)", func(t *testing.T) {
		db := pg.Open(t)
		exec(t, db, `CREATE TABLE keep_me (id integer PRIMARY KEY)`)
		exec(t, db, `CREATE TABLE pipeline_cache (id uuid PRIMARY KEY, blob jsonb)`)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS keep_me, pipeline_cache`) })
		c := generate(t, db, pgcontract.Options{ExcludeTables: []string{"pipeline_cache"}})
		for _, tbl := range c.Schema {
			if tbl.Name == "pipeline_cache" {
				t.Errorf("excluded table leaked")
			}
		}
		_ = tableByName(t, c, "keep_me")
	})

	t.Run("excluded column drops its constraints whole", func(t *testing.T) {
		db := pg.Open(t)
		exec(t, db, `CREATE TABLE assets (id bigserial PRIMARY KEY, isin text UNIQUE NOT NULL)`)
		exec(t, db, `CREATE TABLE keyed (
			id      integer PRIMARY KEY,
			ext_tag text NOT NULL,
			ref_id  bigint REFERENCES assets(id),
			CONSTRAINT keyed_ext_unique UNIQUE (ext_tag),
			CONSTRAINT keyed_ext_len CHECK (length(ext_tag) > 0),
			CONSTRAINT keyed_id_pos CHECK (id > 0)
		)`)
		exec(t, db, `CREATE TABLE pk_keyed (a integer NOT NULL, b integer NOT NULL, PRIMARY KEY (a, b))`)
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS keyed, pk_keyed, assets`) })

		c := generate(t, db, pgcontract.Options{ExcludeColumns: []string{"ext_tag", "b", "ref_id"}})
		keyed := tableByName(t, c, "keyed")
		if u := odcs.UniqueConstraints(keyed.CustomProperties); len(u) != 0 {
			t.Errorf("unique over excluded column kept: %+v", u)
		}
		checks := odcs.CheckConstraints(keyed.CustomProperties)
		if len(checks) != 1 || checks[0].Name != "keyed_id_pos" {
			t.Errorf("checks = %+v, want only keyed_id_pos (excluded-column check dropped)", checks)
		}
		if fk := odcs.ForeignKeys(keyed.CustomProperties); len(fk) != 0 {
			t.Errorf("FK over excluded column kept: %+v", fk)
		}
		if pk := odcsdest.PrimaryKeyColumns(tableByName(t, c, "pk_keyed")); len(pk) != 0 {
			t.Errorf("primary key over excluded column kept: %v", pk)
		}
	})
}

// TestGenerateIntrospectsPublicSchemaOnly: a non-public schema and its tables
// (deliberately using a type outside the vocabulary) never enter the contract,
// so a nil error is itself proof those columns were never read.
func TestGenerateIntrospectsPublicSchemaOnly(t *testing.T) {
	db := pg.Open(t)
	exec(t, db, `CREATE SCHEMA auth`)
	exec(t, db, `CREATE TABLE auth.users (id uuid PRIMARY KEY, secret bytea NOT NULL)`)
	exec(t, db, `CREATE TABLE app_users (id integer PRIMARY KEY, email text NOT NULL)`)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS app_users`)
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS auth CASCADE`)
	})

	c := generate(t, db, pgcontract.Options{})
	for _, tbl := range c.Schema {
		if tbl.Name == "users" {
			t.Errorf("leaked non-public auth.users")
		}
	}
	if got := odcsdest.SortedTableNames(c); len(got) != 1 || got[0] != "app_users" {
		t.Fatalf("tables = %v, want only [app_users]", got)
	}
}

// TestGenerateNilQuerier: a nil handle is rejected, never a panic.
func TestGenerateNilQuerier(t *testing.T) {
	_, err := pgcontract.Generate(context.Background(), nil, pgcontract.Options{})
	if err == nil || !strings.Contains(err.Error(), "non-nil") {
		t.Fatalf("Generate(nil) = %v, want a non-nil-handle error", err)
	}
}

// TestGenerateQueryError: a closed database surfaces a wrapped query error, not
// an empty contract.
func TestGenerateQueryError(t *testing.T) {
	db := pg.Open(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{})
	if err == nil || !strings.Contains(err.Error(), "pgcontract") {
		t.Fatalf("Generate(closed db) = %v, want a wrapped query error", err)
	}
}

// TestGenerateEmptyIsInvalid: a database with no included tables yields a
// fail-closed invalid-contract error, never an empty contract.
func TestGenerateEmptyIsInvalid(t *testing.T) {
	db := pg.Open(t)
	_, err := pgcontract.Generate(context.Background(), db, pgcontract.Options{ExcludeTables: allPublicTables(t, db)})
	if err == nil || !strings.Contains(err.Error(), "at least one table") {
		t.Fatalf("Generate(no included tables) = %v, want an invalid-contract error", err)
	}
}

// allPublicTables lists every public BASE TABLE so a test can exclude them all.
func allPublicTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`)
	if err != nil {
		t.Fatalf("list public tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return names
}
