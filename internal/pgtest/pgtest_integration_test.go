//go:build integration

// This file both exercises the harness against a real Postgres and stands as
// the worked example a destination-contract package will copy when it lands in
// this library: start the shared Postgres once in TestMain, skip the tier when
// Docker is absent, and open a fresh *sql.DB per test.
package pgtest_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/internal/pgtest"
)

// pg is the shared Postgres for this test binary, started in TestMain.
var pg *pgtest.Postgres

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()

	got, err := pgtest.Start(ctx)
	if errors.Is(err, pgtest.ErrNoDocker) {
		// No TEST_PG_CONN and no Docker: the integration tier cannot run
		// here. Report and exit 0 so an opt-in run on a Docker-less machine
		// is a graceful no-op, not a failure.
		fmt.Fprintln(os.Stderr, "pgtest: skipping integration tier:", err)
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: start Postgres:", err)
		return 1
	}
	defer func() { _ = got.Close(ctx) }()
	pg = got

	return m.Run()
}

// TestOpenPing proves the harness yields a usable *sql.DB: it connects and a
// trivial query round-trips against the real engine.
func TestOpenPing(t *testing.T) {
	db := pg.Open(t)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}

	var got int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if got != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", got)
	}
}

// TestCreateDropTable proves a test can own its own schema on the shared
// server: create a table, use it, drop it — the isolation pattern destination
// tests will follow.
func TestCreateDropTable(t *testing.T) {
	db := pg.Open(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE pgtest_probe (id integer PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS pgtest_probe`) })

	if _, err := db.ExecContext(ctx, `INSERT INTO pgtest_probe (id, name) VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var name string
	if err := db.QueryRowContext(ctx, `SELECT name FROM pgtest_probe WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("select: %v", err)
	}
	if name != "a" {
		t.Fatalf("name = %q, want %q", name, "a")
	}
}
