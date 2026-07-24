//go:build integration

package pgtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	// Registers the pgx/v5 database/sql driver under the name "pgx". The
	// harness hands callers a standard *sql.DB, so they need not import a
	// driver themselves.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// image is the Postgres the harness starts when it spins its own container. It
// matches the tag the orchestrator's destination-contract tests already run
// against, so moving that code into this library later needs no engine change.
const image = "postgres:17-alpine"

// ErrNoDocker reports that neither integration path is available: TEST_PG_CONN
// is unset and no usable Docker daemon was found to start a container. Callers
// test for it with errors.Is and skip the integration tier gracefully — a
// contributor without Docker is never blocked, mirroring how declimport's
// integration tests skip when their external tool is absent.
var ErrNoDocker = errors.New("pgtest: no TEST_PG_CONN and no usable Docker daemon")

// Postgres is a live Postgres shared across an integration test binary. Start
// it once (typically in TestMain) and open a fresh *sql.DB per test with Open;
// each test is expected to create and drop its own tables so they stay
// isolated on the shared server.
type Postgres struct {
	dsn       string
	terminate func(context.Context) error
}

// Start brings up Postgres for the whole test binary and returns a handle to
// it. When TEST_PG_CONN is set it connects to that existing database and starts
// no container — useful for CI service containers or a developer's local
// Postgres. Otherwise it runs a throwaway testcontainers Postgres, which
// requires a reachable Docker daemon.
//
// Start returns ErrNoDocker (check with errors.Is) when the container path is
// needed but Docker is unavailable, so a caller's TestMain can skip the tier
// instead of failing.
func Start(ctx context.Context) (*Postgres, error) {
	if dsn := os.Getenv("TEST_PG_CONN"); dsn != "" {
		if err := verify(ctx, dsn); err != nil {
			return nil, fmt.Errorf("pgtest: TEST_PG_CONN unreachable: %w", err)
		}
		// Connect mode owns no container, so there is nothing to terminate.
		return &Postgres{dsn: dsn, terminate: func(context.Context) error { return nil }}, nil
	}

	if !dockerAvailable(ctx) {
		return nil, ErrNoDocker
	}

	ctr, err := tcpostgres.Run(
		ctx, image,
		tcpostgres.WithDatabase("dcg_test"),
		tcpostgres.WithUsername("dcg"),
		tcpostgres.WithPassword("dcg"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("pgtest: start postgres container: %w", err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("pgtest: connection string: %w", err)
	}

	return &Postgres{
		dsn:       dsn,
		terminate: func(context.Context) error { return testcontainers.TerminateContainer(ctr) },
	}, nil
}

// DSN is the connection string for the running Postgres, for callers that need
// to open connections some other way (a pgxpool, a migration tool). Prefer Open
// for ordinary tests.
func (p *Postgres) DSN() string { return p.dsn }

// Open returns a *sql.DB against the running Postgres, closed automatically on
// t.Cleanup. It fails the test rather than returning an error, matching the
// ergonomics of the library's other test helpers.
func (p *Postgres) Open(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", p.dsn)
	if err != nil {
		t.Fatalf("pgtest: open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Close terminates the container the harness started. It is a no-op in
// TEST_PG_CONN (connect) mode. Call it from the TestMain that called Start,
// typically via defer.
func (p *Postgres) Close(ctx context.Context) error { return p.terminate(ctx) }

// verify opens a throwaway connection to dsn and pings it, so a misconfigured
// TEST_PG_CONN fails Start loudly instead of surfacing later as a confusing
// per-test error.
func verify(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.PingContext(ctx)
}

// dockerAvailable reports whether a Docker daemon can be reached, so Start can
// distinguish "no Docker, skip the tier" from a genuine container failure.
func dockerAvailable(ctx context.Context) bool {
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return false
	}
	defer func() { _ = provider.Close() }()
	return provider.Health(ctx) == nil
}
