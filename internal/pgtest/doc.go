// Package pgtest is the live-Postgres harness for the library's integration
// tier. It brings up (or connects to) a real Postgres so integration tests for
// destination-contract code can run against the genuine database engine rather
// than a fake.
//
// The harness itself lives in files gated behind the `integration` build tag,
// so it is absent from the default pure-Go build and test path (`make check`,
// `go test ./...`). Nothing here compiles — and none of its heavyweight
// dependencies (testcontainers-go, the pgx driver) are linked — unless a build
// opts in with `-tags=integration`. This file carries no build tag purely so
// the package is non-empty in the default build, keeping `go build ./...`
// happy; the exported API appears only under the integration tag (see
// pgtest.go).
package pgtest
