// Package declimport imports sources that already carry a declared schema
// and normalises them into the odcs model. Where the file analysers infer a
// contract from data, a declared schema (a Postgres DDL dump or an Avro
// record schema) is authoritative, so dcg does not infer it: it shells out
// to the Data Contract CLI and reads the ODCS the CLI emits back into the
// odcs package (spec section 6, DG-4).
//
// The SQL DDL path is the scoped slice. A live Postgres source has no direct
// importer, so the producer takes a schema-only dump first:
//
//	pg_dump --schema-only db > ddl.sql
//
// and declimport runs the two CLI steps over that file:
//
//	datacontract import sql --source ddl.sql --dialect postgres --output dc.yaml
//	datacontract export odcs dc.yaml
//
// The export step's stdout is ODCS v3.1.0, which odcs.UnmarshalYAML reads
// directly. The two steps are kept even though import already emits an
// ODCS-shaped document, so the canonical ODCS the rest of the platform reads
// always comes from the tool's own export path rather than from declimport
// trusting the intermediate.
//
// The only os/exec contact is the Runner seam: a single small interface the
// importer calls for every subprocess. Tests pass a fake Runner, so the
// argv construction, dialect handling, error paths, and ODCS parsing are all
// exercised without the Python toolchain. The real implementation lives in
// exec.go behind the same interface; a build-tagged integration test drives
// it against the genuine binary.
//
// Avro reuses the same importToODCS seam with a different import subcommand
// and no dialect, so SQL and Avro differ only in the import argv. OpenAPI was
// scoped alongside them, but the pinned Data Contract CLI (1.0.6) ships no
// OpenAPI importer, and its jsonschema importer does not read an OpenAPI
// document's components.schemas (it yields an empty model), so there is no
// faithful OpenAPI path to shell out to yet; the seam is ready for one the
// day the CLI grows the importer.
package declimport

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
)

// DefaultBinary is the Data Contract CLI executable declimport shells out to.
// It is resolved on the process PATH; pin the version via tools/import (the
// importer's argv contract targets datacontract-cli 1.0.6).
const DefaultBinary = "datacontract"

// Runner runs a single subprocess and returns its combined output. It is the
// one seam between declimport and the operating system: every CLI invocation
// goes through Run, so a fake Runner makes the whole importer testable
// without the real binary. The combined stdout+stderr is returned so a
// caller can surface the CLI's own diagnostics on failure; on a non-zero
// exit Run returns a non-nil error and the output captured so far.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// Importer imports declared schemas into odcs contracts. The zero value is
// not ready to use; construct one with New. Binary names the CLI executable;
// Runner is the subprocess seam.
type Importer struct {
	Binary string
	Runner Runner
}

// New returns an Importer bound to the given Runner, using DefaultBinary. A
// nil Runner is a programming error and panics, because an Importer with no
// way to run the CLI can do nothing useful and silently returning errors
// would only hide the mistake.
func New(r Runner) *Importer {
	if r == nil {
		panic("declimport: nil Runner")
	}
	return &Importer{Binary: DefaultBinary, Runner: r}
}

// FromSQLDDL imports a SQL DDL file (for example a pg_dump --schema-only
// output) into an odcs contract, using the default CLI binary on PATH. It is
// the package-level convenience over Importer.FromSQLDDL with a real Runner.
func FromSQLDDL(ctx context.Context, ddlPath, dialect string) (odcs.Contract, error) {
	return New(execRunner{}).FromSQLDDL(ctx, ddlPath, dialect)
}

// FromAvro imports an Avro schema file (a .avsc record schema) into an odcs
// contract, using the default CLI binary on PATH. It is the package-level
// convenience over Importer.FromAvro with a real Runner.
func FromAvro(ctx context.Context, schemaPath string) (odcs.Contract, error) {
	return New(execRunner{}).FromAvro(ctx, schemaPath)
}

// FromSQLDDL imports a SQL DDL file into an odcs contract. dialect selects
// the SQL grammar the CLI parses (for example "postgres"); it is required
// because the CLI cannot reliably guess it and a wrong dialect parses the
// DDL incorrectly. ddlPath must name a readable file. The contract is read
// from the CLI's own ODCS export, so it is canonical ODCS v3.1.0.
//
// The function fails closed: a missing file, a non-zero exit from either CLI
// step, or output that does not parse as an ODCS contract all return an
// error and a zero Contract rather than a partial or silently empty result.
func (im *Importer) FromSQLDDL(ctx context.Context, ddlPath, dialect string) (odcs.Contract, error) {
	if strings.TrimSpace(ddlPath) == "" {
		return odcs.Contract{}, fmt.Errorf("declimport: empty DDL path")
	}
	if strings.TrimSpace(dialect) == "" {
		return odcs.Contract{}, fmt.Errorf("declimport: empty SQL dialect")
	}
	if _, err := os.Stat(ddlPath); err != nil {
		return odcs.Contract{}, fmt.Errorf("declimport: DDL file %q: %w", ddlPath, err)
	}
	return im.importToODCS(ctx, []string{"import", "sql", "--source", ddlPath, "--dialect", dialect})
}

// FromAvro imports an Avro schema file into an odcs contract. schemaPath must
// name a readable Avro schema (an .avsc record definition). Unlike SQL there
// is no dialect: Avro is a single declared format, so the import step needs
// only --source. The contract is read from the CLI's own ODCS export, so it
// is canonical ODCS v3.1.0.
//
// The function fails closed: an empty or missing path, a non-zero exit from
// either CLI step, or output that does not parse as an ODCS contract all
// return an error and a zero Contract rather than a partial or silently empty
// result.
//
// The Avro importer is an optional extra of the Data Contract CLI; install it
// with the pinned datacontract-cli[avro] (see tools/import/requirements.txt).
// Without the extra the import step exits non-zero and that error is wrapped
// here, so a missing extra surfaces as a clear failure rather than a hang.
func (im *Importer) FromAvro(ctx context.Context, schemaPath string) (odcs.Contract, error) {
	if strings.TrimSpace(schemaPath) == "" {
		return odcs.Contract{}, fmt.Errorf("declimport: empty Avro schema path")
	}
	if _, err := os.Stat(schemaPath); err != nil {
		return odcs.Contract{}, fmt.Errorf("declimport: Avro schema file %q: %w", schemaPath, err)
	}
	return im.importToODCS(ctx, []string{"import", "avro", "--source", schemaPath})
}

// importToODCS runs an import subcommand to an intermediate data contract,
// then exports that contract to ODCS and parses it. importArgs is the full
// argv after the binary for the import step, including the source flags; this
// function appends the --output target and owns the export step, so every
// declared-schema path (SQL and Avro) shares one import, export, and parse
// pipeline and differs only in importArgs.
//
// The intermediate contract is written to a temp file the function owns and
// removes, so concurrent imports never collide on a fixed path and nothing
// is left behind on the caller's disk.
func (im *Importer) importToODCS(ctx context.Context, importArgs []string) (odcs.Contract, error) {
	tmp, err := os.CreateTemp("", "declimport-*.yaml")
	if err != nil {
		return odcs.Contract{}, fmt.Errorf("declimport: create temp contract: %w", err)
	}
	intermediate := tmp.Name()
	// Close immediately: the CLI writes the file by path, and a lingering
	// open handle is just a leak. Removal is deferred so it runs on every
	// exit path, including the error returns below.
	_ = tmp.Close()
	defer func() { _ = os.Remove(intermediate) }()

	// Copy rather than append onto importArgs: this function is the shared
	// seam for every declared-schema path (SQL and Avro today), so a caller
	// that hands in a slice with spare capacity must not have its backing
	// array overwritten here.
	fullImportArgs := append(append([]string(nil), importArgs...), "--output", intermediate)
	if out, runErr := im.Runner.Run(ctx, im.Binary, fullImportArgs...); runErr != nil {
		return odcs.Contract{}, fmt.Errorf("declimport: %s %s: %w: %s",
			im.Binary, strings.Join(fullImportArgs, " "), runErr, strings.TrimSpace(string(out)))
	}

	exportArgs := []string{"export", "odcs", intermediate}
	out, runErr := im.Runner.Run(ctx, im.Binary, exportArgs...)
	if runErr != nil {
		return odcs.Contract{}, fmt.Errorf("declimport: %s %s: %w: %s",
			im.Binary, strings.Join(exportArgs, " "), runErr, strings.TrimSpace(string(out)))
	}

	contract, err := odcs.UnmarshalYAML(out)
	if err != nil {
		return odcs.Contract{}, fmt.Errorf("declimport: parse ODCS export: %w", err)
	}
	if contract.APIVersion == "" || len(contract.Schema) == 0 {
		return odcs.Contract{}, fmt.Errorf("declimport: ODCS export has no schema objects (apiVersion %q)", contract.APIVersion)
	}
	return contract, nil
}
