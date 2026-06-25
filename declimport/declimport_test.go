package declimport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// odcsExport is a faithful ODCS v3.1.0 export of a small two-column table,
// matching what `datacontract export odcs` writes for a Postgres DDL. The
// fake Runner returns it for the export step so the parse path is exercised
// without the real tool.
const odcsExport = `version: 1.0.0
kind: DataContract
apiVersion: v3.1.0
id: my-data-contract
name: My Data Contract
status: draft
schema:
- name: customers
  physicalType: table
  logicalType: object
  physicalName: customers
  properties:
  - name: id
    physicalType: INT
    primaryKey: true
    primaryKeyPosition: 1
    logicalType: integer
  - name: email
    physicalType: TEXT
    logicalType: string
    required: true
`

// call records one Runner invocation so a test can assert the argv built for
// each CLI step.
type call struct {
	name string
	args []string
}

// fakeRunner is the test seam standing in for the Data Contract CLI. For each
// call it returns the queued output/error in order, recording the argv. A
// step keyed by its first two args (the subcommand) can also be made to fail,
// which is how the import-failure and export-failure paths are driven.
type fakeRunner struct {
	calls   []call
	outputs [][]byte
	errs    []error
	failOn  string // subcommand prefix, e.g. "import sql" or "export odcs"
	failErr error
	failOut []byte
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, call{name: name, args: append([]string(nil), args...)})
	joined := strings.Join(args, " ")
	if f.failOn != "" && strings.HasPrefix(joined, f.failOn) {
		return f.failOut, f.failErr
	}
	idx := len(f.calls) - 1
	var out []byte
	if idx < len(f.outputs) {
		out = f.outputs[idx]
	}
	var err error
	if idx < len(f.errs) {
		err = f.errs[idx]
	}
	return out, err
}

// writeDDL creates a DDL file in a temp dir and returns its path.
func writeDDL(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ddl.sql")
	if err := os.WriteFile(path, []byte("CREATE TABLE customers (id integer);"), 0o600); err != nil {
		t.Fatalf("write ddl: %v", err)
	}
	return path
}

func TestNewPanicsOnNilRunner(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("New(nil) did not panic")
		}
	}()
	New(nil)
}

func TestFromSQLDDL_Success(t *testing.T) {
	ddl := writeDDL(t)
	fr := &fakeRunner{outputs: [][]byte{nil, []byte(odcsExport)}}
	im := New(fr)

	got, err := im.FromSQLDDL(context.Background(), ddl, "postgres")
	if err != nil {
		t.Fatalf("FromSQLDDL: %v", err)
	}

	// The contract is the parsed ODCS export, not the intermediate.
	if got.APIVersion != "v3.1.0" || got.Kind != "DataContract" {
		t.Fatalf("contract header = %q/%q, want v3.1.0/DataContract", got.APIVersion, got.Kind)
	}
	if len(got.Schema) != 1 || got.Schema[0].Name != "customers" {
		t.Fatalf("schema = %+v, want one object named customers", got.Schema)
	}
	if len(got.Schema[0].Properties) != 2 {
		t.Fatalf("properties = %d, want 2", len(got.Schema[0].Properties))
	}
	id := got.Schema[0].Properties[0]
	if id.Name != "id" || id.LogicalType != "integer" || id.PhysicalType != "INT" {
		t.Fatalf("id property = %+v, want integer/INT", id)
	}
	if id.PrimaryKey == nil || !*id.PrimaryKey {
		t.Fatalf("id primaryKey = %v, want true", id.PrimaryKey)
	}
	email := got.Schema[0].Properties[1]
	if email.Required == nil || !*email.Required {
		t.Fatalf("email required = %v, want true", email.Required)
	}

	// Two steps in order, with the expected argv. Both steps share one
	// intermediate path: the import's --output and the export's location.
	if len(fr.calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(fr.calls))
	}
	imp := fr.calls[0]
	if imp.name != DefaultBinary {
		t.Fatalf("import binary = %q, want %q", imp.name, DefaultBinary)
	}
	wantImportHead := []string{"import", "sql", "--source", ddl, "--dialect", "postgres", "--output"}
	if !startsWith(imp.args, wantImportHead) {
		t.Fatalf("import argv = %v, want prefix %v", imp.args, wantImportHead)
	}
	intermediate := imp.args[len(imp.args)-1]
	exp := fr.calls[1]
	wantExport := []string{"export", "odcs", intermediate}
	if !equal(exp.args, wantExport) {
		t.Fatalf("export argv = %v, want %v", exp.args, wantExport)
	}

	// The intermediate temp file is cleaned up.
	if _, statErr := os.Stat(intermediate); !os.IsNotExist(statErr) {
		t.Fatalf("intermediate %q not removed (stat err %v)", intermediate, statErr)
	}
}

func TestFromSQLDDL_EmptyPath(t *testing.T) {
	im := New(&fakeRunner{})
	_, err := im.FromSQLDDL(context.Background(), "  ", "postgres")
	if err == nil || !strings.Contains(err.Error(), "empty DDL path") {
		t.Fatalf("err = %v, want empty DDL path", err)
	}
}

func TestFromSQLDDL_EmptyDialect(t *testing.T) {
	im := New(&fakeRunner{})
	_, err := im.FromSQLDDL(context.Background(), writeDDL(t), "")
	if err == nil || !strings.Contains(err.Error(), "empty SQL dialect") {
		t.Fatalf("err = %v, want empty SQL dialect", err)
	}
}

func TestFromSQLDDL_MissingFile(t *testing.T) {
	im := New(&fakeRunner{})
	_, err := im.FromSQLDDL(context.Background(), filepath.Join(t.TempDir(), "nope.sql"), "postgres")
	if err == nil || !strings.Contains(err.Error(), "DDL file") {
		t.Fatalf("err = %v, want DDL file error", err)
	}
}

func TestFromSQLDDL_ImportFails(t *testing.T) {
	fr := &fakeRunner{
		failOn:  "import sql",
		failErr: errors.New("exit status 1"),
		failOut: []byte("could not parse DDL"),
	}
	im := New(fr)
	_, err := im.FromSQLDDL(context.Background(), writeDDL(t), "postgres")
	if err == nil {
		t.Fatal("want error on import failure")
	}
	if !strings.Contains(err.Error(), "exit status 1") || !strings.Contains(err.Error(), "could not parse DDL") {
		t.Fatalf("err = %v, want wrapped CLI exit and diagnostics", err)
	}
	// Failed before the export step.
	if len(fr.calls) != 1 {
		t.Fatalf("calls = %d, want 1 (no export after import failure)", len(fr.calls))
	}
}

func TestFromSQLDDL_ExportFails(t *testing.T) {
	fr := &fakeRunner{
		failOn:  "export odcs",
		failErr: errors.New("exit status 2"),
		failOut: []byte("contract missing status"),
	}
	im := New(fr)
	_, err := im.FromSQLDDL(context.Background(), writeDDL(t), "postgres")
	if err == nil {
		t.Fatal("want error on export failure")
	}
	if !strings.Contains(err.Error(), "exit status 2") || !strings.Contains(err.Error(), "contract missing status") {
		t.Fatalf("err = %v, want wrapped export exit and diagnostics", err)
	}
}

func TestFromSQLDDL_UnparseableExport(t *testing.T) {
	fr := &fakeRunner{outputs: [][]byte{nil, []byte("\tnot: [valid: yaml")}}
	im := New(fr)
	_, err := im.FromSQLDDL(context.Background(), writeDDL(t), "postgres")
	if err == nil || !strings.Contains(err.Error(), "parse ODCS export") {
		t.Fatalf("err = %v, want parse ODCS export error", err)
	}
}

func TestFromSQLDDL_EmptyExport(t *testing.T) {
	// Valid YAML, but no schema objects: fail closed rather than return an
	// empty contract that downstream would treat as a real (empty) source.
	fr := &fakeRunner{outputs: [][]byte{nil, []byte("apiVersion: v3.1.0\nkind: DataContract\n")}}
	im := New(fr)
	_, err := im.FromSQLDDL(context.Background(), writeDDL(t), "postgres")
	if err == nil || !strings.Contains(err.Error(), "no schema objects") {
		t.Fatalf("err = %v, want no schema objects error", err)
	}
}

// TestFromSQLDDL_TempCreateFails drives the branch where the intermediate
// contract file cannot be created, by pointing the temp dir at a path that
// does not exist. The importer must fail closed before running any CLI step.
func TestFromSQLDDL_TempCreateFails(t *testing.T) {
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "does-not-exist"))
	fr := &fakeRunner{}
	im := New(fr)
	_, err := im.FromSQLDDL(context.Background(), writeDDL(t), "postgres")
	if err == nil || !strings.Contains(err.Error(), "create temp contract") {
		t.Fatalf("err = %v, want create temp contract error", err)
	}
	if len(fr.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (no CLI before temp file)", len(fr.calls))
	}
}

// TestPackageFromSQLDDL covers the package-level convenience, which binds the
// real execRunner. It is driven down the early validation path (empty
// dialect) so no subprocess runs and the test needs no Python toolchain.
func TestPackageFromSQLDDL_Validates(t *testing.T) {
	_, err := FromSQLDDL(context.Background(), writeDDL(t), "")
	if err == nil || !strings.Contains(err.Error(), "empty SQL dialect") {
		t.Fatalf("err = %v, want empty SQL dialect", err)
	}
}

// TestExecRunner exercises the one os/exec glue line without the Data
// Contract CLI by re-executing this test binary as the subprocess (the
// standard library's helper-process pattern). The helper echoes a marker and
// chooses its exit code from an env var, so both the success and the non-zero
// exit branches of execRunner.Run are covered.
func TestExecRunner(t *testing.T) {
	helper := []string{"-test.run=TestHelperProcess", "--", "marker-output"}

	t.Run("success", func(t *testing.T) {
		t.Setenv("GO_WANT_HELPER_PROCESS", "1")
		out, err := execRunner{}.Run(context.Background(), os.Args[0], helper...)
		if err != nil {
			t.Fatalf("Run: %v (out %q)", err, out)
		}
		if !strings.Contains(string(out), "marker-output") {
			t.Fatalf("combined output = %q, want marker-output", out)
		}
	})

	t.Run("nonzero exit", func(t *testing.T) {
		t.Setenv("GO_WANT_HELPER_PROCESS", "1")
		t.Setenv("DECLIMPORT_HELPER_EXIT", "3")
		out, err := execRunner{}.Run(context.Background(), os.Args[0], helper...)
		if err == nil {
			t.Fatalf("want error on non-zero exit, out %q", out)
		}
		if !strings.Contains(string(out), "marker-output") {
			t.Fatalf("combined output = %q, want marker-output even on failure", out)
		}
	})
}

// TestHelperProcess is not a real test: it is the subprocess execRunner runs
// in TestExecRunner. Guarded by GO_WANT_HELPER_PROCESS so it no-ops during a
// normal test run and only acts when re-executed as the child. It echoes its
// trailing args and exits with the code in DECLIMPORT_HELPER_EXIT (0 when
// unset), standing in for the CLI.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			for _, marker := range args[i+1:] {
				_, _ = os.Stdout.WriteString(marker + "\n")
			}
			break
		}
	}
	if code := os.Getenv("DECLIMPORT_HELPER_EXIT"); code != "" {
		n, _ := strconv.Atoi(code)
		os.Exit(n)
	}
	os.Exit(0)
}

func startsWith(got, prefix []string) bool {
	if len(got) < len(prefix) {
		return false
	}
	return equal(got[:len(prefix)], prefix)
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
