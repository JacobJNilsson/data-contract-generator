package declimport

import (
	"context"
	"os/exec"
)

// execRunner is the production Runner: the single point where declimport
// touches os/exec. It is deliberately the whole of the package's subprocess
// glue, so every other line of declimport runs against a fake Runner in
// tests. Run is exercised in unit tests via the standard re-exec helper
// pattern (a test process stands in for the CLI), and end to end against the
// real Data Contract CLI by the build-tagged integration test.
type execRunner struct{}

// Run executes name with args under ctx and returns the combined
// stdout+stderr. CommandContext ties the subprocess lifetime to ctx, so a
// cancelled or timed-out context kills the CLI rather than leaking it.
// CombinedOutput captures the CLI's own diagnostics, which the caller folds
// into the error message on a non-zero exit.
func (execRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
