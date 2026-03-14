package csvcontract

import (
	"fmt"
	"strings"
)

// detectHeader returns true if the first row looks like a header rather
// than data. The heuristic: if every non-empty cell in the first row is
// numeric, it is probably data. If at least one cell is non-numeric and
// non-empty, it is probably a header.
//
// This uses the same isNumeric function as type inference to ensure
// consistent behavior.
func detectHeader(firstRow []string) bool {
	if len(firstRow) == 0 {
		return false
	}
	for _, cell := range firstRow {
		trimmed := strings.TrimSpace(cell)
		if trimmed == "" {
			continue
		}
		if !isNumeric(trimmed) {
			return true
		}
	}
	return false
}

// generateFieldNames returns column names for a headerless CSV by
// producing "column_1", "column_2", etc.
func generateFieldNames(count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("column_%d", i+1)
	}
	return names
}
