package profile

import (
	"fmt"
	"strings"
)

// DetectHeader returns true if the first row looks like a header rather
// than data. The heuristic: if every non-empty cell in the first row is
// numeric, it is probably data. If at least one cell is non-numeric and
// non-empty, it is probably a header.
//
// This uses the same IsNumeric function as type inference to ensure
// consistent behavior.
func DetectHeader(firstRow []string) bool {
	if len(firstRow) == 0 {
		return false
	}
	for _, cell := range firstRow {
		trimmed := strings.TrimSpace(cell)
		if trimmed == "" {
			continue
		}
		if !IsNumeric(trimmed) {
			return true
		}
	}
	return false
}

// GenerateFieldNames returns column names for a headerless file by
// producing "column_1", "column_2", etc.
func GenerateFieldNames(count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("column_%d", i+1)
	}
	return names
}
