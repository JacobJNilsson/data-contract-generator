package profile

import (
	"fmt"
	"strings"
)

// HeaderProbeRows is the recommended number of data rows to collect and
// pass to DetectHeaderWithRows. It is large enough to establish stable
// column value classes and small enough to keep memory bounded for
// streaming analyzers.
const HeaderProbeRows = 32

// DetectHeader returns true if the first row looks like a header rather
// than data, judging by the first row alone. The heuristic: if every
// non-empty cell in the first row is numeric, it is probably data. If at
// least one cell is non-numeric and non-empty, it is probably a header.
//
// When subsequent rows are available, prefer DetectHeaderWithRows, which
// also compares the first row against the column value classes and
// catches headerless files whose first row contains text.
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

// DetectHeaderWithRows returns true if the first row looks like a header
// rather than data, using the rows that follow it as evidence.
//
// It starts from the single-row heuristic (DetectHeader): an all-numeric
// first row is data. When the first row contains text it then compares
// each first-row cell against the value class of its column in dataRows.
// A first-row cell that parses as numeric, date, timestamp, or boolean
// in a column whose remaining values share one of those classes is
// strong evidence against a header: real headers are names, not
// numbers, dates, or true/false literals. One such column is enough to
// conclude the file has no header, because a homogeneous first row
// would otherwise be promoted to field names and consumers would
// silently drop the first record.
//
// All-text files are genuinely ambiguous: a text header over text
// columns and a headerless text file produce identical value classes.
// For those we keep the historical behavior and report a header, since
// no column-class evidence can distinguish the two cases.
//
// dataRows should be a bounded probe of the rows after the first one;
// HeaderProbeRows is a sensible size. Passing no rows degrades to
// DetectHeader.
func DetectHeaderWithRows(firstRow []string, dataRows [][]string) bool {
	hasHeader, _ := DetectHeaderWithRowsConfidence(firstRow, dataRows)
	return hasHeader
}

// DetectHeaderWithRowsConfidence is DetectHeaderWithRows with the
// verdict's confidence exposed (issue #81). guessed is true when a
// header was reported by the documented all-text fallback: no column
// carried a numeric, date, timestamp, or boolean class to anchor the
// decision, so a text header over text columns and a headerless text
// file are indistinguishable and the historical header guess applies.
// Callers can surface that uncertainty instead of expressing full
// confidence in the contract.
func DetectHeaderWithRowsConfidence(firstRow []string, dataRows [][]string) (hasHeader, guessed bool) {
	if !DetectHeader(firstRow) {
		return false, false
	}
	anchored := false
	for col, cell := range firstRow {
		if !isDataValueClass(columnClass(dataRows, col)) {
			continue
		}
		// A typed column anchors the verdict either way: a first-row
		// cell of the same kind means data, a name over typed values
		// means a real header.
		anchored = true
		if isDataValueClass(ClassifyCell(cell)) {
			return false, false
		}
	}
	return true, !anchored
}

// isDataValueClass reports whether a cell class is one real header
// names never have: numeric, date, timestamp, or boolean.
func isDataValueClass(dt DataType) bool {
	return dt == TypeNumeric || dt == TypeDate || dt == TypeTimestamp || dt == TypeBoolean
}

// columnClass merges the cell classes of one column across rows, using
// the same priority order as type inference (see TypePriority). Rows
// shorter than col contribute an empty cell.
func columnClass(rows [][]string, col int) DataType {
	class := TypeEmpty
	for _, row := range rows {
		var value string
		if col < len(row) {
			value = row[col]
		}
		class = MergeTypes(class, ClassifyCell(value))
	}
	return class
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
