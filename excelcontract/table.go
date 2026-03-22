package excelcontract

import (
	"errors"
	"strings"

	"github.com/xuri/excelize/v2"
)

// dataRegion describes where a table's data starts in a sheet.
type dataRegion struct {
	headerRow int // 0-indexed row index of the header
	startRow  int // 0-indexed row index of the first data row
}

// detectDataRegion finds the header and data start rows for a sheet.
// It first checks for Excel Table objects (which have explicit ranges),
// then falls back to heuristic header detection.
func detectDataRegion(f *excelize.File, sheet string, rows [][]string) *dataRegion {
	// Priority 1: Excel Table objects have explicit header ranges.
	var tables []excelize.Table
	if f != nil {
		var err error
		tables, err = f.GetTables(sheet)
		if err != nil {
			tables = nil
		}
	}
	if len(tables) > 0 {
		// Use the first table's range. The header is the first row of the range.
		ref := tables[0].Range
		topLeft, _, err := parseRange(ref)
		if err == nil {
			return &dataRegion{
				headerRow: topLeft.row,
				startRow:  topLeft.row + 1,
			}
		}
	}

	// Priority 2: heuristic header detection.
	// The header is the first non-empty row where at least one cell is
	// non-numeric (same logic as CSV header detection).
	for i, row := range rows {
		if isEmptyRow(row) {
			continue
		}
		return &dataRegion{
			headerRow: i,
			startRow:  i + 1,
		}
	}

	return nil
}

// cellRef holds a 0-indexed row and column.
type cellRef struct {
	row int
	col int
}

// parseRange parses an Excel range like "A1:C4" into top-left and
// bottom-right cell references (0-indexed).
func parseRange(ref string) (topLeft, bottomRight cellRef, err error) {
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) != 2 {
		return cellRef{}, cellRef{}, errInvalidRange
	}
	col1, row1, err := excelize.CellNameToCoordinates(parts[0])
	if err != nil {
		return cellRef{}, cellRef{}, err
	}
	col2, row2, err := excelize.CellNameToCoordinates(parts[1])
	if err != nil {
		return cellRef{}, cellRef{}, err
	}
	// CellNameToCoordinates returns 1-indexed; convert to 0-indexed.
	return cellRef{row: row1 - 1, col: col1 - 1},
		cellRef{row: row2 - 1, col: col2 - 1}, nil
}

// isEmptyRow returns true if every cell in the row is empty or whitespace.
func isEmptyRow(row []string) bool {
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

// errInvalidRange is returned when a range string cannot be parsed.
var errInvalidRange = errors.New("invalid range")
