package excelcontract

import (
	"errors"
	"strings"

	"github.com/xuri/excelize/v2"
)

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
