package excelcontract

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/profile"
	"github.com/xuri/excelize/v2"
)

// MaxShapeRows is the most data rows of a table whose cell tokens the
// field shape counts. A few thousand rows settle a column's dominant
// mask, and the bound caps the cost of a large sheet.
const MaxShapeRows = 10000

// ErrNoSuchTable reports that a locator names no table in the workbook.
var ErrNoSuchTable = errors.New("no such table")

// Cell is one non-null data cell of a table.
type Cell struct {
	// Ref is the cell's A1 reference, such as "C12".
	Ref string
	// Value is the display text, as the sheet shows it.
	Value string
	// Raw is the stored value. The field shape takes its length from Raw.
	Raw string
	// Token is the mask that the field shape counts for this cell.
	Token string
}

// LocateTable finds a table by the locator that AnalyzeReader gives its
// schema (XL-2): the bare sheet name for the one table of a sheet, or
// "<sheet>#<ordinal>" for a sheet of several tables. It fails with
// ErrNoSuchTable when the workbook has no such table.
func LocateTable(f *excelize.File, locator string) (string, Segment, error) {
	if seg, ok := sheetTable(f, locator, 0); ok {
		return locator, seg, nil
	}
	i := strings.LastIndex(locator, "#")
	if i < 0 {
		return "", Segment{}, fmt.Errorf("%w: %q", ErrNoSuchTable, locator)
	}
	sheet := locator[:i]
	ordinal, err := strconv.Atoi(locator[i+1:])
	if err != nil || ordinal < 1 {
		return "", Segment{}, fmt.Errorf("%w: %q", ErrNoSuchTable, locator)
	}
	if seg, ok := sheetTable(f, sheet, ordinal); ok {
		return sheet, seg, nil
	}
	return "", Segment{}, fmt.Errorf("%w: %q", ErrNoSuchTable, locator)
}

// sheetTable segments a sheet as analyzeSheet does and picks one table.
// Ordinal 0 asks for the only table of a one-table sheet. An ordinal of
// 1 or more asks for that table of a sheet with several tables.
func sheetTable(f *excelize.File, sheet string, ordinal int) (Segment, bool) {
	rows, err := f.GetRows(sheet)
	if err != nil {
		return Segment{}, false
	}
	tables := SegmentSheet(rows, DeclaredTables(f, sheet)).Tables
	switch {
	case ordinal == 0 && len(tables) == 1:
		return tables[0], true
	case ordinal >= 1 && len(tables) > 1 && ordinal <= len(tables):
		return tables[ordinal-1], true
	default:
		return Segment{}, false
	}
}

// ColumnCells returns up to limit non-null data cells of one column of a
// table, in row order. Column counts from 0 at the table's first column.
// It reads the same rows as the field shape, so the tokens of
// ColumnCells(..., MaxShapeRows) give the field's shape.
func ColumnCells(f *excelize.File, sheet string, seg Segment, column, limit int) ([]Cell, error) {
	if column < 0 || column > seg.EndCol-seg.StartCol {
		return nil, fmt.Errorf("column %d is outside the table's %d columns", column, seg.EndCol-seg.StartCol+1)
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}
	s := sheetCells{rows: rows}
	return s.tableCells(seg, column, limit), nil
}

// sheetCells reads the cells of one sheet. It holds the display grid
// that GetRows returns.
type sheetCells struct {
	rows [][]string
}

// tableCells is the one reader of a table's cells and tokens. It returns
// up to limit non-null cells of one column. It reads at most the first
// MaxShapeRows data rows and skips a row that is blank across the table,
// as buildTableSchema does.
func (s *sheetCells) tableCells(seg Segment, column, limit int) []Cell {
	start := seg.StartRow
	if seg.HasHeader {
		start++
	}
	var cells []Cell
	dataRows := 0
	for r := start; r <= seg.EndRow && dataRows < MaxShapeRows && len(cells) < limit; r++ {
		if isEmptyRow(cellRange(s.rows, r, seg.StartCol, seg.EndCol)) {
			continue
		}
		dataRows++
		value := cellRange(s.rows, r, seg.StartCol+column, seg.StartCol+column)[0]
		if profile.IsNull(value) {
			continue
		}
		cells = append(cells, Cell{
			Ref:   cellName(r, seg.StartCol+column),
			Value: value,
			Raw:   value,
			Token: profile.FormatMask(value),
		})
	}
	return cells
}

// cellShape counts the tokens of the given cells into a shape signature.
func cellShape(cells []Cell) profile.ShapeSignature {
	var tracker profile.ShapeTracker
	for _, c := range cells {
		tracker.ObserveMask(c.Token, len([]rune(c.Raw)))
	}
	return tracker.Signature()
}
