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
	s, err := newSheetCells(f, sheet, rows)
	if err != nil {
		return nil, err
	}
	return s.tableCells(seg, column, limit)
}

// sheetCells reads the cells of one sheet. It holds the display grid and
// the raw grid, which share their coordinates, and a cache from style ID
// to whether the style's number format is a date format.
type sheetCells struct {
	f          *excelize.File
	sheet      string
	rows       [][]string
	raw        [][]string
	dateStyles map[int]bool
}

// newSheetCells reads the raw grid of a sheet beside its display grid.
// With RawCellValue, excelize returns each value as the file stores it:
// a number as written, such as "1500.5" or "1.5E-3", a boolean as "1" or
// "0", a cell of type d as ISO text, and a formula without a cached value
// as "".
func newSheetCells(f *excelize.File, sheet string, rows [][]string) (*sheetCells, error) {
	raw, err := f.GetRows(sheet, excelize.Options{RawCellValue: true})
	if err != nil {
		return nil, fmt.Errorf("read raw rows: %w", err)
	}
	return &sheetCells{f: f, sheet: sheet, rows: rows, raw: raw, dateStyles: map[int]bool{}}, nil
}

// tableCells is the one reader of a table's cells and tokens. It returns
// up to limit non-null cells of one column. It reads at most the first
// MaxShapeRows data rows and skips a row that is blank across the table,
// as buildTableSchema does. A cell with an empty raw value is null.
func (s *sheetCells) tableCells(seg Segment, column, limit int) ([]Cell, error) {
	start := seg.StartRow
	if seg.HasHeader {
		start++
	}
	col := seg.StartCol + column
	var cells []Cell
	dataRows := 0
	for r := start; r <= seg.EndRow && dataRows < MaxShapeRows && len(cells) < limit; r++ {
		if isEmptyRow(cellRange(s.rows, r, seg.StartCol, seg.EndCol)) {
			continue
		}
		dataRows++
		raw := cellRange(s.raw, r, col, col)[0]
		if raw == "" {
			continue
		}
		ref := cellName(r, col)
		kind, err := s.cellKindOf(ref)
		if err != nil {
			return nil, fmt.Errorf("cell %s: %w", ref, err)
		}
		cells = append(cells, Cell{
			Ref:   ref,
			Value: cellRange(s.rows, r, col, col)[0],
			Raw:   raw,
			Token: cellToken(kind, raw),
		})
	}
	return cells, nil
}

// cellKind is the type of a cell's stored value, as the shape token
// names it.
type cellKind string

const (
	kindText   cellKind = "text"
	kindNumber cellKind = "number"
	kindDate   cellKind = "date"
	kindBool   cellKind = "bool"
	kindError  cellKind = "error"
)

// cellToken renders the token that the field shape counts for one cell.
// A text or number cell adds the format mask of its raw value, such as
// "number 9.9" or "text A-9". A date, boolean, or error cell is the kind
// word alone, because its raw text has no useful shape. FormatMask never
// writes a lowercase letter or a space, so no token equals a mask of
// display text.
func cellToken(kind cellKind, raw string) string {
	if kind == kindText || kind == kindNumber {
		return string(kind) + " " + profile.FormatMask(raw)
	}
	return string(kind)
}

// cellKindOf reads the kind of a cell whose raw value is not empty.
// GetCellType returns CellTypeUnset, not CellTypeNumber, for a number
// cell without a t attribute, which is how writers store numbers and
// numeric formula results. So CellTypeUnset and CellTypeNumber are both
// numbers here. CellTypeFormula is a formula with a string result (t=str)
// and counts as text. A number is a date when its style's number format
// is a date format.
func (s *sheetCells) cellKindOf(ref string) (cellKind, error) {
	ct, err := s.f.GetCellType(s.sheet, ref)
	if err != nil {
		return "", err
	}
	switch ct {
	case excelize.CellTypeSharedString, excelize.CellTypeInlineString, excelize.CellTypeFormula:
		return kindText, nil
	case excelize.CellTypeBool:
		return kindBool, nil
	case excelize.CellTypeError:
		return kindError, nil
	case excelize.CellTypeDate:
		return kindDate, nil
	}
	date, err := s.dateStyle(ref)
	if err != nil {
		return "", err
	}
	if date {
		return kindDate, nil
	}
	return kindNumber, nil
}

// dateStyle reports whether the cell's number format is a date format.
// GetCellStyle falls back to the row style and then to the column style
// when the cell has none. The answer is cached per style ID.
func (s *sheetCells) dateStyle(ref string) (bool, error) {
	id, err := s.f.GetCellStyle(s.sheet, ref)
	if err != nil {
		return false, err
	}
	if date, ok := s.dateStyles[id]; ok {
		return date, nil
	}
	date := false
	// GetStyle fails for a style ID the workbook does not define, such as
	// ID 0 in a workbook without a style table. Such a cell has the
	// General format, which is no date format.
	if style, err := s.f.GetStyle(id); err == nil {
		date = isDateFormat(style)
	}
	s.dateStyles[id] = date
	return date, nil
}

// isDateFormat reports whether a style's number format is a date format.
// GetStyle sets CustomNumFmt for a custom format. For a custom code that
// equals one of its currency formats it also sets NumFmt, so the custom
// code decides first, and the built-in ID decides only without one.
func isDateFormat(style *excelize.Style) bool {
	if style.CustomNumFmt != nil {
		return isDateFormatCode(*style.CustomNumFmt)
	}
	return isDateFormatID(style.NumFmt)
}

// isDateFormatID reports whether a built-in number format ID is a date
// or time format: IDs 14 to 22 and 45 to 47, and the locale date IDs 27
// to 36, 50 to 62, and 67 to 81.
func isDateFormatID(id int) bool {
	return (14 <= id && id <= 22) || (45 <= id && id <= 47) ||
		(27 <= id && id <= 36) || (50 <= id && id <= 62) || (67 <= id && id <= 81)
}

// isDateFormatCode reports whether a custom format code is a date or time
// format: it holds d, m, y, h, or s outside quoted text, outside brackets
// such as [Red] or [$-409], and not escaped by a backslash. So `0" mm"`
// is no date format, and "d mmm yyyy" is one.
func isDateFormatCode(code string) bool {
	inQuote, inBracket, escaped := false, false, false
	for _, r := range code {
		switch {
		case escaped:
			escaped = false
		case inQuote:
			inQuote = r != '"'
		case inBracket:
			inBracket = r != ']'
		case r == '"':
			inQuote = true
		case r == '[':
			inBracket = true
		case r == '\\':
			escaped = true
		case strings.ContainsRune("dmyhsDMYHS", r):
			return true
		}
	}
	return false
}

// cellShape counts the tokens of the given cells into a shape signature.
func cellShape(cells []Cell) profile.ShapeSignature {
	var tracker profile.ShapeTracker
	for _, c := range cells {
		tracker.ObserveMask(c.Token, len([]rune(c.Raw)))
	}
	return tracker.Signature()
}
