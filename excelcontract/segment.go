package excelcontract

import (
	"fmt"
	"sort"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/profile"
	"github.com/xuri/excelize/v2"
)

// Detection labels say how a table region was found. A Table object is
// author intent (exact header row, exact column bounds); a heuristic
// region is inferred from the cell layout. The label rides the schema
// metadata so the planner knows how much to trust the bounds.
const (
	DetectionTableObject = "table-object"
	DetectionHeuristic   = "heuristic"
)

// DeclaredTable is one Excel Table object as declared in the sheet: its
// name, its range reference, and whether its first row is a header. The
// analyzer builds these from excelize; a runtime caller (an authored
// pipeline that re-runs the segmenter to locate its tables) builds them
// the same way.
type DeclaredTable struct {
	Name      string
	Range     string
	HasHeader bool
}

// Segment is one detected table region in a sheet. Bounds are 0-indexed
// and inclusive; StartRow is the header row when HasHeader is true and
// the first data row otherwise. Caption rows peeled off the region are
// not inside the bounds.
type Segment struct {
	Detection string
	TableName string
	Caption   string
	HasHeader bool
	StartRow  int
	EndRow    int
	StartCol  int
	EndCol    int
	// Issues records per-table detection anomalies (a Table-object range
	// that reads emptier than declared) so they surface on the table's
	// schema rather than disappearing.
	Issues []string
}

// Segmentation is the deterministic segmentation of one sheet: every
// detected table in row-major anchor order, plus the sheet-level issues
// for cells that were seen but are not part of any table. Nothing the
// segmenter sees is silently dropped: it is a table, part of a table's
// caption, or an issue.
type Segmentation struct {
	Tables []Segment
	Issues []string
}

// SegmentSheet splits one sheet's cells into table regions. Detection
// priority follows XL-3: every declared Table object is honored with its
// full range (header row and column bounds), then the remaining cells
// are segmented heuristically into row bands separated by blank rows.
// Leading single-cell rows of a band are absorbed as the table's caption
// when a multi-cell row follows; a band that is a single stray cell is
// recorded as an issue, not invented as a table.
//
// The segmenter is deterministic and order-stable: same cells and same
// declared tables produce the same segmentation. It is exported because
// segmentation is parsing (XL-4): authored pipelines locate tables by
// re-running this same primitive at run time instead of hardcoding
// offsets that benign layout drift would break.
func SegmentSheet(rows [][]string, declared []DeclaredTable) Segmentation {
	grid := newOccupancy(rows)
	var out Segmentation
	for _, d := range declared {
		seg, issue := declaredSegment(grid, d)
		if issue != "" {
			out.Issues = append(out.Issues, issue)
		}
		if seg != nil {
			out.Tables = append(out.Tables, *seg)
		}
	}
	bands := freeRowBands(grid)
	for _, b := range bands {
		seg, issue := heuristicSegment(grid, b)
		if issue != "" {
			out.Issues = append(out.Issues, issue)
		}
		if seg != nil {
			out.Tables = append(out.Tables, *seg)
		}
	}
	// XL-2: ordinals are assigned in row-major order of each table's
	// top-left anchor, Table-object and heuristic tables together.
	sort.SliceStable(out.Tables, func(i, j int) bool {
		if out.Tables[i].StartRow != out.Tables[j].StartRow {
			return out.Tables[i].StartRow < out.Tables[j].StartRow
		}
		return out.Tables[i].StartCol < out.Tables[j].StartCol
	})
	return out
}

// occupancy is the sheet's cell grid: which cells hold non-whitespace
// content, and which of those are claimed by a declared Table-object
// range (heuristics never re-segment authored regions).
type occupancy struct {
	rows    [][]string
	claimed map[int]map[int]bool
}

func newOccupancy(rows [][]string) *occupancy {
	return &occupancy{rows: rows, claimed: map[int]map[int]bool{}}
}

// occupied reports whether the cell holds non-whitespace content.
func (g *occupancy) occupied(r, c int) bool {
	if r < 0 || r >= len(g.rows) || c < 0 || c >= len(g.rows[r]) {
		return false
	}
	return strings.TrimSpace(g.rows[r][c]) != ""
}

// free reports whether the cell is occupied and not claimed by a
// declared Table-object range.
func (g *occupancy) free(r, c int) bool {
	return g.occupied(r, c) && !g.claimed[r][c]
}

// claim marks a rectangle as owned by a declared Table object.
func (g *occupancy) claim(r1, c1, r2, c2 int) {
	for r := r1; r <= r2; r++ {
		if g.claimed[r] == nil {
			g.claimed[r] = map[int]bool{}
		}
		for c := c1; c <= c2; c++ {
			g.claimed[r][c] = true
		}
	}
}

// width is the widest row's cell count.
func (g *occupancy) width() int {
	w := 0
	for _, row := range g.rows {
		if len(row) > w {
			w = len(row)
		}
	}
	return w
}

// value returns the cell content, "" when out of bounds.
func (g *occupancy) value(r, c int) string {
	if r < 0 || r >= len(g.rows) || c < 0 || c >= len(g.rows[r]) {
		return ""
	}
	return g.rows[r][c]
}

// declaredSegment turns one declared Table object into a Segment. The
// declared range is claimed even when it turns out empty, so heuristics
// never re-segment an authored region. An unparseable range cannot be
// claimed; its cells fall through to heuristic segmentation, loudly.
func declaredSegment(g *occupancy, d DeclaredTable) (*Segment, string) {
	tl, br, err := parseRange(d.Range)
	if err != nil {
		return nil, fmt.Sprintf("table object %q has an unparseable range %q; its cells fall back to heuristic segmentation", d.Name, d.Range)
	}
	g.claim(tl.row, tl.col, br.row, br.col)
	// The occupied extent inside the declared range: a range declared
	// past its data (or saved without cached formula values, XL-L4)
	// reads emptier than its extent. Clip rows to what is really there
	// and say so; keep the declared column bounds, which are the point
	// of honoring the object (XL-3).
	lastRow := -1
	for r := tl.row; r <= br.row; r++ {
		for c := tl.col; c <= br.col; c++ {
			if g.occupied(r, c) {
				lastRow = r
				break
			}
		}
	}
	if lastRow < 0 {
		return nil, fmt.Sprintf("table object %q declares range %s but the range is empty; no table was emitted for it", d.Name, d.Range)
	}
	seg := &Segment{
		Detection: DetectionTableObject,
		TableName: d.Name,
		HasHeader: d.HasHeader,
		StartRow:  tl.row,
		EndRow:    lastRow,
		StartCol:  tl.col,
		EndCol:    br.col,
	}
	if lastRow < br.row {
		seg.Issues = append(seg.Issues, fmt.Sprintf("table object %q declares range %s but its data ends at row %d; the declared extent reads emptier than declared", d.Name, d.Range, lastRow+1))
	}
	return seg, ""
}

// band is a maximal run of rows that each hold at least one free cell,
// bounded by blank (or fully claimed) rows.
type band struct {
	startRow, endRow int
}

// freeRowBands splits the sheet's unclaimed cells into row bands: a
// blank row is a separator (XL-1), never skipped.
func freeRowBands(g *occupancy) []band {
	var bands []band
	w := g.width()
	inBand := false
	start := 0
	for r := 0; r < len(g.rows); r++ {
		has := false
		for c := 0; c < w; c++ {
			if g.free(r, c) {
				has = true
				break
			}
		}
		switch {
		case has && !inBand:
			inBand, start = true, r
		case !has && inBand:
			inBand = false
			bands = append(bands, band{startRow: start, endRow: r - 1})
		}
	}
	if inBand {
		bands = append(bands, band{startRow: start, endRow: len(g.rows) - 1})
	}
	return bands
}

// heuristicSegment turns one row band into a table Segment, absorbing a
// leading caption and refusing to invent a table from a stray cell.
func heuristicSegment(g *occupancy, b band) (*Segment, string) {
	w := g.width()
	// Caption absorption (XL §4-4): every leading row of the band that
	// holds exactly one free cell is caption, provided a wider (multi-
	// cell) row follows in the band. Lookahead over the whole band, not
	// just the next row: multi-row titles are common in real workbooks.
	firstMulti := -1
	for r := b.startRow; r <= b.endRow; r++ {
		if freeCellCount(g, r, w) > 1 {
			firstMulti = r
			break
		}
	}
	startRow := b.startRow
	var captions []string
	if firstMulti > b.startRow {
		for r := b.startRow; r < firstMulti; r++ {
			captions = append(captions, singleFreeCellValue(g, r, w))
		}
		startRow = firstMulti
	}
	// Column extent over the table rows only: a caption cell may sit
	// outside the columns the table itself occupies.
	startCol, endCol := -1, -1
	for r := startRow; r <= b.endRow; r++ {
		for c := 0; c < w; c++ {
			if !g.free(r, c) {
				continue
			}
			if startCol < 0 || c < startCol {
				startCol = c
			}
			if c > endCol {
				endCol = c
			}
		}
	}
	// Noise guard (XL §4-5): a single cell is not a table. Record it so
	// nothing silently disappears.
	if startRow == b.endRow && freeCellCount(g, startRow, w) == 1 {
		return nil, fmt.Sprintf("stray cell at %s: %q", cellName(startRow, startCol), truncateCell(singleFreeCellValue(g, startRow, w)))
	}
	first := rowSlice(g, startRow, startCol, endCol)
	var probe [][]string
	for r := startRow + 1; r <= b.endRow && len(probe) < profile.HeaderProbeRows; r++ {
		probe = append(probe, rowSlice(g, r, startCol, endCol))
	}
	return &Segment{
		Detection: DetectionHeuristic,
		Caption:   strings.Join(captions, "\n"),
		HasHeader: profile.DetectHeaderWithRows(first, probe),
		StartRow:  startRow,
		EndRow:    b.endRow,
		StartCol:  startCol,
		EndCol:    endCol,
	}, ""
}

// freeCellCount counts the row's free cells.
func freeCellCount(g *occupancy, r, width int) int {
	n := 0
	for c := 0; c < width; c++ {
		if g.free(r, c) {
			n++
		}
	}
	return n
}

// singleFreeCellValue returns the trimmed value of the row's one free
// cell (the caption / stray-cell reader).
func singleFreeCellValue(g *occupancy, r, width int) string {
	for c := 0; c < width; c++ {
		if g.free(r, c) {
			return strings.TrimSpace(g.value(r, c))
		}
	}
	return ""
}

// rowSlice extracts the row's cells over the column bounds, padding
// short rows so every slice has the same width.
func rowSlice(g *occupancy, r, startCol, endCol int) []string {
	out := make([]string, endCol-startCol+1)
	for c := startCol; c <= endCol; c++ {
		out[c-startCol] = g.value(r, c)
	}
	return out
}

// truncateCell bounds a cell value for issue text, so one enormous cell
// cannot bloat the contract.
func truncateCell(v string) string {
	const maxIssueCell = 80
	if len(v) <= maxIssueCell {
		return v
	}
	return v[:maxIssueCell] + "…"
}

// cellName renders a 0-indexed (row, col) as an A1-style reference.
func cellName(row, col int) string {
	name, err := excelize.CoordinatesToCellName(col+1, row+1)
	if err != nil {
		// Coordinates here come from grid indices, which are always
		// valid; keep a deterministic fallback rather than a panic.
		return fmt.Sprintf("R%dC%d", row+1, col+1)
	}
	return name
}

// rangeRef renders 0-indexed inclusive bounds as an A1-style range.
func rangeRef(r1, c1, r2, c2 int) string {
	return cellName(r1, c1) + ":" + cellName(r2, c2)
}
