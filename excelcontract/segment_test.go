package excelcontract

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

// --- SegmentSheet unit tests ------------------------------------------------

func TestSegmentStackedBands(t *testing.T) {
	rows := [][]string{
		{"Product", "Qty"},
		{"Widget", "4"},
		{"", ""},
		{"Name", "Team"},
		{"Alice", "Platform"},
	}
	seg := SegmentSheet(rows, nil)
	if len(seg.Tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(seg.Tables))
	}
	a, b := seg.Tables[0], seg.Tables[1]
	if a.StartRow != 0 || a.EndRow != 1 || b.StartRow != 3 || b.EndRow != 4 {
		t.Errorf("bounds = %+v / %+v", a, b)
	}
	if a.Detection != DetectionHeuristic || b.Detection != DetectionHeuristic {
		t.Errorf("detection = %q / %q, want heuristic", a.Detection, b.Detection)
	}
	if len(seg.Issues) != 0 {
		t.Errorf("issues = %v, want none", seg.Issues)
	}
}

func TestSegmentCaptionAbsorption(t *testing.T) {
	rows := [][]string{
		{"Kontoutdrag 2024"},
		{"Del 1"},
		{"Datum", "Belopp"},
		{"2024-01-02", "32000"},
	}
	seg := SegmentSheet(rows, nil)
	if len(seg.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(seg.Tables))
	}
	got := seg.Tables[0]
	if got.Caption != "Kontoutdrag 2024\nDel 1" {
		t.Errorf("caption = %q, want the two title rows concatenated", got.Caption)
	}
	if got.StartRow != 2 {
		t.Errorf("StartRow = %d, want 2 (caption peeled)", got.StartRow)
	}
	if !got.HasHeader {
		t.Error("HasHeader = false, want true")
	}
}

func TestSegmentCaptionOutsideDataColumns(t *testing.T) {
	// The caption sits in column D; the table occupies A..B. The table's
	// column extent must not include the caption's column.
	rows := [][]string{
		{"", "", "", "Rapport"},
		{"Datum", "Belopp"},
		{"2024-01-02", "32000"},
	}
	seg := SegmentSheet(rows, nil)
	if len(seg.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(seg.Tables))
	}
	got := seg.Tables[0]
	if got.Caption != "Rapport" {
		t.Errorf("caption = %q, want Rapport", got.Caption)
	}
	if got.StartCol != 0 || got.EndCol != 1 {
		t.Errorf("cols = %d..%d, want 0..1", got.StartCol, got.EndCol)
	}
}

func TestSegmentSingleColumnIsATableNotACaption(t *testing.T) {
	// No multi-cell row follows, so nothing is peeled: a single-column
	// table stays a table.
	rows := [][]string{
		{"Value"},
		{"42"},
		{"17"},
	}
	seg := SegmentSheet(rows, nil)
	if len(seg.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(seg.Tables))
	}
	if seg.Tables[0].Caption != "" {
		t.Errorf("caption = %q, want empty", seg.Tables[0].Caption)
	}
	if seg.Tables[0].StartRow != 0 || seg.Tables[0].EndRow != 2 {
		t.Errorf("bounds = %+v", seg.Tables[0])
	}
}

func TestSegmentStrayCell(t *testing.T) {
	rows := [][]string{
		{"Item", "Count"},
		{"Bolt", "120"},
		{"", ""},
		{"", "", "", "reviewed"},
	}
	seg := SegmentSheet(rows, nil)
	if len(seg.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(seg.Tables))
	}
	if len(seg.Issues) != 1 || !strings.Contains(seg.Issues[0], `stray cell at D4: "reviewed"`) {
		t.Errorf("issues = %v, want the stray cell recorded", seg.Issues)
	}
}

func TestSegmentDeclaredTableRanges(t *testing.T) {
	rows := [][]string{
		{"Product", "Revenue", "", "Quarter", "Target"},
		{"Widget", "1500", "", "Q1", "5000"},
		{"Gadget", "2300", "", "Q2", "6000"},
	}
	declared := []DeclaredTable{
		{Name: "Sales", Range: "A1:B3", HasHeader: true},
		{Name: "Targets", Range: "D1:E3", HasHeader: true},
	}
	seg := SegmentSheet(rows, declared)
	if len(seg.Tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(seg.Tables))
	}
	a, b := seg.Tables[0], seg.Tables[1]
	if a.TableName != "Sales" || b.TableName != "Targets" {
		t.Errorf("row-major order = %q, %q", a.TableName, b.TableName)
	}
	if a.StartCol != 0 || a.EndCol != 1 || b.StartCol != 3 || b.EndCol != 4 {
		t.Errorf("column bounds not respected: %+v / %+v", a, b)
	}
}

func TestSegmentDeclaredRangeEmptierThanDeclared(t *testing.T) {
	rows := [][]string{
		{"Product", "Revenue"},
		{"Widget", "1500"},
	}
	declared := []DeclaredTable{{Name: "Sales", Range: "A1:B10", HasHeader: true}}
	seg := SegmentSheet(rows, declared)
	if len(seg.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(seg.Tables))
	}
	got := seg.Tables[0]
	if got.EndRow != 1 {
		t.Errorf("EndRow = %d, want clipped to 1", got.EndRow)
	}
	if len(got.Issues) != 1 || !strings.Contains(got.Issues[0], "emptier than declared") {
		t.Errorf("issues = %v, want the emptier-than-declared flag", got.Issues)
	}
}

func TestSegmentDeclaredRangeEmpty(t *testing.T) {
	rows := [][]string{
		{"Name", "Age"},
		{"Alice", "30"},
	}
	declared := []DeclaredTable{{Name: "Ghost", Range: "E5:F8", HasHeader: true}}
	seg := SegmentSheet(rows, declared)
	if len(seg.Tables) != 1 {
		t.Fatalf("tables = %d, want 1 (the heuristic one)", len(seg.Tables))
	}
	if len(seg.Issues) != 1 || !strings.Contains(seg.Issues[0], `table object "Ghost"`) {
		t.Errorf("issues = %v, want the empty declared range recorded", seg.Issues)
	}
}

func TestSegmentDeclaredRangeUnparseable(t *testing.T) {
	rows := [][]string{
		{"Name", "Age"},
		{"Alice", "30"},
	}
	declared := []DeclaredTable{{Name: "Broken", Range: "!!!", HasHeader: true}}
	seg := SegmentSheet(rows, declared)
	// The cells fall back to heuristic segmentation.
	if len(seg.Tables) != 1 || seg.Tables[0].Detection != DetectionHeuristic {
		t.Fatalf("tables = %+v, want one heuristic table", seg.Tables)
	}
	if len(seg.Issues) != 1 || !strings.Contains(seg.Issues[0], "unparseable range") {
		t.Errorf("issues = %v, want the unparseable range recorded", seg.Issues)
	}
}

func TestSegmentDeterminism(t *testing.T) {
	rows := [][]string{
		{"Title"},
		{"A", "B"},
		{"1", "2"},
		{""},
		{"C", "D", "E"},
		{"3", "4", "5"},
	}
	first := SegmentSheet(rows, nil)
	second := SegmentSheet(rows, nil)
	if fmt.Sprintf("%+v", first) != fmt.Sprintf("%+v", second) {
		t.Errorf("segmentation is not deterministic:\n%+v\n%+v", first, second)
	}
}

func TestCellNameFallback(t *testing.T) {
	if got := cellName(0, 0); got != "A1" {
		t.Errorf("cellName(0,0) = %q, want A1", got)
	}
	// Out-of-range coordinates keep a deterministic fallback.
	if got := cellName(-2, -2); got != "R-1C-1" {
		t.Errorf("cellName(-2,-2) = %q, want R-1C-1", got)
	}
}

func TestTruncateCell(t *testing.T) {
	long := strings.Repeat("x", 100)
	got := truncateCell(long)
	if len([]rune(got)) != 81 || !strings.HasSuffix(got, "…") {
		t.Errorf("truncateCell long = %q", got)
	}
	if truncateCell("short") != "short" {
		t.Error("short values must pass through unchanged")
	}
}

// --- Analyzer integration over the spec fixtures ----------------------------

func TestAnalyzeMultiTableStacked(t *testing.T) {
	dc, err := AnalyzeFile(context.Background(), "testdata/multi-table-stacked.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(dc.Schemas) != 2 {
		t.Fatalf("schemas = %d, want 2", len(dc.Schemas))
	}
	a, b := dc.Schemas[0], dc.Schemas[1]
	if a.Name != "Sheet1#1" || b.Name != "Sheet1#2" {
		t.Errorf("locators = %q, %q, want Sheet1#1, Sheet1#2", a.Name, b.Name)
	}
	if a.Namespace != "Sheet1" || b.Namespace != "Sheet1" {
		t.Errorf("namespaces = %q, %q, want Sheet1", a.Namespace, b.Namespace)
	}
	// Types are independent per table: table A has a numeric Qty, table
	// B is all text.
	assertField(t, a.Fields[1], "Qty", "numeric")
	assertField(t, b.Fields[1], "Team", "text")
	if a.RowCount == nil || *a.RowCount != 2 || b.RowCount == nil || *b.RowCount != 3 {
		t.Errorf("row counts = %v, %v, want 2, 3", a.RowCount, b.RowCount)
	}
}

func TestAnalyzeTitledTable(t *testing.T) {
	dc, err := AnalyzeFile(context.Background(), "testdata/titled-table.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas = %d, want 1", len(dc.Schemas))
	}
	sc := dc.Schemas[0]
	if sc.Name != "Sheet1" {
		t.Errorf("single-table sheet locator = %q, want bare sheet name", sc.Name)
	}
	if sc.Metadata["caption"] != "Kontoutdrag 2024" {
		t.Errorf("caption = %v, want Kontoutdrag 2024", sc.Metadata["caption"])
	}
	assertField(t, sc.Fields[0], "Datum", "date")
	assertField(t, sc.Fields[2], "Belopp", "numeric")
	if sc.RowCount == nil || *sc.RowCount != 3 {
		t.Errorf("row_count = %v, want 3 (caption and header excluded)", sc.RowCount)
	}
}

func TestAnalyzeTwoTableObjects(t *testing.T) {
	dc, err := AnalyzeFile(context.Background(), "testdata/two-table-objects.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(dc.Schemas) != 3 {
		t.Fatalf("schemas = %d, want 3 (two declared + one heuristic)", len(dc.Schemas))
	}
	sales, targets, notes := dc.Schemas[0], dc.Schemas[1], dc.Schemas[2]
	if sales.Metadata["table_name"] != "Sales" || sales.Metadata["detection"] != DetectionTableObject {
		t.Errorf("sales metadata = %v", sales.Metadata)
	}
	if targets.Metadata["table_name"] != "Targets" {
		t.Errorf("targets metadata = %v", targets.Metadata)
	}
	if targets.Metadata["range"] != "E1:F4" {
		t.Errorf("targets range = %v, want E1:F4 (column bounds honored)", targets.Metadata["range"])
	}
	if notes.Metadata["detection"] != DetectionHeuristic {
		t.Errorf("notes detection = %v, want heuristic", notes.Metadata["detection"])
	}
	if sales.Name != "Sheet1#1" || targets.Name != "Sheet1#2" || notes.Name != "Sheet1#3" {
		t.Errorf("locators = %q, %q, %q", sales.Name, targets.Name, notes.Name)
	}
	issues, ok := dc.Metadata["sheet_issues"].(map[string]any)
	if !ok {
		t.Fatalf("sheet_issues missing: %v", dc.Metadata)
	}
	sheet1, _ := issues["Sheet1"].([]string)
	if len(sheet1) != 1 || !strings.Contains(sheet1[0], "stray cell at D12") {
		t.Errorf("sheet issues = %v, want the D12 stray", sheet1)
	}
}

func TestAnalyzeTablePlusNotes(t *testing.T) {
	dc, err := AnalyzeFile(context.Background(), "testdata/table-plus-notes.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas = %d, want 1 (the note is not a table)", len(dc.Schemas))
	}
	if dc.Schemas[0].Name != "Sheet1" {
		t.Errorf("locator = %q, want bare sheet name", dc.Schemas[0].Name)
	}
	issues, _ := dc.Metadata["sheet_issues"].(map[string]any)
	sheet1, _ := issues["Sheet1"].([]string)
	if len(sheet1) != 1 || !strings.Contains(sheet1[0], "stray cell at A5") {
		t.Errorf("sheet issues = %v, want the A5 stray", sheet1)
	}
}

func TestAnalyzeHeaderlessTable(t *testing.T) {
	dc, err := AnalyzeFile(context.Background(), "testdata/headerless-table.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	sc := dc.Schemas[0]
	if sc.Metadata["has_header"] != false {
		t.Errorf("has_header = %v, want false", sc.Metadata["has_header"])
	}
	if sc.Fields[0].Name != "column_1" {
		t.Errorf("field name = %q, want synthesized column_1", sc.Fields[0].Name)
	}
	if sc.RowCount == nil || *sc.RowCount != 3 {
		t.Errorf("row_count = %v, want 3 (first row is data)", sc.RowCount)
	}
}

func TestAnalyzeMergedCaption(t *testing.T) {
	dc, err := AnalyzeFile(context.Background(), "testdata/merged-caption.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas = %d, want 1", len(dc.Schemas))
	}
	sc := dc.Schemas[0]
	if sc.Metadata["caption"] != "Quarterly Report 2025" {
		t.Errorf("caption = %v, want the merged title absorbed", sc.Metadata["caption"])
	}
	if len(sc.Fields) != 4 || sc.Fields[0].Name != "Region" {
		t.Errorf("fields = %v", sc.Fields)
	}
}

// --- In-memory edge cases ---------------------------------------------------

func TestAnalyzeLocatorCollision(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	// Sheet "Data" holds two tables → locators Data#1, Data#2. A sibling
	// sheet literally named "Data#2" collides with the second locator.
	_ = f.SetSheetName("Sheet1", "Data")
	_ = f.SetSheetRow("Data", "A1", &[]any{"A", "B"})
	_ = f.SetSheetRow("Data", "A2", &[]any{"x", 1})
	_ = f.SetSheetRow("Data", "A4", &[]any{"C", "D"})
	_ = f.SetSheetRow("Data", "A5", &[]any{"y", 2})
	_, _ = f.NewSheet("Data#2")
	_ = f.SetSheetRow("Data#2", "A1", &[]any{"E", "F"})
	_ = f.SetSheetRow("Data#2", "A2", &[]any{"z", 3})

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	_, err = AnalyzeReader(context.Background(), bytes.NewReader(buf.Bytes()), nil)
	if err == nil || !strings.Contains(err.Error(), "locator collision") {
		t.Fatalf("err = %v, want the locator collision refusal", err)
	}
}

func TestAnalyzeHiddenSheetFlagged(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"A", "B"})
	_ = f.SetSheetRow("Sheet1", "A2", &[]any{"x", 1})
	_, _ = f.NewSheet("Hidden")
	_ = f.SetSheetRow("Hidden", "A1", &[]any{"C", "D"})
	_ = f.SetSheetRow("Hidden", "A2", &[]any{"y", 2})
	_ = f.SetSheetVisible("Hidden", false)

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	dc, err := AnalyzeReader(context.Background(), bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if len(dc.Schemas) != 2 {
		t.Fatalf("schemas = %d, want 2 (hidden sheets are analyzed, XL-L3)", len(dc.Schemas))
	}
	if dc.Schemas[0].Metadata["hidden"] != nil {
		t.Errorf("visible sheet flagged hidden: %v", dc.Schemas[0].Metadata)
	}
	if dc.Schemas[1].Metadata["hidden"] != true {
		t.Errorf("hidden sheet not flagged: %v", dc.Schemas[1].Metadata)
	}
}

func TestAnalyzeBlankHeaderCellSynthesized(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Name", "", "City"})
	_ = f.SetSheetRow("Sheet1", "A2", &[]any{"Alice", 30, "NYC"})
	_ = f.SetSheetRow("Sheet1", "A3", &[]any{"Bob", 25, "London"})

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	dc, err := AnalyzeReader(context.Background(), bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	sc := dc.Schemas[0]
	if sc.Fields[1].Name != "column_2" {
		t.Errorf("blank header name = %q, want synthesized column_2", sc.Fields[1].Name)
	}
	if len(sc.Issues) != 1 || !strings.Contains(sc.Issues[0], "header cell B1 is blank") {
		t.Errorf("issues = %v, want the blank-header synthesis recorded", sc.Issues)
	}
}

func TestAnalyzeHeaderOnlyTableAmongOthers(t *testing.T) {
	// A sheet with one real table and one header-only table: the real
	// table keeps its detection-order ordinal, the header-only one is
	// recorded as a sheet issue.
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Name", "Age"})
	_ = f.SetSheetRow("Sheet1", "A2", &[]any{"Alice", 30})
	_ = f.SetSheetRow("Sheet1", "A4", &[]any{"Orphan", "Header"})

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	dc, err := AnalyzeReader(context.Background(), bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas = %d, want 1", len(dc.Schemas))
	}
	if dc.Schemas[0].Name != "Sheet1#1" {
		t.Errorf("locator = %q, want Sheet1#1 (ordinals cover detected tables)", dc.Schemas[0].Name)
	}
	issues, _ := dc.Metadata["sheet_issues"].(map[string]any)
	sheet1, _ := issues["Sheet1"].([]string)
	if len(sheet1) != 1 || !strings.Contains(sheet1[0], "no data rows") {
		t.Errorf("sheet issues = %v, want the header-only drop recorded", sheet1)
	}
}
