package excelcontract

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/xuri/excelize/v2"
)

// tableAndBlock is a two-column table, a blank row, and a one-column
// block below it in column A.
func tableAndBlock(block ...string) [][]string {
	rows := [][]string{{"Item", "Count"}, {"Bolt", "120"}, {"Nut", "118"}, {}}
	for _, v := range block {
		rows = append(rows, []string{v})
	}
	return rows
}

// itemTable is the segment of the table in tableAndBlock.
var itemTable = Segment{Detection: DetectionHeuristic, HasHeader: true, StartRow: 0, EndRow: 2, StartCol: 0, EndCol: 1}

// TestSegmentTextBlockIsAnIssue: a one-column block of text beside a
// wider table is a sheet issue, and the issue quotes no cell text.
func TestSegmentTextBlockIsAnIssue(t *testing.T) {
	got := SegmentSheet(tableAndBlock("Source: spring audit", "Base year 2020 = 100", "Counts are rounded"), nil)
	want := Segmentation{
		Tables: []Segment{itemTable},
		Issues: []string{"text block at A5:A7 is one column of text beside a wider table; it was not read as a table"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SegmentSheet = %+v, want %+v", got, want)
	}
}

// TestSegmentOneColumnTextTableAlone: a one-column text table on a sheet
// without a wider table stays a table.
func TestSegmentOneColumnTextTableAlone(t *testing.T) {
	got := SegmentSheet([][]string{{"Name"}, {"Alice"}, {"Bob"}}, nil)
	want := Segmentation{Tables: []Segment{{Detection: DetectionHeuristic, HasHeader: true, StartRow: 0, EndRow: 2, StartCol: 0, EndCol: 0}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SegmentSheet = %+v, want %+v", got, want)
	}
}

// TestSegmentOneColumnOfNumbersStays: a one-column block of numbers
// beside a wider table stays a table.
func TestSegmentOneColumnOfNumbersStays(t *testing.T) {
	got := SegmentSheet(tableAndBlock("Total", "238", "240"), nil)
	want := Segmentation{Tables: []Segment{
		itemTable,
		{Detection: DetectionHeuristic, HasHeader: true, StartRow: 4, EndRow: 6, StartCol: 0, EndCol: 0},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SegmentSheet = %+v, want %+v", got, want)
	}
}

// TestAnalyzeTextBlockKeepsTheBareLocator: once the text block is an
// issue, the sheet holds one table, so its locator is the bare sheet
// name.
func TestAnalyzeTextBlockKeepsTheBareLocator(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Item", "Count"})
	_ = f.SetSheetRow("Sheet1", "A2", &[]any{"Bolt", 120})
	_ = f.SetSheetRow("Sheet1", "A3", &[]any{"Nut", 118})
	_ = f.SetCellValue("Sheet1", "A5", "Source: spring audit")
	_ = f.SetCellValue("Sheet1", "A6", "Counts are rounded")
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	dc, err := AnalyzeReader(ctx, bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	got := struct {
		Names  []string
		Issues any
	}{nil, dc.Metadata["sheet_issues"]}
	for _, sc := range dc.Schemas {
		got.Names = append(got.Names, sc.Name)
	}
	want := struct {
		Names  []string
		Issues any
	}{
		[]string{"Sheet1"},
		map[string]any{"Sheet1": []string{"text block at A5:A6 is one column of text beside a wider table; it was not read as a table"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
