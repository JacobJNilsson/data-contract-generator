package excelcontract

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/profile"
	"github.com/xuri/excelize/v2"
)

// openFixture opens a test workbook and closes it when the test ends.
func openFixture(t *testing.T, path string) *excelize.File {
	t.Helper()
	f, err := excelize.OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// fieldShapes returns each field's shape of one schema, in field order.
func fieldShapes(t *testing.T, sc contract.SchemaContract) []contract.ShapeSignature {
	t.Helper()
	shapes := make([]contract.ShapeSignature, len(sc.Fields))
	for i, field := range sc.Fields {
		if field.Profile == nil || field.Profile.Shape == nil {
			t.Fatalf("field %q has no shape", field.Name)
		}
		shapes[i] = *field.Profile.Shape
	}
	return shapes
}

func TestAnalyzeHeaderlessTableShapes(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/headerless-table.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	got := fieldShapes(t, dc.Schemas[0])
	want := []contract.ShapeSignature{
		{Masks: []contract.MaskCount{{Mask: "text A-9", Count: 3}}, DominantShare: 1, LengthMin: 5, LengthMax: 5},
		{Masks: []contract.MaskCount{{Mask: "number 9.9", Count: 2}, {Mask: "number 9", Count: 1}}, DominantShare: 0.67, LengthMin: 3, LengthMax: 5},
		{Masks: []contract.MaskCount{{Mask: "text 9-9-9", Count: 3}}, DominantShare: 1, LengthMin: 10, LengthMax: 10},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("shapes = %+v, want %+v", got, want)
	}
}

// TestShapeCountsAtMostMaxShapeRows: the shape counts the first
// MaxShapeRows data rows and leaves the rest of a long table out.
func TestShapeCountsAtMostMaxShapeRows(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Name", "Qty"})
	for r := 2; r <= MaxShapeRows+6; r++ {
		_ = f.SetSheetRow("Sheet1", fmt.Sprintf("A%d", r), &[]any{"row", r})
	}
	path := filepath.Join(t.TempDir(), "long.xlsx")
	if err := f.SaveAs(path); err != nil {
		t.Fatalf("SaveAs: %v", err)
	}
	dc, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	got := *dc.Schemas[0].Fields[0].Profile.Shape
	want := contract.ShapeSignature{
		Masks:         []contract.MaskCount{{Mask: "text A", Count: MaxShapeRows}},
		DominantShare: 1,
		LengthMin:     3,
		LengthMax:     3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("shape = %+v, want %+v", got, want)
	}
}

func TestLocateTable(t *testing.T) {
	stacked := openFixture(t, "testdata/multi-table-stacked.xlsx")
	sheet, seg, err := LocateTable(stacked, "Sheet1#2")
	if err != nil {
		t.Fatalf("LocateTable(Sheet1#2): %v", err)
	}
	got := struct {
		Sheet string
		Seg   Segment
	}{sheet, seg}
	want := struct {
		Sheet string
		Seg   Segment
	}{"Sheet1", Segment{Detection: DetectionHeuristic, HasHeader: true, StartRow: 4, EndRow: 7, StartCol: 0, EndCol: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LocateTable(Sheet1#2) = %+v, want %+v", got, want)
	}

	multi := openFixture(t, "testdata/multi-sheet.xlsx")
	sheet, seg, err = LocateTable(multi, "Products")
	if err != nil {
		t.Fatalf("LocateTable(Products): %v", err)
	}
	got = struct {
		Sheet string
		Seg   Segment
	}{sheet, seg}
	want = struct {
		Sheet string
		Seg   Segment
	}{"Products", Segment{Detection: DetectionHeuristic, HasHeader: true, StartRow: 0, EndRow: 3, StartCol: 0, EndCol: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LocateTable(Products) = %+v, want %+v", got, want)
	}
}

func TestLocateTableRefusesUnknownLocator(t *testing.T) {
	stacked := openFixture(t, "testdata/multi-table-stacked.xlsx")
	multi := openFixture(t, "testdata/multi-sheet.xlsx")
	cases := []struct {
		name    string
		f       *excelize.File
		locator string
	}{
		{"unknown sheet", multi, "Invoices"},
		{"ordinal past the last table", stacked, "Sheet1#9"},
		{"ordinal zero", stacked, "Sheet1#0"},
		{"ordinal that is no number", stacked, "Sheet1#two"},
		{"bare name of a sheet with several tables", stacked, "Sheet1"},
		{"ordinal on a sheet with one table", multi, "Products#1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := LocateTable(c.f, c.locator)
			if !errors.Is(err, ErrNoSuchTable) {
				t.Errorf("LocateTable(%q) error = %v, want ErrNoSuchTable", c.locator, err)
			}
		})
	}
}

func TestColumnCells(t *testing.T) {
	f := openFixture(t, "testdata/headerless-table.xlsx")
	sheet, seg, err := LocateTable(f, "Sheet1")
	if err != nil {
		t.Fatalf("LocateTable: %v", err)
	}
	got, err := ColumnCells(f, sheet, seg, 0, 2)
	if err != nil {
		t.Fatalf("ColumnCells: %v", err)
	}
	want := []Cell{
		{Ref: "A1", Value: "P-600", Raw: "P-600", Token: "text A-9"},
		{Ref: "A2", Value: "P-601", Raw: "P-601", Token: "text A-9"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ColumnCells = %+v, want %+v", got, want)
	}
}

func TestColumnCellsLeavesOutNulls(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Name", "Note"})
	_ = f.SetSheetRow("Sheet1", "A2", &[]any{"a", "x"})
	_ = f.SetSheetRow("Sheet1", "A3", &[]any{"b", ""})
	_ = f.SetSheetRow("Sheet1", "A4", &[]any{"c", "z"})
	sheet, seg, err := LocateTable(f, "Sheet1")
	if err != nil {
		t.Fatalf("LocateTable: %v", err)
	}
	got, err := ColumnCells(f, sheet, seg, 1, MaxShapeRows)
	if err != nil {
		t.Fatalf("ColumnCells: %v", err)
	}
	want := []Cell{
		{Ref: "B2", Value: "x", Raw: "x", Token: "text A"},
		{Ref: "B4", Value: "z", Raw: "z", Token: "text A"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ColumnCells = %+v, want %+v", got, want)
	}
}

func TestColumnCellsRefusesBadInput(t *testing.T) {
	f := openFixture(t, "testdata/headerless-table.xlsx")
	sheet, seg, err := LocateTable(f, "Sheet1")
	if err != nil {
		t.Fatalf("LocateTable: %v", err)
	}
	if _, err := ColumnCells(f, sheet, seg, 3, 1); err == nil {
		t.Error("ColumnCells(column 3 of 3) succeeded, want an error")
	}
	if _, err := ColumnCells(f, sheet, seg, -1, 1); err == nil {
		t.Error("ColumnCells(column -1) succeeded, want an error")
	}
	if _, err := ColumnCells(f, "NoSuchSheet", seg, 0, 1); err == nil {
		t.Error("ColumnCells(unknown sheet) succeeded, want an error")
	}
}

// TestColumnCellsGiveTheFieldShape pins the sample tokens to the shape:
// for every fixture and every column, the tokens from ColumnCells, fed
// into a profile.ShapeTracker, give the field's shape from AnalyzeFile.
func TestColumnCellsGiveTheFieldShape(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.xlsx")
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob found %d fixtures: %v", len(paths), err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			dc, err := AnalyzeFile(ctx, path, nil)
			if err != nil {
				// A fixture without a table, such as empty.xlsx, has no shape to pin.
				t.Skipf("AnalyzeFile: %v", err)
			}
			f := openFixture(t, path)
			for _, sc := range dc.Schemas {
				sheet, seg, err := LocateTable(f, sc.Name)
				if err != nil {
					t.Fatalf("LocateTable(%q): %v", sc.Name, err)
				}
				for col, field := range sc.Fields {
					cells, err := ColumnCells(f, sheet, seg, col, MaxShapeRows)
					if err != nil {
						t.Fatalf("ColumnCells(%q, %d): %v", sc.Name, col, err)
					}
					var tracker profile.ShapeTracker
					for _, c := range cells {
						tracker.ObserveMask(c.Token, len([]rune(c.Raw)))
					}
					got := tracker.Signature().Contract()
					want := *field.Profile.Shape
					if !reflect.DeepEqual(got, want) {
						t.Errorf("%s field %q: shape from ColumnCells = %+v, want %+v", sc.Name, field.Name, got, want)
					}
				}
			}
		})
	}
}
