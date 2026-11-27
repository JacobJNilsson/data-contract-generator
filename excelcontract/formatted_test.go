package excelcontract

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/xuri/excelize/v2"
)

// fieldTypes returns each field's name and type of one schema.
func fieldTypes(sc contract.SchemaContract) [][2]string {
	out := make([][2]string, len(sc.Fields))
	for i, f := range sc.Fields {
		out[i] = [2]string{f.Name, f.DataType}
	}
	return out
}

// analyzeWorkbook writes a workbook to a buffer and analyzes it.
func analyzeWorkbook(t *testing.T, f *excelize.File) *contract.DataContract {
	t.Helper()
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	dc, err := AnalyzeReader(ctx, bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	return dc
}

// TestAnalyzeFormattedCellTypes: a column's type comes from the stored
// values, so a currency, thousands, or rounding format keeps a number
// column numeric, and a date format makes a serial column a date.
func TestAnalyzeFormattedCellTypes(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/formatted-cells.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	got := fieldTypes(dc.Schemas[0])
	want := [][2]string{
		{"Lodging", "numeric"},
		{"Price", "numeric"},
		{"Area", "numeric"},
		{"Month", "date"},
		{"Day", "date"},
		{"Done", "boolean"},
		{"Note", "text"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}
}

// TestAnalyzeFormattedCellSamples: the samples hold the typed text of
// each cell, not its display text.
func TestAnalyzeFormattedCellSamples(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/formatted-cells.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	got := dc.Schemas[0].SampleData[0]
	want := []string{"126", "90347", "15.83", "1995-01-01", "2024-01-02", "true", "first"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("first sample row = %q, want %q", got, want)
	}
}

// TestAnalyzeFormattedCellRange: the profile reads the stored fraction,
// not the rounded display text.
func TestAnalyzeFormattedCellRange(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/formatted-cells.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	area := dc.Schemas[0].Fields[2].Profile
	if area.MinValue == nil || area.MaxValue == nil {
		t.Fatalf("Area range = %v, %v, want 15.83 and 146.77", area.MinValue, area.MaxValue)
	}
	got := [2]string{*area.MinValue, *area.MaxValue}
	want := [2]string{"15.83", "146.77"}
	if got != want {
		t.Errorf("Area range = %v, want %v", got, want)
	}
}

// TestAnalyze1904DateSystem: a workbook in the 1904 date system counts
// its serials from 1904-01-01.
func TestAnalyze1904DateSystem(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	date1904 := true
	if err := f.SetWorkbookProps(&excelize.WorkbookPropsOptions{Date1904: &date1904}); err != nil {
		t.Fatalf("SetWorkbookProps: %v", err)
	}
	day, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	if err != nil {
		t.Fatalf("NewStyle: %v", err)
	}
	_ = f.SetCellValue("Sheet1", "A1", "Day")
	_ = f.SetCellValue("Sheet1", "A2", 0)
	_ = f.SetCellValue("Sheet1", "A3", 1)
	if err := f.SetCellStyle("Sheet1", "A2", "A3", day); err != nil {
		t.Fatalf("SetCellStyle: %v", err)
	}
	got := analyzeWorkbook(t, f).Schemas[0].SampleData
	want := [][]string{{"1904-01-01"}, {"1904-01-02"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("samples = %q, want %q", got, want)
	}
}

// TestAnalyzeHeaderOverCurrencyColumn: a header over a column of
// currency-formatted numbers is a header, because the numbers read as
// numbers.
func TestAnalyzeHeaderOverCurrencyColumn(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	code := `"$"\ #,##0`
	dollars, err := f.NewStyle(&excelize.Style{CustomNumFmt: &code})
	if err != nil {
		t.Fatalf("NewStyle: %v", err)
	}
	_ = f.SetCellValue("Sheet1", "A1", "Cost")
	for i, v := range []int{126, 110, 98} {
		ref, _ := excelize.CoordinatesToCellName(1, i+2)
		_ = f.SetCellValue("Sheet1", ref, v)
		_ = f.SetCellStyle("Sheet1", ref, ref, dollars)
	}
	sc := analyzeWorkbook(t, f).Schemas[0]
	got := struct {
		HasHeader any
		Fields    [][2]string
	}{sc.Metadata["has_header"], fieldTypes(sc)}
	want := struct {
		HasHeader any
		Fields    [][2]string
	}{true, [][2]string{{"Cost", "numeric"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestLocateFormattedTable: LocateTable finds the bounds that AnalyzeFile
// reports, and ColumnCells keeps each cell's display text in Value.
func TestLocateFormattedTable(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/formatted-cells.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	f := openFixture(t, "testdata/formatted-cells.xlsx")
	sheet, seg, err := LocateTable(f, "Sheet1")
	if err != nil {
		t.Fatalf("LocateTable: %v", err)
	}
	if got, want := rangeRef(seg.StartRow, seg.StartCol, seg.EndRow, seg.EndCol), dc.Schemas[0].Metadata["range"]; got != want {
		t.Errorf("LocateTable range = %s, AnalyzeFile range = %v", got, want)
	}
	got, err := ColumnCells(f, sheet, seg, 0, 1)
	if err != nil {
		t.Fatalf("ColumnCells: %v", err)
	}
	want := []Cell{{Ref: "A2", Value: "$ 126", Raw: "126", Token: "number 9"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ColumnCells = %+v, want %+v", got, want)
	}
}

func TestTypedValue(t *testing.T) {
	cases := []struct {
		name    string
		kind    cellKind
		raw     string
		display string
		want    string
	}{
		{"float noise", kindNumber, "9.3000000000000007", "9.30", "9.3"},
		{"exponent", kindNumber, "1.5E-3", "0.00", "0.0015"},
		{"negative", kindNumber, "-467351136.73000002", "-467,351,137", "-467351136.73"},
		{"unreadable number", kindNumber, "12abc", "12abc ", "12abc"},
		{"date serial", kindDate, "34700", "Jan-95", "1995-01-01"},
		{"serial with a fraction", kindDate, "45293.75", "1/2/24 18:00", "2024-01-02 18:00:00"},
		{"ISO date of type d", kindDate, "2024-01-02T00:00:00Z", "01-02-24", "2024-01-02T00:00:00Z"},
		{"negative serial", kindDate, "-5", "#####", "#####"},
		{"true", kindBool, "1", "TRUE", "true"},
		{"false", kindBool, "0", "FALSE", "false"},
		{"text", kindText, "raw", "shown", "shown"},
		{"error", kindError, "#N/A", "#N/A ", "#N/A "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := typedValue(c.kind, c.raw, c.display, false); got != c.want {
				t.Errorf("typedValue(%s, %q, %q) = %q, want %q", c.kind, c.raw, c.display, got, c.want)
			}
		})
	}
}

// TestSheetRows: SheetRows gives the typed grid that the analysis
// segments.
func TestSheetRows(t *testing.T) {
	f := openFixture(t, "testdata/formatted-cells.xlsx")
	rows, err := SheetRows(f, "Sheet1")
	if err != nil {
		t.Fatalf("SheetRows: %v", err)
	}
	got := rows[:2]
	want := [][]string{
		{"Lodging", "Price", "Area", "Month", "Day", "Done", "Note"},
		{"126", "90347", "15.83", "1995-01-01", "2024-01-02", "true", "first"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SheetRows = %q, want %q", got, want)
	}
	if _, err := SheetRows(f, "NoSuchSheet"); err == nil {
		t.Error("SheetRows(unknown sheet) succeeded, want an error")
	}
}
