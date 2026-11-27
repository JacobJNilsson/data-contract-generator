package excelcontract

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/xuri/excelize/v2"
)

// shape builds a signature in which every value has one token.
func shape(token string, count, lengthMin, lengthMax int) contract.ShapeSignature {
	return contract.ShapeSignature{
		Masks:         []contract.MaskCount{{Mask: token, Count: count}},
		DominantShare: 1,
		LengthMin:     lengthMin,
		LengthMax:     lengthMax,
	}
}

// TestAnalyzeTypedCellShapes: the shape reads each cell's type and raw
// value, not its display text. Two number formats give one number token,
// digit strings stay text, dates under any date format give "date", a
// quoted m in a custom format is no date, and a formula without a cached
// value is null.
func TestAnalyzeTypedCellShapes(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/typed-cells.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	got := fieldShapes(t, dc.Schemas[0])
	want := []contract.ShapeSignature{
		shape("number 9.9", 3, 5, 7), // Amount
		shape("text 9", 3, 1, 4),     // Code
		shape("date", 3, 5, 5),       // Booked
		shape("date", 3, 5, 5),       // Custom
		shape("number 9", 3, 2, 2),   // Length
		shape("number 9", 1, 1, 1),   // Formula
		shape("bool", 3, 1, 1),       // Flag
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("shapes = %+v\nwant %+v", got, want)
	}
}

// workbookWithSheet returns a workbook whose first sheet holds the given
// sheetData XML. It lets a test write cell types that excelize has no
// setter for, such as an error cell or a cached string formula result.
func workbookWithSheet(t *testing.T, sheetData string) []byte {
	t.Helper()
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	for _, entry := range zr.File {
		w, err := zw.Create(entry.Name)
		if err != nil {
			t.Fatalf("Create(%s): %v", entry.Name, err)
		}
		if entry.Name == "xl/worksheets/sheet1.xml" {
			_, err = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`+
				`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`+
				sheetData+`</sheetData></worksheet>`)
		} else {
			err = copyEntry(w, entry)
		}
		if err != nil {
			t.Fatalf("write %s: %v", entry.Name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip Close: %v", err)
	}
	return out.Bytes()
}

// copyEntry copies one zip entry's content into w.
func copyEntry(w io.Writer, entry *zip.File) error {
	r, err := entry.Open()
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	_, err = io.Copy(w, r)
	return err
}

// TestAnalyzeCellTypesWithoutSetters covers the cell types that only a
// hand-written sheet holds: an error cell, a formula with a cached string
// result (t=str), a number with an explicit t=n, and an ISO date (t=d).
func TestAnalyzeCellTypesWithoutSetters(t *testing.T) {
	row := func(n int, err, str, num, day string) string {
		return fmt.Sprintf(`<row r="%d">`+
			`<c r="A%d" t="e"><v>%s</v></c>`+
			`<c r="B%d" t="str"><f>LOWER("%s")</f><v>%s</v></c>`+
			`<c r="C%d" t="n"><v>%s</v></c>`+
			`<c r="D%d" t="d"><v>%s</v></c>`+
			`</row>`, n, n, err, n, str, str, n, num, n, day)
	}
	data := workbookWithSheet(t,
		`<row r="1">`+
			`<c r="A1" t="inlineStr"><is><t>Err</t></is></c>`+
			`<c r="B1" t="inlineStr"><is><t>Str</t></is></c>`+
			`<c r="C1" t="inlineStr"><is><t>Num</t></is></c>`+
			`<c r="D1" t="inlineStr"><is><t>Day</t></is></c>`+
			`</row>`+
			row(2, "#DIV/0!", "abc", "12", "2024-01-02T00:00:00Z")+
			row(3, "#N/A", "de", "345", "2024-01-05T00:00:00Z"))
	dc, err := AnalyzeReader(ctx, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	got := fieldShapes(t, dc.Schemas[0])
	want := []contract.ShapeSignature{
		shape("error", 2, 4, 7),
		shape("text A", 2, 2, 3),
		shape("number 9", 2, 2, 3),
		shape("date", 2, 20, 20),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("shapes = %+v\nwant %+v", got, want)
	}
}

func TestCellToken(t *testing.T) {
	cases := []struct {
		kind cellKind
		raw  string
		want string
	}{
		{kindNumber, "1500.5", "number 9.9"},
		{kindNumber, "1.5E-3", "number 9.9A-9"},
		{kindText, "A-9", "text A-9"},
		{kindDate, "45293", "date"},
		{kindBool, "1", "bool"},
		{kindError, "#N/A", "error"},
	}
	for _, c := range cases {
		got := cellToken(c.kind, c.raw)
		if got != c.want {
			t.Errorf("cellToken(%q, %q) = %q, want %q", c.kind, c.raw, got, c.want)
		}
	}
}

func TestIsDateFormatCode(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"d mmm yyyy", true},
		{"yyyy-mm-dd", true},
		{"[h]:mm", true},
		{"hh:mm:ss", true},
		{"[$-409]d/m/yy", true},
		{`0" mm"`, false},
		{`0\m`, false},
		{"0.00", false},
		{"#,##0.00", false},
		{"[Red]0.00", false},
		{"[$-409]0.00", false},
		{`#,##0.00 "kr"`, false},
		{"General", false},
		{"@", false},
	}
	for _, c := range cases {
		got := isDateFormatCode(c.code)
		if got != c.want {
			t.Errorf("isDateFormatCode(%q) = %v, want %v", c.code, got, c.want)
		}
	}
}

func TestIsDateFormatID(t *testing.T) {
	dates := []int{14, 18, 22, 27, 30, 36, 45, 47, 50, 57, 62, 67, 75, 81}
	numbers := []int{0, 1, 2, 4, 9, 13, 23, 26, 37, 44, 48, 49, 63, 66, 82, 164}
	for _, id := range dates {
		if got := isDateFormatID(id); !got {
			t.Errorf("isDateFormatID(%d) = false, want true", id)
		}
	}
	for _, id := range numbers {
		if got := isDateFormatID(id); got {
			t.Errorf("isDateFormatID(%d) = true, want false", id)
		}
	}
}

// TestIsDateFormatCustomCodeFirst: a custom code decides before the
// built-in ID, which GetStyle also sets for a currency-like custom code.
func TestIsDateFormatCustomCodeFirst(t *testing.T) {
	number, date := "0.00", "yyyy"
	cases := []struct {
		name  string
		style excelize.Style
		want  bool
	}{
		{"custom number code over a date ID", excelize.Style{NumFmt: 14, CustomNumFmt: &number}, false},
		{"custom date code over a number ID", excelize.Style{NumFmt: 4, CustomNumFmt: &date}, true},
		{"built-in date ID", excelize.Style{NumFmt: 14}, true},
		{"built-in number ID", excelize.Style{NumFmt: 4}, false},
	}
	for _, c := range cases {
		got := isDateFormat(&c.style)
		if got != c.want {
			t.Errorf("%s: isDateFormat = %v, want %v", c.name, got, c.want)
		}
	}
}

// BenchmarkAnalyzeFile100kRows records the cost of the typed pass on a
// sheet of 100000 rows: text, a formatted number, and a date per row.
func BenchmarkAnalyzeFile100kRows(b *testing.B) {
	path := filepath.Join(b.TempDir(), "large.xlsx")
	writeLargeWorkbook(b, path, 100000)
	b.ResetTimer()
	for b.Loop() {
		if _, err := AnalyzeFile(ctx, path, nil); err != nil {
			b.Fatalf("AnalyzeFile: %v", err)
		}
	}
}

// writeLargeWorkbook writes a one-table workbook of the given data rows
// with the stream writer.
func writeLargeWorkbook(b *testing.B, path string, rows int) {
	b.Helper()
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	amount, err := f.NewStyle(&excelize.Style{NumFmt: 4})
	if err != nil {
		b.Fatal(err)
	}
	day, err := f.NewStyle(&excelize.Style{NumFmt: 14})
	if err != nil {
		b.Fatal(err)
	}
	sw, err := f.NewStreamWriter("Sheet1")
	if err != nil {
		b.Fatal(err)
	}
	if err := sw.SetRow("A1", []any{"Text", "Amount", "Booked"}); err != nil {
		b.Fatal(err)
	}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for r := 0; r < rows; r++ {
		ref, _ := excelize.CoordinatesToCellName(1, r+2)
		err := sw.SetRow(ref, []any{
			fmt.Sprintf("P-%d", r),
			excelize.Cell{StyleID: amount, Value: float64(r) + 0.25},
			excelize.Cell{StyleID: day, Value: start.AddDate(0, 0, r%365)},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	if err := sw.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := f.SaveAs(path); err != nil {
		b.Fatal(err)
	}
}
