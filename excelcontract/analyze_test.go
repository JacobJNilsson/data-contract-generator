package excelcontract

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/xuri/excelize/v2"
)

var ctx = context.Background()

func TestAnalyzeSimple(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/simple.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if dc.ContractType != "source" {
		t.Errorf("contract_type = %q, want source", dc.ContractType)
	}
	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas count = %d, want 1", len(dc.Schemas))
	}

	sc := dc.Schemas[0]
	if sc.Name != "People" {
		t.Errorf("schema name = %q, want People", sc.Name)
	}
	if sc.RowCount == nil || *sc.RowCount != 5 {
		t.Errorf("row_count = %v, want 5", sc.RowCount)
	}
	if len(sc.Fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(sc.Fields))
	}

	// Verify complete field definitions including profiling.
	assertField(t, sc.Fields[0], "Name", "text")
	assertProfile(t, "Name", sc.Fields[0].Profile, 5, 0, 0.0, 5)
	assertField(t, sc.Fields[1], "Age", "numeric")
	assertProfile(t, "Age", sc.Fields[1].Profile, 5, 0, 0.0, 5)
	assertField(t, sc.Fields[2], "City", "text")
	assertProfile(t, "City", sc.Fields[2].Profile, 5, 0, 0.0, 5)

	// All fields non-nullable.
	for _, f := range sc.Fields {
		if f.Nullable {
			t.Errorf("field %q should not be nullable", f.Name)
		}
	}

	if len(sc.SampleData) != 5 {
		t.Errorf("sample_data count = %d, want 5", len(sc.SampleData))
	}
	// Verify first sample row.
	if len(sc.SampleData) > 0 {
		first := sc.SampleData[0]
		if len(first) != 3 || first[0] != "Alice" || first[1] != "30" || first[2] != "New York" {
			t.Errorf("sample_data[0] = %v, want [Alice 30 New York]", first)
		}
	}
}

func TestAnalyzeMultiSheet(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/multi-sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(dc.Schemas) != 3 {
		t.Fatalf("schemas count = %d, want 3", len(dc.Schemas))
	}

	// Verify sheet names.
	names := make([]string, len(dc.Schemas))
	for i, s := range dc.Schemas {
		names[i] = s.Name
	}
	wantNames := []string{"Users", "Products", "Orders"}
	for i, want := range wantNames {
		if names[i] != want {
			t.Errorf("schema[%d].name = %q, want %q", i, names[i], want)
		}
	}

	// Users: 2 rows, 3 fields.
	if dc.Schemas[0].RowCount == nil || *dc.Schemas[0].RowCount != 2 {
		t.Errorf("Users row_count = %v, want 2", dc.Schemas[0].RowCount)
	}
	if len(dc.Schemas[0].Fields) != 3 {
		t.Errorf("Users fields = %d, want 3", len(dc.Schemas[0].Fields))
	}

	// Products: 3 rows, 4 fields.
	if dc.Schemas[1].RowCount == nil || *dc.Schemas[1].RowCount != 3 {
		t.Errorf("Products row_count = %v, want 3", dc.Schemas[1].RowCount)
	}
	if len(dc.Schemas[1].Fields) != 4 {
		t.Errorf("Products fields = %d, want 4", len(dc.Schemas[1].Fields))
	}
}

func TestAnalyzeNoHeader(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/no-header.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas count = %d, want 1", len(dc.Schemas))
	}

	sc := dc.Schemas[0]
	// All 3 rows are data (no header detected).
	if sc.RowCount == nil || *sc.RowCount != 3 {
		t.Errorf("row_count = %v, want 3", sc.RowCount)
	}

	// Field names should be generated.
	if sc.Fields[0].Name != "column_1" {
		t.Errorf("field[0].name = %q, want column_1", sc.Fields[0].Name)
	}
	if sc.Fields[1].Name != "column_2" {
		t.Errorf("field[1].name = %q, want column_2", sc.Fields[1].Name)
	}
	if sc.Fields[2].Name != "column_3" {
		t.Errorf("field[2].name = %q, want column_3", sc.Fields[2].Name)
	}
}

func TestAnalyzeExcelTable(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/excel-table.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas count = %d, want 1", len(dc.Schemas))
	}

	sc := dc.Schemas[0]
	if sc.RowCount == nil || *sc.RowCount != 3 {
		t.Errorf("row_count = %v, want 3", sc.RowCount)
	}

	assertField(t, sc.Fields[0], "Product", "text")
	assertField(t, sc.Fields[1], "Revenue", "numeric")
	assertField(t, sc.Fields[2], "Region", "text")
}

func TestAnalyzeEmpty(t *testing.T) {
	_, err := AnalyzeFile(ctx, "testdata/empty.xlsx", nil)
	if err == nil {
		t.Fatal("expected error for empty workbook")
	}
}

func TestAnalyzeTypes(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/types.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	sc := dc.Schemas[0]
	if len(sc.Fields) != 5 {
		t.Fatalf("fields count = %d, want 5", len(sc.Fields))
	}

	assertField(t, sc.Fields[0], "Text", "text")
	assertField(t, sc.Fields[1], "Integer", "numeric")
	assertField(t, sc.Fields[2], "Decimal", "numeric")
	assertField(t, sc.Fields[3], "Date", "date")
	// Booleans come through as text from GetRows ("TRUE"/"FALSE").
	assertField(t, sc.Fields[4], "Boolean", "text")
}

func TestAnalyzeSingleColumn(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/single-column.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	sc := dc.Schemas[0]
	if len(sc.Fields) != 1 {
		t.Fatalf("fields count = %d, want 1", len(sc.Fields))
	}
	assertField(t, sc.Fields[0], "Value", "numeric")
	if sc.RowCount == nil || *sc.RowCount != 1 {
		t.Errorf("row_count = %v, want 1", sc.RowCount)
	}
}

func TestAnalyzeReader(t *testing.T) {
	data, err := os.ReadFile("testdata/simple.xlsx")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	dc, err := AnalyzeReader(ctx, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if len(dc.Schemas) != 1 {
		t.Fatalf("schemas count = %d, want 1", len(dc.Schemas))
	}
	if dc.Schemas[0].Name != "People" {
		t.Errorf("schema name = %q, want People", dc.Schemas[0].Name)
	}
}

func TestAnalyzeFileNotFound(t *testing.T) {
	_, err := AnalyzeFile(ctx, "testdata/nonexistent.xlsx", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestAnalyzeCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := AnalyzeFile(ctx, "testdata/simple.xlsx", nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestAnalyzeReaderCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	data, err := os.ReadFile("testdata/simple.xlsx")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	_, err = AnalyzeReader(ctx, bytes.NewReader(data), nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestAnalyzeWithOptions(t *testing.T) {
	opts := &Options{TopN: 2, MaxSampleRows: 2, MaxTracked: 5}
	dc, err := AnalyzeFile(ctx, "testdata/simple.xlsx", opts)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	sc := dc.Schemas[0]
	if len(sc.SampleData) != 2 {
		t.Errorf("sample_data count = %d, want 2", len(sc.SampleData))
	}

	// TopN=2 should limit top values.
	for _, f := range sc.Fields {
		if f.Profile != nil && len(f.Profile.TopValues) > 2 {
			t.Errorf("field %q has %d top values, want <= 2", f.Name, len(f.Profile.TopValues))
		}
	}
}

func TestAnalyzeMetadata(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/multi-sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if dc.Metadata == nil {
		t.Fatal("metadata is nil")
	}
	if dc.Metadata["source_format"] != "xlsx" {
		t.Errorf("source_format = %v, want xlsx", dc.Metadata["source_format"])
	}
	if dc.Metadata["sheet_count"] != 3 {
		t.Errorf("sheet_count = %v, want 3", dc.Metadata["sheet_count"])
	}
}

func TestRequiredFields(t *testing.T) {
	dc, err := AnalyzeFile(ctx, "testdata/simple.xlsx", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	sc := dc.Schemas[0]
	// All fields should be required (no nulls in simple.xlsx).
	if len(sc.ValidationRules.RequiredFields) != 3 {
		t.Errorf("required_fields = %v, want 3 fields", sc.ValidationRules.RequiredFields)
	}
}

func TestOptionsDefaults(t *testing.T) {
	var nilOpts *Options
	if nilOpts.topN() != 5 {
		t.Errorf("nil topN() = %d, want 5", nilOpts.topN())
	}
	if nilOpts.maxTracked() != 10000 {
		t.Errorf("nil maxTracked() = %d, want 10000", nilOpts.maxTracked())
	}
	if nilOpts.maxSampleRows() != 5 {
		t.Errorf("nil maxSampleRows() = %d, want 5", nilOpts.maxSampleRows())
	}

	opts := &Options{TopN: 3, MaxTracked: 42, MaxSampleRows: 10}
	if opts.topN() != 3 {
		t.Errorf("topN() = %d, want 3", opts.topN())
	}
	if opts.maxTracked() != 42 {
		t.Errorf("maxTracked() = %d, want 42", opts.maxTracked())
	}
	if opts.maxSampleRows() != 10 {
		t.Errorf("maxSampleRows() = %d, want 10", opts.maxSampleRows())
	}
}

func TestAnalyzeReaderInvalidData(t *testing.T) {
	_, err := AnalyzeReader(ctx, bytes.NewReader([]byte("not an xlsx file")), nil)
	if err == nil {
		t.Fatal("expected error for invalid data")
	}
}

func TestAnalyzeContextCancelledDuringSheetIteration(t *testing.T) {
	// Create a context that cancels after the workbook is opened but
	// ideally during sheet iteration. Since we can't control timing
	// precisely, we test with an already-cancelled context.
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	data, err := os.ReadFile("testdata/multi-sheet.xlsx")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	_, err = AnalyzeReader(cancelCtx, bytes.NewReader(data), nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestDetectDataRegionWithExcelTable(t *testing.T) {
	// Test that detectDataRegion uses Excel Table objects when available.
	data, err := os.ReadFile("testdata/excel-table.xlsx")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	f, err := openTestFile(data)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer func() { _ = f.Close() }()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		t.Fatal("no sheets")
	}

	rows, err := f.GetRows(sheets[0])
	if err != nil {
		t.Fatalf("GetRows: %v", err)
	}

	region := detectDataRegion(f, sheets[0], rows)
	if region == nil {
		t.Fatal("expected non-nil region")
	}
	// Excel Table starts at A1, so header=0, start=1.
	if region.headerRow != 0 || region.startRow != 1 {
		t.Errorf("region = %+v, want {0, 1}", region)
	}
}

func openTestFile(data []byte) (*excelize.File, error) {
	return excelize.OpenReader(bytes.NewReader(data))
}

func TestAnalyzeHeaderOnlySheet(t *testing.T) {
	// Create a workbook with only a header row (no data rows).
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Name", "Age"})

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}

	_, err = AnalyzeReader(ctx, bytes.NewReader(buf.Bytes()), nil)
	if err == nil {
		t.Fatal("expected error for header-only sheet")
	}
}

func TestAnalyzeAllEmptySheets(t *testing.T) {
	// Create a workbook with only empty sheets.
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	// Sheet1 is empty by default. Add another empty sheet.
	_, _ = f.NewSheet("Empty2")

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}

	_, err = AnalyzeReader(ctx, bytes.NewReader(buf.Bytes()), nil)
	if err == nil {
		t.Fatal("expected error for all-empty-sheets workbook")
	}
}

func TestAnalyzeSheetWithEmptyDataRows(t *testing.T) {
	// Header + empty data rows → totalRows == 0 → nil schema.
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	_ = f.SetSheetRow("Sheet1", "A1", &[]any{"Name", "Age"})
	_ = f.SetSheetRow("Sheet1", "A2", &[]any{"", ""})
	_ = f.SetSheetRow("Sheet1", "A3", &[]any{"", ""})

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}

	_, err = AnalyzeReader(ctx, bytes.NewReader(buf.Bytes()), nil)
	if err == nil {
		t.Fatal("expected error for sheet with only empty data rows")
	}
}

// --- helpers ----------------------------------------------------------------

func assertField(t *testing.T, f contract.FieldDefinition, wantName, wantType string) {
	t.Helper()
	if f.Name != wantName {
		t.Errorf("field name = %q, want %q", f.Name, wantName)
	}
	if f.DataType != wantType {
		t.Errorf("field %q type = %q, want %q", f.Name, f.DataType, wantType)
	}
}

func assertProfile(t *testing.T, fieldName string, p *contract.FieldProfile, wantSampleSize, wantNullCount int, wantNullPct float64, wantDistinct int) {
	t.Helper()
	if p == nil {
		t.Errorf("field %q: profile is nil", fieldName)
		return
	}
	if p.SampleSize != wantSampleSize {
		t.Errorf("field %q: sample_size = %d, want %d", fieldName, p.SampleSize, wantSampleSize)
	}
	if p.NullCount != wantNullCount {
		t.Errorf("field %q: null_count = %d, want %d", fieldName, p.NullCount, wantNullCount)
	}
	if p.NullPercentage != wantNullPct {
		t.Errorf("field %q: null_percentage = %.2f, want %.2f", fieldName, p.NullPercentage, wantNullPct)
	}
	if p.DistinctCount != wantDistinct {
		t.Errorf("field %q: distinct_count = %d, want %d", fieldName, p.DistinctCount, wantDistinct)
	}
}
