package csvcontract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/profile"
)

var ctx = context.Background()

func ptr(s string) *string { return &s }

func tv(value string, count int) profile.TopValue {
	return profile.TopValue{Value: value, Count: count}
}

func TestAnalyzeSimpleCSV(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/simple.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	assertContract(t, contract, SourceContract{
		SourceFormat: "csv",
		Encoding:     "utf-8",
		Delimiter:    ",",
		HasHeader:    true,
		TotalRows:    5,
		Fields: []Field{
			{Name: "Name", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("Alice"), MaxValue: ptr("Eve"),
				TopValues: []profile.TopValue{tv("Alice", 1), tv("Bob", 1), tv("Charlie", 1), tv("Diana", 1), tv("Eve", 1)},
			}},
			{Name: "Age", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("25"), MaxValue: ptr("35"),
				TopValues: []profile.TopValue{tv("25", 1), tv("28", 1), tv("30", 1), tv("32", 1), tv("35", 1)},
			}},
			{Name: "City", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("Berlin"), MaxValue: ptr("Tokyo"),
				TopValues: []profile.TopValue{tv("Berlin", 1), tv("London", 1), tv("New York", 1), tv("Paris", 1), tv("Tokyo", 1)},
			}},
			{Name: "Score", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 1, NullPercentage: 20,
				MinValue: ptr("87.3"), MaxValue: ptr("95.5"),
				TopValues: []profile.TopValue{tv("87.3", 1), tv("88.9", 1), tv("92.1", 1), tv("95.5", 1)},
			}},
		},
		SampleData: [][]string{
			{"Alice", "30", "New York", "95.5"},
			{"Bob", "25", "London", "87.3"},
			{"Charlie", "35", "Berlin", "92.1"},
			{"Diana", "28", "Paris", ""},
			{"Eve", "32", "Tokyo", "88.9"},
		},
		Issues: nil,
	})
}

func TestAnalyzeEuropeanCSV(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/european.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	assertContract(t, contract, SourceContract{
		SourceFormat: "csv",
		Encoding:     "utf-8",
		Delimiter:    ";",
		HasHeader:    true,
		TotalRows:    5,
		Fields: []Field{
			{Name: "Date", DataType: profile.TypeDate, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("2024-01-15"), MaxValue: ptr("2024-01-19"),
				TopValues: []profile.TopValue{tv("2024-01-15", 1), tv("2024-01-16", 1), tv("2024-01-17", 1), tv("2024-01-18", 1), tv("2024-01-19", 1)},
			}},
			{Name: "Account", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("Depot"), MaxValue: ptr("Savings"),
				TopValues: []profile.TopValue{tv("Depot", 3), tv("Savings", 2)},
			}},
			{Name: "Amount", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 2, NullPercentage: 40,
				MinValue: ptr("910,11"), MaxValue: ptr("5678,90"),
				TopValues: []profile.TopValue{tv("1234,56", 1), tv("5678,90", 1), tv("910,11", 1)},
			}},
			{Name: "Currency", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("SEK"), MaxValue: ptr("SEK"),
				TopValues: []profile.TopValue{tv("SEK", 5)},
			}},
			{Name: "Quantity", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 2, NullPercentage: 40,
				MinValue: ptr("50"), MaxValue: ptr("200"),
				TopValues: []profile.TopValue{tv("100", 1), tv("200", 1), tv("50", 1)},
			}},
		},
		SampleData: [][]string{
			{"2024-01-15", "Depot", "1234,56", "SEK", "100"},
			{"2024-01-16", "Savings", "", "SEK", ""},
			{"2024-01-17", "Depot", "5678,90", "SEK", "200"},
			{"2024-01-18", "Savings", "910,11", "SEK", "50"},
			{"2024-01-19", "Depot", "", "SEK", ""},
		},
		Issues: []string{"UTF-8 BOM detected and stripped"},
	})
}

func TestAnalyzeNoHeader(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/no_header.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	assertContract(t, contract, SourceContract{
		SourceFormat: "csv",
		Encoding:     "utf-8",
		Delimiter:    ",",
		HasHeader:    false,
		TotalRows:    3,
		Fields: []Field{
			{Name: "column_1", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 3, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("1"), MaxValue: ptr("3"),
				TopValues: []profile.TopValue{tv("1", 1), tv("2", 1), tv("3", 1)},
			}},
			{Name: "column_2", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 3, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("100"), MaxValue: ptr("300"),
				TopValues: []profile.TopValue{tv("100", 1), tv("200", 1), tv("300", 1)},
			}},
			{Name: "column_3", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 3, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("1.41"), MaxValue: ptr("3.14"),
				TopValues: []profile.TopValue{tv("1.41", 1), tv("2.72", 1), tv("3.14", 1)},
			}},
		},
		SampleData: [][]string{
			{"1", "100", "3.14"},
			{"2", "200", "2.72"},
			{"3", "300", "1.41"},
		},
		Issues: nil,
	})
}

func TestAnalyzeEmptyCSV(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/empty.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	assertContract(t, contract, SourceContract{
		SourceFormat: "csv",
		Encoding:     "utf-8",
		Delimiter:    ",",
		HasHeader:    true,
		TotalRows:    0,
		Fields: []Field{
			{Name: "Name", DataType: profile.TypeEmpty, Profile: profile.FieldProfile{TopValues: []profile.TopValue{}}},
			{Name: "Age", DataType: profile.TypeEmpty, Profile: profile.FieldProfile{TopValues: []profile.TopValue{}}},
			{Name: "City", DataType: profile.TypeEmpty, Profile: profile.FieldProfile{TopValues: []profile.TopValue{}}},
		},
		SampleData: nil,
		Issues:     nil,
	})
}

func TestAnalyzeMixedTypes(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/mixed_types.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	assertContract(t, contract, SourceContract{
		SourceFormat: "csv",
		Encoding:     "utf-8",
		Delimiter:    ",",
		HasHeader:    true,
		TotalRows:    5,
		Fields: []Field{
			{Name: "ID", DataType: profile.TypeNumeric, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("1"), MaxValue: ptr("5"),
				TopValues: []profile.TopValue{tv("1", 1), tv("2", 1), tv("3", 1), tv("4", 1), tv("5", 1)},
			}},
			{Name: "Value", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 1, NullPercentage: 20,
				MinValue: ptr("100"), MaxValue: ptr("hello"),
				TopValues: []profile.TopValue{tv("100", 1), tv("300", 1), tv("500", 1), tv("hello", 1)},
			}},
			{Name: "Date", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 0, NullPercentage: 0,
				MinValue: ptr("2024-01-15"), MaxValue: ptr("not-a-date"),
				TopValues: []profile.TopValue{tv("2024-01-15", 1), tv("2024-01-16", 1), tv("2024-01-18", 1), tv("2024-01-19", 1), tv("not-a-date", 1)},
			}},
			{Name: "Notes", DataType: profile.TypeText, Profile: profile.FieldProfile{
				TotalCount: 5, NullCount: 1, NullPercentage: 20,
				MinValue: ptr("fifth entry"), MaxValue: ptr("third entry"),
				TopValues: []profile.TopValue{tv("fifth entry", 1), tv("first entry", 1), tv("second entry", 1), tv("third entry", 1)},
			}},
		},
		SampleData: [][]string{
			{"1", "100", "2024-01-15", "first entry"},
			{"2", "hello", "2024-01-16", "second entry"},
			{"3", "300", "not-a-date", "third entry"},
			{"4", "", "2024-01-18", ""},
			{"5", "500", "2024-01-19", "fifth entry"},
		},
		Issues: nil,
	})
}

func TestAnalyzeLatin1(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/latin1.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.Encoding != "latin-1" {
		t.Errorf("encoding = %q, want %q", contract.Encoding, "latin-1")
	}
	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	if len(contract.Fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(contract.Fields))
	}
	if contract.Fields[0].Name != "Name" {
		t.Errorf("field 0 name = %q, want %q", contract.Fields[0].Name, "Name")
	}
}

func TestAnalyzeAllEmptyColumn(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/all_empty_column.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	notes := contract.Fields[1]
	if notes.DataType != profile.TypeEmpty {
		t.Errorf("Notes data_type = %q, want %q", notes.DataType, profile.TypeEmpty)
	}
	if notes.Profile.NullCount != 3 {
		t.Errorf("Notes null_count = %d, want 3", notes.Profile.NullCount)
	}
	if notes.Profile.NullPercentage != 100 {
		t.Errorf("Notes null_percentage = %f, want 100", notes.Profile.NullPercentage)
	}
}

func TestAnalyzeQuotedFields(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/quoted_fields.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	if len(contract.SampleData) < 1 || len(contract.SampleData[0]) < 2 {
		t.Fatal("insufficient sample data")
	}
	if contract.SampleData[0][1] != "Has a comma, in description" {
		t.Errorf("quoted field = %q, want %q", contract.SampleData[0][1], "Has a comma, in description")
	}
}

func TestAnalyzeSingleColumn(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/single_column.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(contract.Fields) != 1 {
		t.Fatalf("fields count = %d, want 1", len(contract.Fields))
	}
	if contract.Fields[0].Name != "Email" {
		t.Errorf("field name = %q, want %q", contract.Fields[0].Name, "Email")
	}
	if contract.Fields[0].DataType != profile.TypeText {
		t.Errorf("data_type = %q, want %q", contract.Fields[0].DataType, profile.TypeText)
	}
}

func TestAnalyzeWhitespaceNulls(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/whitespace_nulls.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	age := contract.Fields[1]
	if age.Profile.NullCount != 2 {
		t.Errorf("Age null_count = %d, want 2", age.Profile.NullCount)
	}
	name := contract.Fields[0]
	if name.Profile.NullCount != 1 {
		t.Errorf("Name null_count = %d, want 1", name.Profile.NullCount)
	}
}

func TestAnalyzeLargeNumbers(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/large_numbers.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	rev := contract.Fields[1]
	if rev.DataType != profile.TypeNumeric {
		t.Errorf("Revenue data_type = %q, want %q", rev.DataType, profile.TypeNumeric)
	}
}

func TestAnalyzeDatesMultiFormat(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/dates_multi_format.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	eventDate := contract.Fields[1]
	if eventDate.DataType != profile.TypeDate {
		t.Errorf("EventDate data_type = %q, want %q", eventDate.DataType, profile.TypeDate)
	}
}

func TestAnalyzeNonexistentFile(t *testing.T) {
	_, err := AnalyzeFile(ctx, "testdata/does_not_exist.csv", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestAnalyzeWithOptions(t *testing.T) {
	opts := &Options{
		MaxSampleRows: 2,
		TopN:          2,
	}
	contract, err := AnalyzeFile(ctx, "testdata/simple.csv", opts)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(contract.SampleData) != 2 {
		t.Errorf("sample data rows = %d, want 2", len(contract.SampleData))
	}
	for _, f := range contract.Fields {
		if len(f.Profile.TopValues) > 2 {
			t.Errorf("field %q has %d top values, want <= 2", f.Name, len(f.Profile.TopValues))
		}
	}
	// TotalRows should still be 5 (all rows scanned).
	if contract.TotalRows != 5 {
		t.Errorf("total_rows = %d, want 5", contract.TotalRows)
	}
}

func TestAnalyzeSampleDataTruncation(t *testing.T) {
	opts := &Options{MaxSampleRows: 3}
	contract, err := AnalyzeFile(ctx, "testdata/simple.csv", opts)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(contract.SampleData) != 3 {
		t.Errorf("sample data rows = %d, want 3", len(contract.SampleData))
	}
	if contract.TotalRows != 5 {
		t.Errorf("total_rows = %d, want 5", contract.TotalRows)
	}
}

func TestAnalyzeInlineCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	content := "A,B\n1,\n,2\n,\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	contract, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	if contract.Fields[0].Profile.NullCount != 2 {
		t.Errorf("A null_count = %d, want 2", contract.Fields[0].Profile.NullCount)
	}
	if contract.Fields[0].DataType != profile.TypeNumeric {
		t.Errorf("A data_type = %q, want %q", contract.Fields[0].DataType, profile.TypeNumeric)
	}
}

func TestAnalyzeContractJSON(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/simple.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	data, err := json.Marshal(contract)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var roundTrip SourceContract
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	if roundTrip.SourceFormat != "csv" {
		t.Errorf("round-trip source_format = %q", roundTrip.SourceFormat)
	}
	if len(roundTrip.Fields) != 4 {
		t.Errorf("round-trip fields count = %d", len(roundTrip.Fields))
	}
}

func TestAnalyzeSingleRowNoHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "single.csv")
	if err := os.WriteFile(path, []byte("100,200,300\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	contract, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.HasHeader {
		t.Error("expected no header for all-numeric row")
	}
	if contract.TotalRows != 1 {
		t.Errorf("total_rows = %d, want 1", contract.TotalRows)
	}
	if contract.Fields[0].Name != "column_1" {
		t.Errorf("field name = %q, want column_1", contract.Fields[0].Name)
	}
}

func TestAnalyzeUnreadableFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "noperm.csv")
	if err := os.WriteFile(path, []byte("a,b\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(path, 0o644) }()

	_, err := AnalyzeFile(ctx, path, nil)
	if err == nil {
		t.Fatal("expected error for unreadable file")
	}
}

func TestAnalyzeReaderEmpty(t *testing.T) {
	r := bytes.NewReader(nil)
	_, err := AnalyzeReader(ctx, r, nil)
	if err == nil {
		t.Fatal("expected error for empty content")
	}
}

func TestAnalyzeReaderDirect(t *testing.T) {
	data := []byte("Name,Age\nAlice,30\nBob,25\n")
	r := bytes.NewReader(data)
	contract, err := AnalyzeReader(ctx, r, nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if contract.TotalRows != 2 {
		t.Errorf("total_rows = %d, want 2", contract.TotalRows)
	}
	if contract.SourcePath != "" {
		t.Errorf("source_path = %q, want empty for reader", contract.SourcePath)
	}
}

func TestAnalyzeReaderCancelled(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	r := bytes.NewReader([]byte("a,b\n1,2\n"))
	_, err := AnalyzeReader(cancelled, r, nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestAnalyzeReaderCancelDuringStream(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString("Name,Value\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&buf, "row%d,%d\n", i, i)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	r := &cancelAfterSeekReader{
		ReadSeeker: bytes.NewReader(buf.Bytes()),
		cancel:     cancel,
	}

	_, err := AnalyzeReader(cancelCtx, r, nil)
	if err == nil {
		t.Fatal("expected error for context cancelled during streaming")
	}
}

type cancelAfterSeekReader struct {
	io.ReadSeeker
	cancel func()
	seeked bool
}

func (r *cancelAfterSeekReader) Seek(offset int64, whence int) (int64, error) {
	n, err := r.ReadSeeker.Seek(offset, whence)
	if !r.seeked {
		r.seeked = true
		r.cancel()
	}
	return n, err
}

func TestAnalyzeReaderSeekError(t *testing.T) {
	r := &failSeekReader{data: []byte("a,b\n1,2\n")}
	_, err := AnalyzeReader(ctx, r, nil)
	if err == nil {
		t.Fatal("expected error for seek failure")
	}
}

type failSeekReader struct {
	data   []byte
	offset int
}

func (r *failSeekReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *failSeekReader) Seek(_ int64, _ int) (int64, error) {
	return 0, fmt.Errorf("seek not supported")
}

func TestAnalyzeReaderReadError(t *testing.T) {
	r := &failReadSeeker{}
	_, err := AnalyzeReader(ctx, r, nil)
	if err == nil {
		t.Fatal("expected error for read failure")
	}
}

type failReadSeeker struct{}

func (r *failReadSeeker) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("disk error")
}

func (r *failReadSeeker) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func TestAnalyzeReaderBOMReadError(t *testing.T) {
	data := slices.Concat(utf8BOM, []byte("Name,Value\nAlice,1\n"))
	r := &failAfterSeekReader{data: data}
	_, err := AnalyzeReader(ctx, r, nil)
	if err == nil {
		t.Fatal("expected error for BOM skip failure")
	}
}

type failAfterSeekReader struct {
	data   []byte
	offset int
	seeked bool
}

func (r *failAfterSeekReader) Read(p []byte) (int, error) {
	if r.seeked {
		return 0, fmt.Errorf("read failed after seek")
	}
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *failAfterSeekReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == 0 {
		r.seeked = true
		r.offset = 0
	}
	return int64(r.offset), nil
}

func TestAnalyzeReaderBOM(t *testing.T) {
	data := slices.Concat(utf8BOM, []byte("Name,Value\nAlice,1\n"))
	r := bytes.NewReader(data)
	contract, err := AnalyzeReader(ctx, r, nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if contract.Fields[0].Name != "Name" {
		t.Errorf("field name = %q, want Name", contract.Fields[0].Name)
	}
	if len(contract.Issues) != 1 || contract.Issues[0] != "UTF-8 BOM detected and stripped" {
		t.Errorf("issues = %v, want BOM issue", contract.Issues)
	}
}

func TestAnalyzeEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := AnalyzeFile(ctx, path, nil)
	if err == nil {
		t.Fatal("expected error for completely empty file")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error = %q, want message about empty", err.Error())
	}
}

func TestAnalyzeCancelledContext(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := AnalyzeFile(cancelled, "testdata/simple.csv", nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestAnalyzeAllRowsProfiled(t *testing.T) {
	// Verify that profiling covers all rows, not just a sample.
	// Create a file with 20 rows where a value appears only in row 15.
	dir := t.TempDir()
	path := filepath.Join(dir, "all_rows.csv")
	var buf bytes.Buffer
	buf.WriteString("Status\n")
	for i := 0; i < 14; i++ {
		buf.WriteString("active\n")
	}
	buf.WriteString("rare_value\n")
	for i := 0; i < 5; i++ {
		buf.WriteString("active\n")
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	contract, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.TotalRows != 20 {
		t.Errorf("total_rows = %d, want 20", contract.TotalRows)
	}

	status := contract.Fields[0]
	// "active" should appear 19 times, "rare_value" 1 time.
	found := map[string]int{}
	for _, tv := range status.Profile.TopValues {
		found[tv.Value] = tv.Count
	}
	if found["active"] != 19 {
		t.Errorf("active count = %d, want 19", found["active"])
	}
	if found["rare_value"] != 1 {
		t.Errorf("rare_value count = %d, want 1", found["rare_value"])
	}
}

func TestAnalyzeReaderStreamParseError(t *testing.T) {
	// Verify that a non-EOF error during CSV streaming is returned,
	// not silently swallowed.
	r := &failDuringStreamReader{
		data: []byte("name,age\nAlice,30\nBob,25\n"),
	}
	_, err := AnalyzeReader(ctx, r, nil)
	if err == nil {
		t.Fatal("expected error for mid-stream read failure")
	}
	if !strings.Contains(err.Error(), "injected stream error") {
		t.Errorf("error = %q, want to contain 'injected stream error'", err)
	}
}

// failDuringStreamReader succeeds for the sniffing and first-row reads
// but fails partway through streaming.
type failDuringStreamReader struct {
	data   []byte
	offset int
	reads  int
}

func (r *failDuringStreamReader) Read(p []byte) (int, error) {
	r.reads++
	// Fail on the 4th read (after sniff buffer, seek, and first row).
	if r.reads >= 4 {
		return 0, fmt.Errorf("injected stream error")
	}
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *failDuringStreamReader) Seek(offset int64, _ int) (int64, error) {
	r.offset = int(offset)
	return offset, nil
}

func TestAnalyzeRowShorterThanHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "short.csv")
	// Row 2 has only 1 field, header has 3.
	content := "name,age,city\nAlice,30,NYC\nBob\nCarol,25,LA\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	contract, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	// All 3 rows should be counted (short rows are still valid rows).
	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	// Should still have 3 fields.
	if len(contract.Fields) != 3 {
		t.Errorf("fields = %d, want 3", len(contract.Fields))
	}
}

func TestAnalyzeRowLongerThanHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "long.csv")
	// Row 2 has 4 fields, header has 2. Extra fields are dropped.
	content := "name,age\nAlice,30\nBob,25,NYC,extra\nCarol,35\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	contract, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	// Should still have 2 fields (header determines field count).
	if len(contract.Fields) != 2 {
		t.Errorf("fields = %d, want 2", len(contract.Fields))
	}
}

// assertContract compares the actual contract against the expected one,
// ignoring the SourcePath field (which depends on the caller).
func assertContract(t *testing.T, got *SourceContract, want SourceContract) {
	t.Helper()

	if got.SourceFormat != want.SourceFormat {
		t.Errorf("source_format = %q, want %q", got.SourceFormat, want.SourceFormat)
	}
	if got.Encoding != want.Encoding {
		t.Errorf("encoding = %q, want %q", got.Encoding, want.Encoding)
	}
	if got.Delimiter != want.Delimiter {
		t.Errorf("delimiter = %q, want %q", got.Delimiter, want.Delimiter)
	}
	if got.HasHeader != want.HasHeader {
		t.Errorf("has_header = %v, want %v", got.HasHeader, want.HasHeader)
	}
	if got.TotalRows != want.TotalRows {
		t.Errorf("total_rows = %d, want %d", got.TotalRows, want.TotalRows)
	}

	assertIssues(t, got.Issues, want.Issues)
	assertFields(t, got.Fields, want.Fields)
	assertSampleData(t, got.SampleData, want.SampleData)
}

func assertFields(t *testing.T, got, want []Field) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("fields count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		prefix := "field " + w.Name
		if g.Name != w.Name {
			t.Errorf("%s: name = %q, want %q", prefix, g.Name, w.Name)
		}
		if g.DataType != w.DataType {
			t.Errorf("%s: data_type = %q, want %q", prefix, g.DataType, w.DataType)
		}
		assertProfile(t, prefix, g.Profile, w.Profile)
	}
}

func assertProfile(t *testing.T, prefix string, got, want profile.FieldProfile) {
	t.Helper()
	if got.TotalCount != want.TotalCount {
		t.Errorf("%s: total_count = %d, want %d", prefix, got.TotalCount, want.TotalCount)
	}
	if got.NullCount != want.NullCount {
		t.Errorf("%s: null_count = %d, want %d", prefix, got.NullCount, want.NullCount)
	}
	if got.NullPercentage != want.NullPercentage {
		t.Errorf("%s: null_percentage = %f, want %f", prefix, got.NullPercentage, want.NullPercentage)
	}
	assertStringPtr(t, prefix+": min_value", got.MinValue, want.MinValue)
	assertStringPtr(t, prefix+": max_value", got.MaxValue, want.MaxValue)
	assertTopValues(t, prefix, got.TopValues, want.TopValues)
}

func assertTopValues(t *testing.T, prefix string, got, want []profile.TopValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: top_values len = %d, want %d (got %v)", prefix, len(got), len(want), got)
		return
	}
	for i := range want {
		if got[i].Value != want[i].Value || got[i].Count != want[i].Count {
			t.Errorf("%s: top_values[%d] = {%q, %d}, want {%q, %d}",
				prefix, i, got[i].Value, got[i].Count, want[i].Value, want[i].Count)
		}
	}
}

func assertStringPtr(t *testing.T, label string, got, want *string) {
	t.Helper()
	if got == nil && want == nil {
		return
	}
	if got == nil || want == nil {
		t.Errorf("%s: got=%v, want=%v", label, got, want)
		return
	}
	if *got != *want {
		t.Errorf("%s = %q, want %q", label, *got, *want)
	}
}

func assertSampleData(t *testing.T, got, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("sample_data rows = %d, want %d", len(got), len(want))
		return
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Errorf("sample_data[%d] cols = %d, want %d", i, len(got[i]), len(want[i]))
			continue
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("sample_data[%d][%d] = %q, want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}

func assertIssues(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("issues count = %d, want %d (got %v)", len(got), len(want), got)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("issues[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOptionsDefaults(t *testing.T) {
	// Nil options use defaults.
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

	// Explicit values override defaults.
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
