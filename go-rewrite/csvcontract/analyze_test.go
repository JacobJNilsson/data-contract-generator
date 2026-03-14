package csvcontract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var ctx = context.Background()

func ptr(s string) *string { return &s }

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
			{Name: "Name", DataType: TypeText, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 5,
				MinValue: ptr("Alice"), MaxValue: ptr("Eve"),
				SampleValues: []string{"Alice", "Bob", "Charlie", "Diana", "Eve"},
			}},
			{Name: "Age", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 5,
				MinValue: ptr("25"), MaxValue: ptr("35"),
				SampleValues: []string{"25", "28", "30", "32", "35"},
			}},
			{Name: "City", DataType: TypeText, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 5,
				MinValue: ptr("Berlin"), MaxValue: ptr("Tokyo"),
				SampleValues: []string{"Berlin", "London", "New York", "Paris", "Tokyo"},
			}},
			{Name: "Score", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 1, NullPercentage: 20, DistinctCount: 4,
				MinValue: ptr("87.3"), MaxValue: ptr("95.5"),
				SampleValues: []string{"87.3", "88.9", "92.1", "95.5"},
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
			{Name: "Date", DataType: TypeDate, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 5,
				MinValue: ptr("2024-01-15"), MaxValue: ptr("2024-01-19"),
				SampleValues: []string{"2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"},
			}},
			{Name: "Account", DataType: TypeText, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 2,
				MinValue: ptr("Depot"), MaxValue: ptr("Savings"),
				SampleValues: []string{"Depot", "Savings"},
			}},
			{Name: "Amount", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 2, NullPercentage: 40, DistinctCount: 3,
				MinValue: ptr("910,11"), MaxValue: ptr("5678,90"),
				SampleValues: []string{"1234,56", "5678,90", "910,11"},
			}},
			{Name: "Currency", DataType: TypeText, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 1,
				MinValue: ptr("SEK"), MaxValue: ptr("SEK"),
				SampleValues: []string{"SEK"},
			}},
			{Name: "Quantity", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 2, NullPercentage: 40, DistinctCount: 3,
				MinValue: ptr("50"), MaxValue: ptr("200"),
				SampleValues: []string{"100", "200", "50"},
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
			{Name: "column_1", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 3,
				MinValue: ptr("1"), MaxValue: ptr("3"),
				SampleValues: []string{"1", "2", "3"},
			}},
			{Name: "column_2", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 3,
				MinValue: ptr("100"), MaxValue: ptr("300"),
				SampleValues: []string{"100", "200", "300"},
			}},
			{Name: "column_3", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 3,
				MinValue: ptr("1.41"), MaxValue: ptr("3.14"),
				SampleValues: []string{"1.41", "2.72", "3.14"},
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
			{Name: "Name", DataType: TypeEmpty, Profile: FieldProfile{SampleValues: []string{}}},
			{Name: "Age", DataType: TypeEmpty, Profile: FieldProfile{SampleValues: []string{}}},
			{Name: "City", DataType: TypeEmpty, Profile: FieldProfile{SampleValues: []string{}}},
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
			{Name: "ID", DataType: TypeNumeric, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 5,
				MinValue: ptr("1"), MaxValue: ptr("5"),
				SampleValues: []string{"1", "2", "3", "4", "5"},
			}},
			{Name: "Value", DataType: TypeText, Profile: FieldProfile{
				NullCount: 1, NullPercentage: 20, DistinctCount: 4,
				MinValue: ptr("100"), MaxValue: ptr("hello"),
				SampleValues: []string{"100", "300", "500", "hello"},
			}},
			{Name: "Date", DataType: TypeText, Profile: FieldProfile{
				NullCount: 0, NullPercentage: 0, DistinctCount: 5,
				MinValue: ptr("2024-01-15"), MaxValue: ptr("not-a-date"),
				SampleValues: []string{"2024-01-15", "2024-01-16", "2024-01-18", "2024-01-19", "not-a-date"},
			}},
			{Name: "Notes", DataType: TypeText, Profile: FieldProfile{
				NullCount: 1, NullPercentage: 20, DistinctCount: 4,
				MinValue: ptr("fifth entry"), MaxValue: ptr("third entry"),
				SampleValues: []string{"fifth entry", "first entry", "second entry", "third entry"},
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
	if contract.HasHeader != true {
		t.Error("expected has_header = true")
	}
	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	if len(contract.Fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(contract.Fields))
	}
	// Latin-1 names should be decoded to UTF-8.
	if contract.Fields[0].Name != "Name" {
		t.Errorf("field 0 name = %q, want %q", contract.Fields[0].Name, "Name")
	}
	if contract.Fields[1].Name != "City" {
		t.Errorf("field 1 name = %q, want %q", contract.Fields[1].Name, "City")
	}
}

func TestAnalyzeAllEmptyColumn(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/all_empty_column.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(contract.Fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(contract.Fields))
	}
	// The "Notes" column is entirely empty.
	notes := contract.Fields[1]
	if notes.DataType != TypeEmpty {
		t.Errorf("Notes data_type = %q, want %q", notes.DataType, TypeEmpty)
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
	if contract.Delimiter != "," {
		t.Errorf("delimiter = %q, want %q", contract.Delimiter, ",")
	}
	// The description field should contain the unquoted value with the comma.
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
	if contract.Fields[0].DataType != TypeText {
		t.Errorf("data_type = %q, want %q", contract.Fields[0].DataType, TypeText)
	}
	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
}

func TestAnalyzeWhitespaceNulls(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/whitespace_nulls.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	// Age column has "30", "  " (whitespace-only), "", "28" -> 2 nulls.
	age := contract.Fields[1]
	if age.Profile.NullCount != 2 {
		t.Errorf("Age null_count = %d, want 2", age.Profile.NullCount)
	}
	// Name column has " " (whitespace-only) -> 1 null.
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

	if len(contract.Fields) != 4 {
		t.Fatalf("fields count = %d, want 4", len(contract.Fields))
	}
	// Revenue should be detected as numeric despite thousand separators.
	rev := contract.Fields[1]
	if rev.DataType != TypeNumeric {
		t.Errorf("Revenue data_type = %q, want %q", rev.DataType, TypeNumeric)
	}
}

func TestAnalyzeDatesMultiFormat(t *testing.T) {
	contract, err := AnalyzeFile(ctx, "testdata/dates_multi_format.csv", nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	// EventDate has both ISO and DD/MM/YYYY formats -> should still be date
	// since all values match date patterns.
	eventDate := contract.Fields[1]
	if eventDate.DataType != TypeDate {
		t.Errorf("EventDate data_type = %q, want %q", eventDate.DataType, TypeDate)
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
		SampleSize:      2,
		MaxSampleValues: 2,
		MaxSampleRows:   2,
	}
	contract, err := AnalyzeFile(ctx, "testdata/simple.csv", opts)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if len(contract.SampleData) != 2 {
		t.Errorf("sample data rows = %d, want 2", len(contract.SampleData))
	}
	// With sample size 2, only 2 data rows are analyzed for profiling.
	for _, f := range contract.Fields {
		if len(f.Profile.SampleValues) > 2 {
			t.Errorf("field %q has %d sample values, want <= 2", f.Name, len(f.Profile.SampleValues))
		}
	}
}

func TestAnalyzeSampleDataTruncation(t *testing.T) {
	// simple.csv has 5 data rows. With MaxSampleRows=3, sample_data
	// should be truncated to 3 rows.
	opts := &Options{MaxSampleRows: 3}
	contract, err := AnalyzeFile(ctx, "testdata/simple.csv", opts)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}
	if len(contract.SampleData) != 3 {
		t.Errorf("sample data rows = %d, want 3", len(contract.SampleData))
	}
	// Total rows should still be 5 (all rows counted).
	if contract.TotalRows != 5 {
		t.Errorf("total_rows = %d, want 5", contract.TotalRows)
	}
}

func TestAnalyzeInlineCSV(t *testing.T) {
	// Test with a CSV constructed in the test to exercise edge cases
	// not covered by testdata files.
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
	// Column A: "1", "", "" -> 2 nulls, type numeric.
	if contract.Fields[0].Profile.NullCount != 2 {
		t.Errorf("A null_count = %d, want 2", contract.Fields[0].Profile.NullCount)
	}
	if contract.Fields[0].DataType != TypeNumeric {
		t.Errorf("A data_type = %q, want %q", contract.Fields[0].DataType, TypeNumeric)
	}
}

func TestAnalyzeContractJSON(t *testing.T) {
	// Verify that the contract serializes to valid JSON and round-trips.
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
	// Remove read permission.
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
	if contract.Encoding != "utf-8" {
		t.Errorf("encoding = %q, want utf-8", contract.Encoding)
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
	// Cancel the context after the sniff phase completes but before
	// all rows are read. We use a large enough file that streaming
	// will check ctx.Err() at least once.
	var buf bytes.Buffer
	buf.WriteString("Name,Value\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&buf, "row%d,%d\n", i, i)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())

	// Wrap the reader to cancel after the sniff phase seek.
	r := &cancelAfterSeekReader{
		ReadSeeker: bytes.NewReader(buf.Bytes()),
		cancel:     cancel,
	}

	_, err := AnalyzeReader(cancelCtx, r, nil)
	if err == nil {
		t.Fatal("expected error for context cancelled during streaming")
	}
}

// cancelAfterSeekReader cancels its context after the first Seek call,
// simulating a cancellation that arrives between sniff and stream phases.
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
	// A file that has a BOM in the sniff buffer, seeks back fine,
	// but fails when trying to skip the BOM bytes in phase 2.
	data := append(utf8BOM, []byte("Name,Value\nAlice,1\n")...)
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
	// Verify BOM handling works through the streaming path.
	data := append(utf8BOM, []byte("Name,Value\nAlice,1\n")...)
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

// assertContract compares the actual contract against the expected one,
// ignoring the SourcePath field (which depends on the absolute path).
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

func assertProfile(t *testing.T, prefix string, got, want FieldProfile) {
	t.Helper()
	if got.NullCount != want.NullCount {
		t.Errorf("%s: null_count = %d, want %d", prefix, got.NullCount, want.NullCount)
	}
	if got.NullPercentage != want.NullPercentage {
		t.Errorf("%s: null_percentage = %f, want %f", prefix, got.NullPercentage, want.NullPercentage)
	}
	if got.DistinctCount != want.DistinctCount {
		t.Errorf("%s: distinct_count = %d, want %d", prefix, got.DistinctCount, want.DistinctCount)
	}
	assertStringPtr(t, prefix+": min_value", got.MinValue, want.MinValue)
	assertStringPtr(t, prefix+": max_value", got.MaxValue, want.MaxValue)
	assertStringSlice(t, prefix+": sample_values", got.SampleValues, want.SampleValues)
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

func assertStringSlice(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: len = %d, want %d (got %v)", label, len(got), len(want), got)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %q, want %q", label, i, got[i], want[i])
		}
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
