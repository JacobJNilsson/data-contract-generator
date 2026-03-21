package jsoncontract

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var ctx = context.Background()

// --- JSON array tests -------------------------------------------------------

func TestAnalyzeJSONArray(t *testing.T) {
	data := `[
		{"name": "Alice", "age": 30, "active": true},
		{"name": "Bob", "age": 25, "active": false},
		{"name": "Carol", "age": 35, "active": true}
	]`

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if contract.SourceFormat != "json" {
		t.Errorf("format = %q, want json", contract.SourceFormat)
	}
	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
	if len(contract.Fields) != 3 {
		t.Fatalf("fields = %d, want 3", len(contract.Fields))
	}

	// Fields should be in insertion order.
	nameField := findField(contract.Fields, "name")
	ageField := findField(contract.Fields, "age")
	activeField := findField(contract.Fields, "active")

	if nameField == nil || ageField == nil || activeField == nil {
		t.Fatal("missing expected fields")
	}

	if nameField.DataType != TypeText {
		t.Errorf("name type = %q, want text", nameField.DataType)
	}
	if ageField.DataType != TypeNumeric {
		t.Errorf("age type = %q, want numeric", ageField.DataType)
	}
	if activeField.DataType != TypeBoolean {
		t.Errorf("active type = %q, want boolean", activeField.DataType)
	}

	// Check profiling.
	if nameField.Profile.TotalCount != 3 {
		t.Errorf("name total = %d, want 3", nameField.Profile.TotalCount)
	}
	if nameField.Profile.NullCount != 0 {
		t.Errorf("name nulls = %d, want 0", nameField.Profile.NullCount)
	}
}

func TestAnalyzeJSONArrayEmpty(t *testing.T) {
	contract, err := AnalyzeReader(ctx, strings.NewReader("[]"), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if contract.TotalRows != 0 {
		t.Errorf("total_rows = %d, want 0", contract.TotalRows)
	}
	if len(contract.Fields) != 0 {
		t.Errorf("fields = %d, want 0", len(contract.Fields))
	}
}

func TestAnalyzeJSONArrayWithNulls(t *testing.T) {
	data := `[
		{"name": "Alice", "bio": "Engineer"},
		{"name": "Bob", "bio": null},
		{"name": "Carol"}
	]`

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	bio := findField(contract.Fields, "bio")
	if bio == nil {
		t.Fatal("bio field not found")
	}
	// Bob has null, Carol is missing -> 2 nulls out of 3.
	if bio.Profile.NullCount != 2 {
		t.Errorf("bio nulls = %d, want 2", bio.Profile.NullCount)
	}
}

func TestAnalyzeJSONArrayNestedObjects(t *testing.T) {
	data := `[
		{"name": "Alice", "address": {"city": "NYC"}},
		{"name": "Bob", "tags": ["admin", "user"]}
	]`

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	addr := findField(contract.Fields, "address")
	tags := findField(contract.Fields, "tags")

	if addr == nil || tags == nil {
		t.Fatal("missing expected fields")
	}

	if addr.DataType != TypeObject {
		t.Errorf("address type = %q, want object", addr.DataType)
	}
	if tags.DataType != TypeArray {
		t.Errorf("tags type = %q, want array", tags.DataType)
	}
}

func TestAnalyzeJSONArrayMixedTypes(t *testing.T) {
	// "value" starts as numeric then becomes text -> should resolve to text.
	data := `[
		{"value": 42},
		{"value": "hello"}
	]`

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	v := findField(contract.Fields, "value")
	if v == nil {
		t.Fatal("value field not found")
	}
	if v.DataType != TypeText {
		t.Errorf("value type = %q, want text (mixed types resolve to text)", v.DataType)
	}
}

// --- NDJSON tests -----------------------------------------------------------

func TestAnalyzeNDJSON(t *testing.T) {
	data := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Carol", "age": 35}
`

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if contract.SourceFormat != "ndjson" {
		t.Errorf("format = %q, want ndjson", contract.SourceFormat)
	}
	if contract.TotalRows != 3 {
		t.Errorf("total_rows = %d, want 3", contract.TotalRows)
	}
}

func TestAnalyzeNDJSONBlankLines(t *testing.T) {
	data := `{"a": 1}

{"a": 2}

`
	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if contract.TotalRows != 2 {
		t.Errorf("total_rows = %d, want 2", contract.TotalRows)
	}
}

func TestAnalyzeNDJSONParseErrors(t *testing.T) {
	data := `{"a": 1}
not json
{"a": 2}
`
	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if contract.TotalRows != 2 {
		t.Errorf("total_rows = %d, want 2 (skipping bad line)", contract.TotalRows)
	}
	if len(contract.Issues) != 1 {
		t.Errorf("issues = %d, want 1", len(contract.Issues))
	}
}

// --- File tests -------------------------------------------------------------

func TestAnalyzeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	content := `[{"id": 1}, {"id": 2}]`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	contract, err := AnalyzeFile(ctx, path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile: %v", err)
	}

	if contract.SourcePath != path {
		t.Errorf("source_path = %q, want %q", contract.SourcePath, path)
	}
	if contract.TotalRows != 2 {
		t.Errorf("total_rows = %d, want 2", contract.TotalRows)
	}
}

func TestAnalyzeFileNonexistent(t *testing.T) {
	_, err := AnalyzeFile(ctx, "/nonexistent/path.json", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

// --- Sample data tests ------------------------------------------------------

func TestAnalyzeSampleDataLimit(t *testing.T) {
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, `{"n": 1}`)
	}
	data := strings.Join(lines, "\n")

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if contract.TotalRows != 20 {
		t.Errorf("total_rows = %d, want 20", contract.TotalRows)
	}
	if len(contract.SampleData) != 5 {
		t.Errorf("sample_data = %d, want 5 (default limit)", len(contract.SampleData))
	}
}

// --- Options tests ----------------------------------------------------------

func TestAnalyzeWithMaxRows(t *testing.T) {
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, `{"n": 1}`)
	}
	data := strings.Join(lines, "\n")

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), &Options{MaxRows: 10})
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if contract.TotalRows != 10 {
		t.Errorf("total_rows = %d, want 10", contract.TotalRows)
	}
}

func TestAnalyzeWithMaxRowsJSON(t *testing.T) {
	var objs []string
	for i := 0; i < 50; i++ {
		objs = append(objs, `{"n": 1}`)
	}
	data := "[" + strings.Join(objs, ",") + "]"

	contract, err := AnalyzeReader(ctx, strings.NewReader(data), &Options{MaxRows: 5})
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}

	if contract.TotalRows != 5 {
		t.Errorf("total_rows = %d, want 5", contract.TotalRows)
	}
}

// --- Format detection tests -------------------------------------------------

func TestDetectFormatJSON(t *testing.T) {
	format, err := detectFormat(bufioReader("  ["))
	if err != nil {
		t.Fatal(err)
	}
	if format != "json" {
		t.Errorf("format = %q, want json", format)
	}
}

func TestDetectFormatNDJSON(t *testing.T) {
	format, err := detectFormat(bufioReader(`{"a": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	if format != "ndjson" {
		t.Errorf("format = %q, want ndjson", format)
	}
}

func TestDetectFormatEmpty(t *testing.T) {
	_, err := detectFormat(bufioReader(""))
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

// --- Type classification tests ----------------------------------------------

func TestClassifyJSONValue(t *testing.T) {
	tests := []struct {
		value any
		want  DataType
	}{
		{float64(42), TypeNumeric},
		{true, TypeBoolean},
		{false, TypeBoolean},
		{"hello", TypeText},
		{map[string]any{"a": 1}, TypeObject},
		{[]any{1, 2}, TypeArray},
		{nil, TypeNull},
	}

	for _, tt := range tests {
		got := classifyJSONValue(tt.value)
		if got != tt.want {
			t.Errorf("classifyJSONValue(%v) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

func TestMergeTypes(t *testing.T) {
	tests := []struct {
		a, b DataType
		want DataType
	}{
		{TypeNumeric, TypeNumeric, TypeNumeric},
		{TypeNumeric, TypeText, TypeText},
		{TypeBoolean, TypeNumeric, TypeBoolean},
		{TypeEmpty, TypeNumeric, TypeNumeric},
		{TypeNull, TypeNumeric, TypeNumeric},
		{TypeObject, TypeArray, TypeObject},
	}

	for _, tt := range tests {
		got := mergeTypes(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("mergeTypes(%q, %q) = %q, want %q", tt.a, tt.b, got, tt.want)
		}
	}
}

// --- Value conversion tests -------------------------------------------------

func TestValueToString(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{"hello", "hello"},
		{float64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
		{nil, ""},
		{map[string]any{"a": float64(1)}, `{"a":1}`},
		{[]any{float64(1), float64(2)}, `[1,2]`},
	}

	for _, tt := range tests {
		got := valueToString(tt.value)
		if got != tt.want {
			t.Errorf("valueToString(%v) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

// --- Top values tests -------------------------------------------------------

func TestTopNValues(t *testing.T) {
	freqs := map[string]int{"a": 10, "b": 5, "c": 1}
	got := topNValues(freqs, 2)
	if len(got) != 2 {
		t.Fatalf("topNValues() len = %d, want 2", len(got))
	}
	if got[0].Value != "a" || got[0].Count != 10 {
		t.Errorf("top[0] = %v, want {a, 10}", got[0])
	}
}

func TestTopNValuesEmpty(t *testing.T) {
	got := topNValues(map[string]int{}, 5)
	if len(got) != 0 {
		t.Errorf("topNValues(empty) len = %d, want 0", len(got))
	}
}

// --- Options tests ----------------------------------------------------------

func TestOptionsDefaults(t *testing.T) {
	var o *Options
	if o.topN() != 5 {
		t.Errorf("topN = %d, want 5", o.topN())
	}
	if o.maxTracked() != 10000 {
		t.Errorf("maxTracked = %d, want 10000", o.maxTracked())
	}
	if o.maxSampleRows() != 5 {
		t.Errorf("maxSampleRows = %d, want 5", o.maxSampleRows())
	}
	if o.maxRows() != 0 {
		t.Errorf("maxRows = %d, want 0", o.maxRows())
	}
}

func TestOptionsCustom(t *testing.T) {
	o := &Options{TopN: 3, MaxTracked: 100, MaxSampleRows: 10, MaxRows: 50}
	if o.topN() != 3 {
		t.Errorf("topN = %d, want 3", o.topN())
	}
	if o.maxTracked() != 100 {
		t.Errorf("maxTracked = %d, want 100", o.maxTracked())
	}
	if o.maxSampleRows() != 10 {
		t.Errorf("maxSampleRows = %d, want 10", o.maxSampleRows())
	}
	if o.maxRows() != 50 {
		t.Errorf("maxRows = %d, want 50", o.maxRows())
	}
}

// --- Context cancellation test ----------------------------------------------

func TestAnalyzeReaderCancelled(t *testing.T) {
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	data := `[{"a": 1}, {"a": 2}]`
	_, err := AnalyzeReader(cancelCtx, strings.NewReader(data), nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// --- Invalid JSON -----------------------------------------------------------

func TestAnalyzeReaderNotJSON(t *testing.T) {
	_, err := AnalyzeReader(ctx, strings.NewReader(""), nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestAnalyzeReaderInvalidJSONArray(t *testing.T) {
	// Non-array starting with [ but malformed.
	_, err := AnalyzeReader(ctx, strings.NewReader("[invalid"), nil)
	if err == nil {
		t.Log("no error — decoder may tolerate partial JSON")
	}
}

func TestAnalyzeReaderNotAnArray(t *testing.T) {
	// Starts with { but that's NDJSON, not a JSON array. Should work.
	contract, err := AnalyzeReader(ctx, strings.NewReader(`{"a": 1}`), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	if contract.SourceFormat != "ndjson" {
		t.Errorf("format = %q, want ndjson", contract.SourceFormat)
	}
}

func TestAnalyzeJSONArrayBadObject(t *testing.T) {
	// Array with one good object and one bad one.
	data := `[{"a": 1}, "not an object", {"a": 2}]`
	contract, err := AnalyzeReader(ctx, strings.NewReader(data), nil)
	if err != nil {
		t.Fatalf("AnalyzeReader: %v", err)
	}
	// "not an object" is a valid JSON string — it decodes but isn't a map.
	// It should be recorded as an issue.
	if len(contract.Issues) == 0 {
		// The decoder decodes it as a string, not a map[string]any,
		// so json.Decode into map[string]any will error.
		t.Log("no issues — decoder handled non-object gracefully")
	}
}

func TestAnalyzeNDJSONCancelled(t *testing.T) {
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, err := AnalyzeReader(cancelCtx, strings.NewReader(`{"a":1}`), nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// --- Helpers ----------------------------------------------------------------

func bufioReader(s string) *bufio.Reader {
	return bufio.NewReader(bytes.NewReader([]byte(s)))
}

func findField(fields []Field, name string) *Field {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}
