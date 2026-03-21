package jsoncontract

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
)

// AnalyzeFile opens a JSON or NDJSON file and produces a SourceContract.
func AnalyzeFile(ctx context.Context, path string, opts *Options) (*SourceContract, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func() { _ = f.Close() }()

	contract, err := AnalyzeReader(ctx, f, opts)
	if err != nil {
		return nil, err
	}
	contract.SourcePath = path
	return contract, nil
}

// AnalyzeReader analyzes JSON or NDJSON from any io.Reader.
// It auto-detects the format by peeking at the first non-whitespace byte.
func AnalyzeReader(ctx context.Context, r io.Reader, opts *Options) (*SourceContract, error) {
	br := bufio.NewReader(r)

	// Peek at first non-whitespace byte to determine format.
	format, err := detectFormat(br)
	if err != nil {
		return nil, err
	}

	switch format {
	case "json":
		return analyzeJSONArray(ctx, br, opts)
	case "ndjson":
		return analyzeNDJSON(ctx, br, opts)
	default:
		return nil, fmt.Errorf("unexpected format: %s", format)
	}
}

// detectFormat peeks at the first non-whitespace byte to determine
// whether the input is a JSON array or NDJSON.
func detectFormat(br *bufio.Reader) (string, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return "", fmt.Errorf("empty input: %w", err)
		}
		if b == ' ' || b == '\t' || b == '\r' || b == '\n' {
			continue
		}
		// Put it back so the decoder sees it.
		if err := br.UnreadByte(); err != nil {
			return "", fmt.Errorf("unread byte: %w", err)
		}
		if b == '[' {
			return "json", nil
		}
		return "ndjson", nil
	}
}

// analyzeJSONArray streams a JSON array using json.Decoder, reading
// one object at a time to avoid loading the entire array into memory.
func analyzeJSONArray(ctx context.Context, r io.Reader, opts *Options) (*SourceContract, error) {
	dec := json.NewDecoder(r)

	// Read opening bracket.
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("read array start: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", tok)
	}

	state := newAnalysisState(opts)

	for dec.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if state.maxRows > 0 && state.totalRows >= state.maxRows {
			break
		}

		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			state.issues = append(state.issues, fmt.Sprintf("object %d: %s", state.totalRows+1, err))
			// Decode errors are unrecoverable for the JSON decoder —
			// it cannot skip to the next valid object in an array.
			break
		}
		state.observe(obj)
	}

	return state.finish("json"), nil
}

// analyzeNDJSON reads newline-delimited JSON line by line.
func analyzeNDJSON(ctx context.Context, r io.Reader, opts *Options) (*SourceContract, error) {
	scanner := bufio.NewScanner(r)
	// Allow lines up to 10MB (NDJSON can have large objects).
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	state := newAnalysisState(opts)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if state.maxRows > 0 && state.totalRows >= state.maxRows {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			state.issues = append(state.issues, fmt.Sprintf("line %d: %s", state.totalRows+1, err))
			continue
		}
		state.observe(obj)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}

	return state.finish("ndjson"), nil
}

// --- analysis state ---------------------------------------------------------

type analysisState struct {
	fieldOrder []string // insertion order
	fieldSet   map[string]bool
	profilers  map[string]*fieldProfiler
	sampleData [][]string
	issues     []string
	totalRows  int
	maxRows    int
	maxSample  int
	topN       int
}

func newAnalysisState(opts *Options) *analysisState {
	return &analysisState{
		fieldSet:  make(map[string]bool),
		profilers: make(map[string]*fieldProfiler),
		maxRows:   opts.maxRows(),
		maxSample: opts.maxSampleRows(),
		topN:      opts.topN(),
	}
}

func (s *analysisState) observe(obj map[string]any) {
	s.totalRows++

	// Discover new fields (preserving insertion order).
	for key := range obj {
		if !s.fieldSet[key] {
			s.fieldSet[key] = true
			s.fieldOrder = append(s.fieldOrder, key)
			s.profilers[key] = newFieldProfiler(10000) // maxTracked
		}
	}

	// Profile each known field. Missing fields count as null.
	row := make([]string, len(s.fieldOrder))
	for i, key := range s.fieldOrder {
		val, exists := obj[key]
		if !exists || val == nil {
			s.profilers[key].observeNull()
			row[i] = ""
		} else {
			str := valueToString(val)
			dt := classifyJSONValue(val)
			s.profilers[key].observe(str, dt)
			row[i] = str
		}
	}

	if len(s.sampleData) < s.maxSample {
		s.sampleData = append(s.sampleData, row)
	}
}

func (s *analysisState) finish(format string) *SourceContract {
	fields := make([]Field, len(s.fieldOrder))
	for i, key := range s.fieldOrder {
		fields[i] = s.profilers[key].finish(key, s.topN)
	}

	return &SourceContract{
		SourceFormat: format,
		TotalRows:    s.totalRows,
		Encoding:     "utf-8",
		Fields:       fields,
		SampleData:   s.sampleData,
		Issues:       s.issues,
	}
}

// --- per-field profiler -----------------------------------------------------

type fieldProfiler struct {
	totalCount int
	nullCount  int
	dataType   DataType
	freqs      map[string]int
	maxTracked int
	min, max   *string
}

func newFieldProfiler(maxTracked int) *fieldProfiler {
	return &fieldProfiler{
		dataType:   TypeEmpty,
		freqs:      make(map[string]int),
		maxTracked: maxTracked,
	}
}

func (p *fieldProfiler) observeNull() {
	p.totalCount++
	p.nullCount++
}

func (p *fieldProfiler) observe(str string, dt DataType) {
	p.totalCount++
	p.dataType = mergeTypes(p.dataType, dt)

	// Min/max (lexicographic).
	if p.min == nil || str < *p.min {
		cp := str
		p.min = &cp
	}
	if p.max == nil || str > *p.max {
		cp := str
		p.max = &cp
	}

	// Frequency tracking with cap.
	if count, exists := p.freqs[str]; exists {
		p.freqs[str] = count + 1
	} else if len(p.freqs) < p.maxTracked {
		p.freqs[str] = 1
	}
}

func (p *fieldProfiler) finish(name string, topN int) Field {
	nullPct := 0.0
	if p.totalCount > 0 {
		nullPct = math.Round(float64(p.nullCount)/float64(p.totalCount)*10000) / 100
	}

	return Field{
		Name:     name,
		DataType: p.dataType,
		Profile: FieldProfile{
			TotalCount:     p.totalCount,
			NullCount:      p.nullCount,
			NullPercentage: nullPct,
			MinValue:       p.min,
			MaxValue:       p.max,
			TopValues:      topNValues(p.freqs, topN),
		},
	}
}

// --- type classification ----------------------------------------------------

// classifyJSONValue determines the DataType from a JSON-decoded Go value.
func classifyJSONValue(v any) DataType {
	switch v.(type) {
	case float64: // json.Unmarshal decodes all numbers as float64
		return TypeNumeric
	case bool:
		return TypeBoolean
	case string:
		return TypeText
	case map[string]any:
		return TypeObject
	case []any:
		return TypeArray
	case nil:
		return TypeNull
	default:
		return TypeText
	}
}

// mergeTypes combines two data types using priority:
// text > object > array > date > boolean > numeric > null > empty.
func mergeTypes(a, b DataType) DataType {
	if a == b {
		return a
	}
	if typePriority(a) >= typePriority(b) {
		return a
	}
	return b
}

func typePriority(t DataType) int {
	switch t {
	case TypeText:
		return 7
	case TypeObject:
		return 6
	case TypeArray:
		return 5
	case TypeDate:
		return 4
	case TypeBoolean:
		return 3
	case TypeNumeric:
		return 2
	case TypeNull:
		return 1
	default:
		return 0
	}
}

// --- helpers ----------------------------------------------------------------

// valueToString converts a JSON value to its string representation.
func valueToString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		// Avoid trailing zeros for integers.
		if val == math.Trunc(val) && !math.IsInf(val, 0) {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

// topNValues returns the N most frequent values from a frequency map.
func topNValues(freqs map[string]int, n int) []TopValue {
	if len(freqs) == 0 {
		return []TopValue{}
	}

	entries := make([]TopValue, 0, len(freqs))
	for k, v := range freqs {
		entries = append(entries, TopValue{Value: k, Count: v})
	}

	slices.SortFunc(entries, func(a, b TopValue) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Value, b.Value)
	})

	if n > len(entries) {
		n = len(entries)
	}
	return entries[:n]
}
