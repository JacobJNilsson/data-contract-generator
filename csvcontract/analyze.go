package csvcontract

import (
	"bytes"
	"context"
	csvstd "encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/profile"
)

// AnalyzeFile opens a CSV file and produces a SourceContract describing
// its structure, encoding, schema, and data quality. For backend use
// where the data comes from a stream (HTTP upload, S3, etc.), use
// AnalyzeReader directly.
func AnalyzeFile(ctx context.Context, path string, opts *Options) (*SourceContract, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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

// AnalyzeReader analyzes CSV data from a seekable reader. It reads the
// stream in two phases:
//
//  1. Sniff: read the first 8KB to detect encoding and delimiter, then
//     seek back to the start.
//  2. Stream: single sequential pass through all rows. The first row
//     plus a bounded probe of the following rows are used for header
//     detection. Every data row is fed to per-column profilers (type
//     inference, null counting, frequency tracking, min/max). Up to
//     MaxSampleRows are kept for the SampleData field.
//
// Peak memory is bounded by MaxTracked (default 10,000) distinct values
// per column plus MaxSampleRows (default 5) stored rows, regardless of
// file size.
func AnalyzeReader(ctx context.Context, rs io.ReadSeeker, opts *Options) (*SourceContract, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Phase 1: sniff encoding and delimiter from the first 8KB.
	sniffBuf := make([]byte, sniffSize)
	n, err := io.ReadFull(rs, sniffBuf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read sniff buffer: %w", err)
	}
	sniffBuf = sniffBuf[:n]

	encoding, hasBOM := detectEncodingFromBytes(sniffBuf)

	delimBuf := sniffBuf
	switch encoding {
	case encodingUTF8:
		if hasBOM {
			delimBuf = bytes.TrimPrefix(delimBuf, utf8BOM)
		}
	case encodingUTF16LE, encodingUTF16BE:
		// The UTF-16 decoder consumes the BOM itself.
		delimBuf = decodeUTF16(delimBuf)
	case encodingWindows1252:
		delimBuf = decodeWindows1252(delimBuf)
	}
	delimiter := detectDelimiterFromBytes(delimBuf)

	// Seek back to start for the full streaming pass.
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to start: %w", err)
	}

	// Phase 2: stream through the CSV content.
	var csvReader io.Reader = rs
	switch encoding {
	case encodingUTF8:
		if hasBOM {
			bomBuf := make([]byte, len(utf8BOM))
			if _, err := io.ReadFull(rs, bomBuf); err != nil {
				return nil, fmt.Errorf("skip BOM: %w", err)
			}
		}
	case encodingUTF16LE, encodingUTF16BE:
		csvReader = newUTF16Reader(csvReader)
	case encodingWindows1252:
		csvReader = newWindows1252Reader(csvReader)
	}

	result, err := streamAnalyze(ctx, csvReader, delimiter, opts)
	if err != nil {
		return nil, err
	}

	var issues []string
	switch encoding {
	case encodingUTF8:
		if hasBOM {
			issues = append(issues, "UTF-8 BOM detected and stripped")
		}
	case encodingUTF16LE:
		issues = append(issues, "UTF-16 LE BOM detected; input decoded to UTF-8")
	case encodingUTF16BE:
		issues = append(issues, "UTF-16 BE BOM detected; input decoded to UTF-8")
	}

	result.SourceFormat = "csv"
	result.Encoding = encoding
	result.Delimiter = string(delimiter)
	// Encoding issues lead, then the data-quality issues collected
	// during the streaming pass, already in deterministic order.
	result.Issues = append(issues, result.Issues...)

	return result, nil
}

// streamAnalyze does a single sequential pass over all CSV rows. Every
// row is fed to per-column profilers for type inference, null counting,
// frequency tracking, and min/max. Only MaxSampleRows rows are stored
// in memory for the SampleData output.
func streamAnalyze(ctx context.Context, r io.Reader, delimiter rune, opts *Options) (*SourceContract, error) {
	reader := csvstd.NewReader(r)
	reader.Comma = delimiter
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	// Read the first row to determine header/field names. An empty file
	// and a failing read are different facts (issue #81): only a clean
	// EOF means the file is empty, anything else carries the real error.
	firstRow, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("file is empty")
	}
	if err != nil {
		return nil, fmt.Errorf("read first row: %w", err)
	}

	// Buffer a bounded probe of the following rows so header detection
	// can compare the first row's value classes against the column
	// value classes. A read error during the probe is remembered and
	// surfaced after the buffered rows are processed, preserving the
	// row number reported for parse errors.
	var probe [][]string
	var probeErr error
	for len(probe) < profile.HeaderProbeRows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			probeErr = readErr
			break
		}
		probe = append(probe, record)
	}

	hasHeader, headerGuessed := profile.DetectHeaderWithRowsConfidence(firstRow, probe)

	var fieldNames []string
	maxSampleRows := opts.maxSampleRows()
	maxTracked := opts.maxTracked()

	// Track whether the first row is data (no header) so we can
	// feed it to the profilers and include it in sample data.
	var firstDataRow []string

	if hasHeader {
		fieldNames = firstRow
		if len(fieldNames) > 0 {
			// Defense in depth: strip BOM character if the CSV reader
			// re-introduces it as a zero-width no-break space.
			fieldNames[0] = strings.TrimPrefix(fieldNames[0], "\ufeff")
		}
	} else {
		if len(firstRow) > 0 {
			fieldNames = profile.GenerateFieldNames(len(firstRow))
		}
		firstDataRow = firstRow
	}

	numFields := len(fieldNames)
	stats := newColumnStats(numFields, maxTracked)

	var sampleRows [][]string
	totalRows := 0

	processRow := func(record []string) {
		totalRows++
		stats.observeRow(record)
		if len(sampleRows) < maxSampleRows {
			sampleRows = append(sampleRows, record)
		}
	}

	// If the first row is data, process it, then drain the probe buffer.
	if firstDataRow != nil {
		processRow(firstDataRow)
	}
	for _, record := range probe {
		processRow(record)
	}
	if probeErr != nil {
		return nil, fmt.Errorf("parse error at row %d: %w", totalRows+2, probeErr)
	}

	// Stream all remaining rows.
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("parse error at row %d: %w", totalRows+2, readErr)
		}
		processRow(record)
	}

	topN := opts.topN()
	fields := make([]Field, numFields)
	for i, name := range fieldNames {
		fields[i] = Field{
			Name:     name,
			DataType: stats.types[i],
			Profile:  stats.profilers[i].Finish(topN),
		}
	}

	return &SourceContract{
		HasHeader:  hasHeader,
		TotalRows:  totalRows,
		Fields:     fields,
		SampleData: sampleRows,
		Issues:     collectIssues(headerGuessed, stats, fields),
	}, nil
}

// nullSentinels are the common spellings that mean "no value" but count
// as ordinary text (issue #81). They are matched exactly after trimming
// surrounding whitespace; the empty string is absent because it already
// counts as null. The slice is the canonical iteration order, so issue
// messages never depend on map order.
var nullSentinels = []string{"-", "N/A", "NULL", "n/a", "null"}

// sentinelShareFloor is the share of a column's non-null values that
// must be null sentinels before the column is flagged: below it a
// stray "-" in free text would make every prose column noisy.
const sentinelShareFloor = 0.05

// mixedMajorityFloor is the share a single non-text class needs among
// a text column's non-empty values before the column is reported as
// mixed-majority. It matches the dominance floor used by shape
// signatures: a clear dominant, not a bare plurality.
const mixedMajorityFloor = 0.6

// mixedClassOrder is the canonical order for reporting class splits.
var mixedClassOrder = []profile.DataType{
	profile.TypeNumeric,
	profile.TypeDate,
	profile.TypeTimestamp,
	profile.TypeBoolean,
	profile.TypeText,
}

// columnStats accumulates everything the streaming pass observes per
// column: the existing profilers and type trackers, plus the evidence
// the issues channel reports on (issue #81): ragged rows, sentinel
// counts, and per-class value counts.
type columnStats struct {
	profilers []*profile.ColumnProfiler
	types     []profile.DataType
	// classCounts counts non-empty cell classes per column.
	classCounts []map[profile.DataType]int
	// sentinelCounts counts exact null-sentinel matches per column.
	sentinelCounts []map[string]int
	// shortRows and longRows count ragged rows; droppedCells is the
	// total number of cells lost from rows longer than the schema.
	shortRows, longRows, droppedCells int
}

func newColumnStats(numFields, maxTracked int) *columnStats {
	s := &columnStats{
		profilers:      make([]*profile.ColumnProfiler, numFields),
		types:          make([]profile.DataType, numFields),
		classCounts:    make([]map[profile.DataType]int, numFields),
		sentinelCounts: make([]map[string]int, numFields),
	}
	for i := 0; i < numFields; i++ {
		s.profilers[i] = profile.NewColumnProfiler(maxTracked)
		s.types[i] = profile.TypeEmpty
		s.classCounts[i] = make(map[profile.DataType]int)
		s.sentinelCounts[i] = make(map[string]int)
	}
	return s
}

// observeRow feeds a single row to the per-column trackers. Short rows
// contribute empty cells (nulls) for the missing columns; cells beyond
// the schema width are dropped. Both distortions are counted so the
// contract can disclose them.
func (s *columnStats) observeRow(row []string) {
	numFields := len(s.profilers)
	if len(row) < numFields {
		s.shortRows++
	}
	if len(row) > numFields {
		s.longRows++
		s.droppedCells += len(row) - numFields
	}
	for col := 0; col < numFields; col++ {
		var value string
		if col < len(row) {
			value = row[col]
		}
		s.profilers[col].Observe(value)
		cellType := profile.ClassifyCell(value)
		s.types[col] = profile.MergeTypes(s.types[col], cellType)
		if cellType != profile.TypeEmpty {
			s.classCounts[col][cellType]++
			if trimmed := strings.TrimSpace(value); slices.Contains(nullSentinels, trimmed) {
				s.sentinelCounts[col][trimmed]++
			}
		}
	}
}

// collectIssues renders the streaming pass's evidence into the
// contract's issues channel (issue #81). Order is fixed: the header
// verdict first, then ragged-row facts, then per-column findings in
// schema order, so identical input always yields identical output.
func collectIssues(headerGuessed bool, stats *columnStats, fields []Field) []string {
	var issues []string
	if headerGuessed {
		issues = append(issues, "header detection fell back to the all-text guess: a text header over text columns and headerless text data are indistinguishable, so the first row was assumed to be a header")
	}
	if stats.shortRows > 0 {
		issues = append(issues, fmt.Sprintf("%d rows had fewer than %d columns; missing cells were read as nulls", stats.shortRows, len(fields)))
	}
	if stats.longRows > 0 {
		issues = append(issues, fmt.Sprintf("%d rows had more than %d columns; %d extra cells were dropped", stats.longRows, len(fields), stats.droppedCells))
	}
	for i, field := range fields {
		if msg, ok := sentinelIssue(field, stats.sentinelCounts[i]); ok {
			issues = append(issues, msg)
		}
		if msg, ok := mixedMajorityIssue(field, stats.classCounts[i]); ok {
			issues = append(issues, msg)
		}
	}
	return issues
}

// sentinelIssue reports a column whose non-null values include null
// sentinels above the share floor. The values are NOT reclassified as
// nulls: that would change the contract's semantics silently; the
// issue discloses the fact instead.
func sentinelIssue(field Field, counts map[string]int) (string, bool) {
	total := 0
	var seen []string
	for _, sentinel := range nullSentinels {
		if n := counts[sentinel]; n > 0 {
			total += n
			seen = append(seen, sentinel)
		}
	}
	nonNull := field.Profile.TotalCount - field.Profile.NullCount
	if total == 0 || float64(total) < sentinelShareFloor*float64(nonNull) {
		return "", false
	}
	return fmt.Sprintf("column %q: %d of %d non-null values are null sentinels (%s); they were profiled as ordinary values", field.Name, total, nonNull, strings.Join(seen, ", ")), true
}

// mixedMajorityIssue reports a column that typed text even though a
// clear majority of its non-empty values belong to a single non-text
// class, so consumers can tell true prose from a typed column with
// stray text.
func mixedMajorityIssue(field Field, counts map[profile.DataType]int) (string, bool) {
	if field.DataType != profile.TypeText {
		return "", false
	}
	total := 0
	for _, class := range mixedClassOrder {
		total += counts[class]
	}
	dominant := profile.TypeText
	dominantCount := 0
	for _, class := range mixedClassOrder[:len(mixedClassOrder)-1] {
		if counts[class] > dominantCount {
			dominant = class
			dominantCount = counts[class]
		}
	}
	// A text-typed column always has at least one text value, so total
	// is positive whenever a dominant non-text class exists.
	if dominantCount == 0 || float64(dominantCount)/float64(total) < mixedMajorityFloor {
		return "", false
	}
	// parts is built by walking mixedClassOrder, so the split listing is
	// deterministic without sorting.
	parts := make([]string, 0, len(mixedClassOrder))
	for _, class := range mixedClassOrder {
		if counts[class] > 0 {
			parts = append(parts, fmt.Sprintf("%d percent %s", percent(counts[class], total), class))
		}
	}
	return fmt.Sprintf("column %q: typed text but %d percent of its %d non-empty values are %s (%s)", field.Name, percent(dominantCount, total), total, dominant, strings.Join(parts, ", ")), true
}

// percent renders a share as a whole-number percentage.
func percent(count, total int) int {
	return int(math.Round(float64(count) / float64(total) * 100))
}
