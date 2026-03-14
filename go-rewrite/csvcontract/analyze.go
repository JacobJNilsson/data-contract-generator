package csvcontract

import (
	"bytes"
	"context"
	csvstd "encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
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
//  2. Stream: single sequential pass through the CSV reader. The first
//     row is used for header detection. Up to SampleSize data rows are
//     kept in memory for type inference and profiling. All remaining
//     rows are counted but not stored.
//
// Peak memory is proportional to SampleSize (default 1000 rows), not
// the file size. A 2GB file uses a few MB of RAM.
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

	// Detect delimiter from the sniff buffer (already decoded for UTF-8,
	// needs decoding for Latin-1).
	delimBuf := sniffBuf
	if hasBOM {
		delimBuf = bytes.TrimPrefix(delimBuf, utf8BOM)
	}
	if encoding == "latin-1" {
		hasBOM = false
		delimBuf = decodeLatin1(delimBuf)
	}
	delimiter := detectDelimiterFromBytes(delimBuf)

	// Seek back to start for the full streaming pass.
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to start: %w", err)
	}

	// Phase 2: stream through the CSV content.
	var csvReader io.Reader = rs
	if hasBOM {
		// Skip the 3-byte BOM before feeding to the CSV parser.
		bomBuf := make([]byte, len(utf8BOM))
		if _, err := io.ReadFull(rs, bomBuf); err != nil {
			return nil, fmt.Errorf("skip BOM: %w", err)
		}
	}
	if encoding == "latin-1" {
		csvReader = newLatin1Reader(csvReader)
	}

	result, err := streamAnalyze(ctx, csvReader, delimiter, opts)
	if err != nil {
		return nil, err
	}

	var issues []string
	if hasBOM {
		issues = append(issues, "UTF-8 BOM detected and stripped")
	}

	result.SourceFormat = "csv"
	result.Encoding = encoding
	result.Delimiter = string(delimiter)
	result.Issues = issues

	return result, nil
}

// streamAnalyze does a single sequential pass over the CSV content,
// collecting header, sample rows, type inference, profiling, and total
// row count. Only SampleSize rows are held in memory.
func streamAnalyze(ctx context.Context, r io.Reader, delimiter rune, opts *Options) (*SourceContract, error) {
	reader := csvstd.NewReader(r)
	reader.Comma = delimiter
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	// Read the first row to determine header/field names.
	firstRow, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("file is empty")
	}

	hasHeader := detectHeader(firstRow)

	var fieldNames []string
	var sampleRows [][]string

	sampleSize := opts.sampleSize()

	if hasHeader {
		fieldNames = firstRow
		if len(fieldNames) > 0 {
			// Defense in depth: strip BOM character if the CSV reader
			// re-introduces it as a zero-width no-break space.
			fieldNames[0] = strings.TrimPrefix(fieldNames[0], "\ufeff")
		}
	} else {
		if len(firstRow) > 0 {
			fieldNames = generateFieldNames(len(firstRow))
		}
		// First row is data, not a header -- include it in the sample.
		sampleRows = append(sampleRows, firstRow)
	}

	// Stream remaining rows: keep up to sampleSize in memory, count the rest.
	totalRows := len(sampleRows)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		record, readErr := reader.Read()
		if readErr != nil {
			break
		}
		totalRows++
		if len(sampleRows) < sampleSize {
			sampleRows = append(sampleRows, record)
		}
	}

	numFields := len(fieldNames)

	// Use the sample rows for type inference and profiling.
	columnTypes := inferColumnTypes(sampleRows, numFields)

	fields := make([]Field, numFields)
	for i, name := range fieldNames {
		colValues := extractColumn(sampleRows, i)
		fields[i] = Field{
			Name:     name,
			DataType: columnTypes[i],
			Profile:  profileColumn(colValues, opts.maxSampleValues()),
		}
	}

	maxSampleRows := opts.maxSampleRows()
	sampleData := sampleRows
	if len(sampleData) > maxSampleRows {
		sampleData = sampleData[:maxSampleRows]
	}

	return &SourceContract{
		HasHeader:  hasHeader,
		TotalRows:  totalRows,
		Fields:     fields,
		SampleData: sampleData,
	}, nil
}

// extractColumn collects all values for a single column index from the
// data rows. If a row is shorter than the index, an empty string is used.
func extractColumn(rows [][]string, col int) []string {
	values := make([]string, len(rows))
	for i, row := range rows {
		if col < len(row) {
			values[i] = row[col]
		}
	}
	return values
}
