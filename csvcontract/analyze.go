package csvcontract

import (
	"bytes"
	"context"
	csvstd "encoding/csv"
	"fmt"
	"io"
	"os"
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
//  2. Stream: single sequential pass through all rows. The first row is
//     used for header detection. Every data row is fed to per-column
//     profilers (type inference, null counting, frequency tracking,
//     min/max). Up to MaxSampleRows are kept for the SampleData field.
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
	if hasBOM {
		delimBuf = bytes.TrimPrefix(delimBuf, utf8BOM)
	}
	if encoding == "latin-1" {
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

// streamAnalyze does a single sequential pass over all CSV rows. Every
// row is fed to per-column profilers for type inference, null counting,
// frequency tracking, and min/max. Only MaxSampleRows rows are stored
// in memory for the SampleData output.
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

	hasHeader := profile.DetectHeader(firstRow)

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

	// Initialize per-column profilers and type trackers.
	profilers := make([]*profile.ColumnProfiler, numFields)
	colTypes := make([]profile.DataType, numFields)
	for i := range profilers {
		profilers[i] = profile.NewColumnProfiler(maxTracked)
		colTypes[i] = profile.TypeEmpty
	}

	// If the first row is data, process it.
	var sampleRows [][]string
	totalRows := 0

	if firstDataRow != nil {
		totalRows++
		observeRow(firstDataRow, profilers, colTypes, numFields)
		if len(sampleRows) < maxSampleRows {
			sampleRows = append(sampleRows, firstDataRow)
		}
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
		totalRows++
		observeRow(record, profilers, colTypes, numFields)
		if len(sampleRows) < maxSampleRows {
			sampleRows = append(sampleRows, record)
		}
	}

	topN := opts.topN()
	fields := make([]Field, numFields)
	for i, name := range fieldNames {
		fields[i] = Field{
			Name:     name,
			DataType: colTypes[i],
			Profile:  profilers[i].Finish(topN),
		}
	}

	return &SourceContract{
		HasHeader:  hasHeader,
		TotalRows:  totalRows,
		Fields:     fields,
		SampleData: sampleRows,
	}, nil
}

// observeRow feeds a single row to per-column profilers and type trackers.
func observeRow(row []string, profilers []*profile.ColumnProfiler, colTypes []profile.DataType, numFields int) {
	for col := 0; col < numFields; col++ {
		var value string
		if col < len(row) {
			value = row[col]
		}
		profilers[col].Observe(value)
		cellType := profile.ClassifyCell(value)
		colTypes[col] = profile.MergeTypes(colTypes[col], cellType)
	}
}
