package csvcontract

import (
	"bytes"
	"context"
	csvstd "encoding/csv"
	"fmt"
	"os"
	"strings"
)

// AnalyzeFile reads a CSV file and produces a SourceContract describing
// its structure, encoding, schema, and data quality.
func AnalyzeFile(ctx context.Context, path string, opts *Options) (*SourceContract, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	encoding, hasBOM := detectEncodingFromBytes(raw)

	// Normalize to UTF-8 for all subsequent processing.
	// Only strip BOM for UTF-8 files. A UTF-8 BOM (EF BB BF) in a
	// Latin-1 file would be three printable characters, not a BOM.
	content := raw
	if encoding == "latin-1" {
		hasBOM = false
		content = decodeLatin1(content)
	} else if hasBOM {
		content = bytes.TrimPrefix(content, utf8BOM)
	}

	delimiter := detectDelimiterFromBytes(content)

	result, err := parseAndAnalyze(content, delimiter, opts)
	if err != nil {
		return nil, err
	}

	var issues []string
	if hasBOM {
		issues = append(issues, "UTF-8 BOM detected and stripped")
	}

	result.SourceFormat = "csv"
	result.SourcePath = path
	result.Encoding = encoding
	result.Delimiter = string(delimiter)
	result.Issues = issues

	return result, nil
}

// parseAndAnalyze does a single pass over the CSV content, collecting
// all information needed for the contract: header detection, row counting,
// type inference, and per-column profiling.
func parseAndAnalyze(content []byte, delimiter rune, opts *Options) (*SourceContract, error) {
	allRows := readAllRows(content, delimiter)
	if len(allRows) == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	hasHeader := detectHeader(allRows[0])

	var fieldNames []string
	var dataRows [][]string

	if hasHeader {
		fieldNames = allRows[0]
		if len(fieldNames) > 0 {
			// Defense in depth: the BOM was already stripped from raw bytes
			// in AnalyzeFile, but some CSV readers re-introduce the BOM
			// character (U+FEFF) as a zero-width no-break space. Strip it
			// if present to keep field names clean.
			fieldNames[0] = strings.TrimPrefix(fieldNames[0], "\ufeff")
		}
		dataRows = allRows[1:]
	} else {
		if len(allRows[0]) > 0 {
			fieldNames = generateFieldNames(len(allRows[0]))
		}
		dataRows = allRows
	}

	numFields := len(fieldNames)
	sampleSize := opts.sampleSize()

	// Use at most sampleSize rows for type inference and profiling.
	analysisRows := dataRows
	if len(analysisRows) > sampleSize {
		analysisRows = analysisRows[:sampleSize]
	}

	columnTypes := inferColumnTypes(analysisRows, numFields)

	fields := make([]Field, numFields)
	for i, name := range fieldNames {
		colValues := extractColumn(analysisRows, i)
		fields[i] = Field{
			Name:     name,
			DataType: columnTypes[i],
			Profile:  profileColumn(colValues, opts.maxSampleValues()),
		}
	}

	maxSampleRows := opts.maxSampleRows()
	sampleData := dataRows
	if len(sampleData) > maxSampleRows {
		sampleData = sampleData[:maxSampleRows]
	}

	return &SourceContract{
		HasHeader:  hasHeader,
		TotalRows:  len(dataRows),
		Fields:     fields,
		SampleData: sampleData,
	}, nil
}

// readAllRows parses all CSV records from content. With LazyQuotes and
// no field count constraint, parse errors are extremely unlikely, but
// we handle them by stopping and returning what was successfully read.
func readAllRows(content []byte, delimiter rune) [][]string {
	reader := csvstd.NewReader(bytes.NewReader(content))
	reader.Comma = delimiter
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	// ReadAll is safe here: with LazyQuotes=true and FieldsPerRecord=-1,
	// the only error it returns is from the underlying io.Reader, which
	// for bytes.Reader is always nil.
	rows, _ := reader.ReadAll()
	return rows
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
