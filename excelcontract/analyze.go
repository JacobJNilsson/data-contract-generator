package excelcontract

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/profile"
	"github.com/xuri/excelize/v2"
)

// AnalyzeFile opens an Excel file and produces a DataContract with one
// SchemaContract per non-empty sheet.
func AnalyzeFile(ctx context.Context, path string, opts *Options) (*contract.DataContract, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func() { _ = f.Close() }()

	return AnalyzeReader(ctx, f, opts)
}

// AnalyzeReader analyzes an Excel workbook from any io.Reader and
// produces a DataContract with one SchemaContract per non-empty sheet.
func AnalyzeReader(ctx context.Context, r io.Reader, opts *Options) (*contract.DataContract, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("open workbook: %w", err)
	}
	defer func() { _ = f.Close() }()

	sheets := f.GetSheetList()
	var schemas []contract.SchemaContract
	for _, sheet := range sheets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		sc, err := analyzeSheet(f, sheet, opts)
		if err != nil {
			return nil, fmt.Errorf("sheet %q: %w", sheet, err)
		}
		if sc != nil {
			schemas = append(schemas, *sc)
		}
	}

	if len(schemas) == 0 {
		return nil, fmt.Errorf("all sheets are empty")
	}

	return &contract.DataContract{
		ContractType: "source",
		ID:           "excel",
		Schemas:      schemas,
		Metadata: map[string]any{
			"source_format": "xlsx",
			"sheet_count":   len(sheets),
		},
	}, nil
}

// analyzeSheet analyzes a single sheet and returns a SchemaContract, or
// nil if the sheet is empty.
func analyzeSheet(f *excelize.File, sheet string, opts *Options) (*contract.SchemaContract, error) {
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	region := detectDataRegion(f, sheet, rows)
	if region == nil {
		return nil, nil
	}

	// Determine field names from the header row.
	headerRow := rows[region.headerRow]
	hasHeader := profile.DetectHeader(headerRow)

	var fieldNames []string
	if hasHeader {
		fieldNames = headerRow
	} else {
		fieldNames = profile.GenerateFieldNames(len(headerRow))
		// When there's no header, the header row IS data, so start from it.
		region.startRow = region.headerRow
	}

	numFields := len(fieldNames)
	maxTracked := opts.maxTracked()
	maxSampleRows := opts.maxSampleRows()
	topN := opts.topN()

	// Initialize per-column profilers and type trackers.
	profilers := make([]*profile.ColumnProfiler, numFields)
	colTypes := make([]profile.DataType, numFields)
	for i := range profilers {
		profilers[i] = profile.NewColumnProfiler(maxTracked)
		colTypes[i] = profile.TypeEmpty
	}

	var sampleData [][]string
	totalRows := 0

	// Stream data rows.
	for i := region.startRow; i < len(rows); i++ {
		row := rows[i]
		if isEmptyRow(row) {
			continue
		}
		totalRows++

		// Feed each cell to profilers and type trackers.
		for col := 0; col < numFields; col++ {
			var value string
			if col < len(row) {
				value = row[col]
			}
			profilers[col].Observe(value)
			cellType := profile.ClassifyCell(value)
			colTypes[col] = profile.MergeTypes(colTypes[col], cellType)
		}

		if len(sampleData) < maxSampleRows {
			sample := make([]string, numFields)
			for col := 0; col < numFields; col++ {
				if col < len(row) {
					sample[col] = row[col]
				}
			}
			sampleData = append(sampleData, sample)
		}
	}

	if totalRows == 0 {
		return nil, nil
	}

	// Build field definitions.
	fields := make([]contract.FieldDefinition, numFields)
	for i, name := range fieldNames {
		fp := profilers[i].Finish(topN)
		fields[i] = contract.FieldDefinition{
			Name:     name,
			DataType: string(colTypes[i]),
			Nullable: fp.NullCount > 0,
			Profile: &contract.FieldProfile{
				NullCount:      fp.NullCount,
				NullPercentage: fp.NullPercentage,
				DistinctCount:  fp.DistinctCount,
				MinValue:       fp.MinValue,
				MaxValue:       fp.MaxValue,
				TopValues:      fp.TopValues,
				SampleSize:     fp.TotalCount,
			},
		}
	}

	rowCount := int64(totalRows)
	return &contract.SchemaContract{
		Name:       sheet,
		RowCount:   &rowCount,
		Fields:     fields,
		SampleData: sampleData,
		ValidationRules: contract.ValidationRules{
			RequiredFields: requiredFields(fields),
		},
	}, nil
}

// requiredFields returns field names where all values are non-null.
func requiredFields(fields []contract.FieldDefinition) []string {
	var required []string
	for _, f := range fields {
		if !f.Nullable {
			required = append(required, f.Name)
		}
	}
	return required
}
