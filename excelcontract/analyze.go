package excelcontract

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/profile"
	"github.com/xuri/excelize/v2"
)

// AnalyzeFile opens an Excel file and produces a DataContract with one
// SchemaContract per detected table (XL-1: a sheet is a canvas, not a
// schema; a sheet with N tables fans into N schemas).
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
// produces a DataContract with one SchemaContract per detected table.
// Single-table sheets keep the bare sheet name as the schema name;
// multi-table sheets name their tables "<sheet>#<ordinal>" in row-major
// anchor order (XL-2). The sheet name always rides Namespace (XL-6).
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
	sheetIssues := map[string]any{}
	for _, sheet := range sheets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		scs, issues, err := analyzeSheet(f, sheet, opts)
		if err != nil {
			return nil, fmt.Errorf("sheet %q: %w", sheet, err)
		}
		schemas = append(schemas, scs...)
		if len(issues) > 0 {
			sheetIssues[sheet] = issues
		}
	}

	if len(schemas) == 0 {
		return nil, errors.New("no tables detected in any sheet")
	}
	if err := refuseLocatorCollision(schemas); err != nil {
		return nil, err
	}

	md := map[string]any{
		"source_format": "xlsx",
		"sheet_count":   len(sheets),
	}
	// Sheet-level issues (stray cells, empty declared tables, dropped
	// header-only tables) belong to the sheet, not to any one table's
	// schema; they ride the contract metadata so nothing the analyzer
	// saw silently disappears, even from a sheet that emitted no schema.
	if len(sheetIssues) > 0 {
		md["sheet_issues"] = sheetIssues
	}

	return &contract.DataContract{
		ContractType: "source",
		ID:           "excel",
		Schemas:      schemas,
		Metadata:     md,
	}, nil
}

// refuseLocatorCollision fails the analysis when two schemas share a
// name. A sheet literally named "Data#2" beside a multi-table sheet
// "Data" would silently alias two different tables into one cache
// identity; that is the catastrophic false positive the fingerprint
// spec forbids, so it is a loud error instead.
func refuseLocatorCollision(schemas []contract.SchemaContract) error {
	seen := make(map[string]bool, len(schemas))
	for _, sc := range schemas {
		if seen[sc.Name] {
			return fmt.Errorf("locator collision: two tables share the name %q (a sheet name collides with another sheet's table locator); rename the sheet", sc.Name)
		}
		seen[sc.Name] = true
	}
	return nil
}

// analyzeSheet segments one sheet into tables and builds one schema per
// table that carries data. It returns the schemas plus the sheet-level
// issues (strays, empty or header-only tables).
func analyzeSheet(f *excelize.File, sheet string, opts *Options) ([]contract.SchemaContract, []string, error) {
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, nil, fmt.Errorf("read rows: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil, nil
	}

	seg := SegmentSheet(rows, DeclaredTables(f, sheet))
	issues := seg.Issues

	// Hidden sheets are analyzed like visible ones (XL-L3): skipping
	// them would make identity depend on presentation state. The flag
	// rides the metadata so the planner can see it.
	visible, visErr := f.GetSheetVisible(sheet)
	hidden := visErr == nil && !visible

	var schemas []contract.SchemaContract
	for ordinal, t := range seg.Tables {
		sc := buildTableSchema(rows, t, opts)
		if sc == nil {
			issues = append(issues, fmt.Sprintf("table at %s has a header but no data rows; no schema was emitted for it", cellName(t.StartRow, t.StartCol)))
			continue
		}
		// XL-2: ordinals cover every detected table, so a table that
		// emitted no schema still keeps its neighbours' ordinals stable.
		sc.Name = sheet
		if len(seg.Tables) > 1 {
			sc.Name = fmt.Sprintf("%s#%d", sheet, ordinal+1)
		}
		sc.Namespace = sheet
		sc.Metadata = tableMetadata(t, hidden)
		schemas = append(schemas, *sc)
	}
	return schemas, issues, nil
}

// DeclaredTables reads a sheet's Excel Table objects into the segmenter's
// declared form (XL-3): the exported form of the read this package's own
// analysis runs, so a caller that re-segments the SAME sheet later (the
// orchestrator's reconciliation source pass, XL-14) reads the identical
// declared bounds instead of a second, hand-copied implementation that
// could drift from this one. A read failure yields none: the cells then
// segment heuristically, which loses author intent but never data.
func DeclaredTables(f *excelize.File, sheet string) []DeclaredTable {
	tables, err := f.GetTables(sheet)
	if err != nil {
		return nil
	}
	out := make([]DeclaredTable, 0, len(tables))
	for _, t := range tables {
		out = append(out, DeclaredTable{
			Name:      t.Name,
			Range:     t.Range,
			HasHeader: t.ShowHeaderRow == nil || *t.ShowHeaderRow,
		})
	}
	return out
}

// tableMetadata renders a Segment as the XL-6 schema metadata. Position
// is advice for the planner and the authoring agent, never identity:
// the fingerprint reads none of these keys except through the parse
// profile's has_header.
func tableMetadata(t Segment, hidden bool) map[string]any {
	md := map[string]any{
		"anchor":     cellName(t.StartRow, t.StartCol),
		"range":      rangeRef(t.StartRow, t.StartCol, t.EndRow, t.EndCol),
		"detection":  t.Detection,
		"has_header": t.HasHeader,
	}
	if t.TableName != "" {
		md["table_name"] = t.TableName
	}
	if t.Caption != "" {
		md["caption"] = t.Caption
	}
	if hidden {
		md["hidden"] = true
	}
	return md
}

// buildTableSchema profiles one table region into a SchemaContract.
// It returns nil for a table with a header and no data rows; the caller
// records that loudly.
func buildTableSchema(rows [][]string, t Segment, opts *Options) *contract.SchemaContract {
	numFields := t.EndCol - t.StartCol + 1
	var fieldNames []string
	issues := t.Issues
	dataStart := t.StartRow
	if t.HasHeader {
		fieldNames = cellRange(rows, t.StartRow, t.StartCol, t.EndCol)
		dataStart = t.StartRow + 1
		// A blank header cell (a merged header's sibling column, XL-L2)
		// gets a synthesized positional name, loudly: a nameless column
		// could never be mapped or dropped by name downstream.
		synthesized := profile.GenerateFieldNames(numFields)
		for i, name := range fieldNames {
			if strings.TrimSpace(name) != "" {
				continue
			}
			fieldNames[i] = synthesized[i]
			issues = append(issues, fmt.Sprintf("header cell %s is blank; its column was assigned the synthesized name %q", cellName(t.StartRow, t.StartCol+i), synthesized[i]))
		}
	} else {
		fieldNames = profile.GenerateFieldNames(numFields)
	}

	maxTracked := opts.maxTracked()
	maxSampleRows := opts.maxSampleRows()
	topN := opts.topN()

	profilers := make([]*profile.ColumnProfiler, numFields)
	colTypes := make([]profile.DataType, numFields)
	for i := range profilers {
		profilers[i] = profile.NewColumnProfiler(maxTracked)
		colTypes[i] = profile.TypeEmpty
	}

	var sampleData [][]string
	totalRows := 0
	for r := dataStart; r <= t.EndRow; r++ {
		row := cellRange(rows, r, t.StartCol, t.EndCol)
		// A row blank inside the table's bounds can occur in a sparse
		// declared Table-object range; heuristic bands never contain one.
		if isEmptyRow(row) {
			continue
		}
		totalRows++

		for col, value := range row {
			profilers[col].Observe(value)
			colTypes[col] = profile.MergeTypes(colTypes[col], profile.ClassifyCell(value))
		}

		if len(sampleData) < maxSampleRows {
			sampleData = append(sampleData, row)
		}
	}

	if totalRows == 0 {
		return nil
	}

	fields := make([]contract.FieldDefinition, numFields)
	for i, name := range fieldNames {
		fp := profilers[i].Finish(topN)
		fields[i] = contract.FieldDefinition{
			Name:     name,
			DataType: string(colTypes[i]),
			Nullable: fp.NullCount > 0,
			Profile: &contract.FieldProfile{
				NullCount:           fp.NullCount,
				NullPercentage:      fp.NullPercentage,
				DistinctCount:       fp.DistinctCount,
				DistinctCountCapped: fp.DistinctCountCapped,
				MinValue:            fp.MinValue,
				MaxValue:            fp.MaxValue,
				TopValues:           fp.TopValues,
				SampleSize:          fp.TotalCount,
			},
		}
	}

	rowCount := int64(totalRows)
	return &contract.SchemaContract{
		RowCount:   &rowCount,
		Fields:     fields,
		SampleData: sampleData,
		Issues:     issues,
		ValidationRules: contract.ValidationRules{
			RequiredFields: requiredFields(fields),
		},
	}
}

// cellRange extracts row cells over inclusive column bounds, padding a
// short row so every extracted row has the table's width.
func cellRange(rows [][]string, r, startCol, endCol int) []string {
	out := make([]string, endCol-startCol+1)
	if r < 0 || r >= len(rows) {
		return out
	}
	row := rows[r]
	for c := startCol; c <= endCol && c < len(row); c++ {
		out[c-startCol] = row[c]
	}
	return out
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
