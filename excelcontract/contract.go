// Package excelcontract analyzes Excel (.xlsx) files and produces data
// contracts describing the structure, types, and data quality of each
// sheet. Multi-sheet workbooks produce one SchemaContract per non-empty
// sheet.
package excelcontract

// Options controls the analysis behavior. A nil Options uses defaults.
type Options struct {
	// TopN is the number of most frequent values to include per field.
	// Zero means use the default (5).
	TopN int

	// MaxTracked is the maximum number of distinct values tracked per
	// column for frequency counting. Zero means use the default (10000).
	MaxTracked int

	// MaxSampleRows is the maximum number of rows to include in
	// SampleData. Zero means use the default (5).
	MaxSampleRows int

	// MaxUnzipBytes caps the EXPANDED size excelize may inflate a workbook
	// to while unzipping it (excelize.Options.UnzipSizeLimit). A workbook is
	// a zip archive, so a hostile file can compress to a small download and
	// still expand to gigabytes in memory (a zip bomb). Zero means use the
	// default (1 GiB), a size no legitimate client workbook this analyzer
	// targets should approach, chosen deliberately far below excelize's own
	// 16 GiB default so a bomb is refused long before it exhausts worker
	// memory.
	MaxUnzipBytes int64
}

func (o *Options) topN() int {
	if o == nil || o.TopN <= 0 {
		return 5
	}
	return o.TopN
}

func (o *Options) maxTracked() int {
	if o == nil || o.MaxTracked <= 0 {
		return 10000
	}
	return o.MaxTracked
}

func (o *Options) maxSampleRows() int {
	if o == nil || o.MaxSampleRows <= 0 {
		return 5
	}
	return o.MaxSampleRows
}

// maxUnzipBytes is documented on MaxUnzipBytes: 1 GiB when unset.
func (o *Options) maxUnzipBytes() int64 {
	if o == nil || o.MaxUnzipBytes <= 0 {
		return 1 << 30
	}
	return o.MaxUnzipBytes
}
