// Package profile provides shared type classification, column profiling,
// and header detection used by file-based source analyzers (CSV, Excel).
package profile

import "github.com/JacobJNilsson/data-contract-generator/contract"

// DataType represents the inferred type of a column.
type DataType string

// Supported data types for file-based columns.
const (
	TypeText    DataType = "text"
	TypeNumeric DataType = "numeric"
	TypeDate    DataType = "date"
	TypeEmpty   DataType = "empty"
)

// FieldProfile contains statistical observations about a column's values.
// TotalCount tracks the number of rows observed (used internally by
// profilers). Callers map this to contract.FieldProfile.SampleSize.
type FieldProfile struct {
	TotalCount     int                 `json:"total_count"`
	NullCount      int                 `json:"null_count"`
	NullPercentage float64             `json:"null_percentage"`
	DistinctCount  int                 `json:"distinct_count"`
	MinValue       *string             `json:"min_value"`
	MaxValue       *string             `json:"max_value"`
	TopValues      []contract.TopValue `json:"top_values"`
}

// Options controls the analysis behavior. A nil Options uses defaults.
type Options struct {
	// TopN is the number of most frequent values to include per field.
	// Zero means use the default (5).
	TopN int

	// MaxTracked is the maximum number of distinct values tracked per
	// column for frequency counting. Once exceeded, new values are
	// ignored but existing counters keep incrementing. Zero means use
	// the default (10000).
	MaxTracked int

	// MaxSampleRows is the maximum number of rows to include in
	// SampleData. Zero means use the default (5).
	MaxSampleRows int
}

// GetTopN returns the configured TopN or the default (5).
func (o *Options) GetTopN() int {
	if o == nil || o.TopN <= 0 {
		return 5
	}
	return o.TopN
}

// GetMaxTracked returns the configured MaxTracked or the default (10000).
func (o *Options) GetMaxTracked() int {
	if o == nil || o.MaxTracked <= 0 {
		return 10000
	}
	return o.MaxTracked
}

// GetMaxSampleRows returns the configured MaxSampleRows or the default (5).
func (o *Options) GetMaxSampleRows() int {
	if o == nil || o.MaxSampleRows <= 0 {
		return 5
	}
	return o.MaxSampleRows
}
