// Package profile provides shared type classification, column profiling,
// and header detection used by file-based source analyzers (CSV, Excel).
package profile

import "github.com/JacobJNilsson/data-contract-generator/contract"

// DataType represents the inferred type of a column.
type DataType string

// Supported data types for file-based columns.
//
// TypeBoolean covers exactly the literals "true" and "false",
// case-insensitive. TypeTimestamp covers ISO 8601 date-times with a 'T'
// or space separator, optional fractional seconds, and an optional
// zone. See IsBoolean and IsTimestamp for the full vocabulary
// decisions (issue #80).
const (
	TypeText      DataType = "text"
	TypeNumeric   DataType = "numeric"
	TypeDate      DataType = "date"
	TypeTimestamp DataType = "timestamp"
	TypeBoolean   DataType = "boolean"
	TypeEmpty     DataType = "empty"
)

// FieldProfile contains statistical observations about a column's values.
// TotalCount tracks the number of rows observed (used internally by
// profilers). Callers map this to contract.FieldProfile.SampleSize.
type FieldProfile struct {
	TotalCount     int     `json:"total_count"`
	NullCount      int     `json:"null_count"`
	NullPercentage float64 `json:"null_percentage"`

	// DistinctCount is the number of distinct non-null values tracked.
	// When DistinctCountCapped is true, tracking stopped at the
	// MaxTracked limit and DistinctCount means "at least this many".
	DistinctCount int `json:"distinct_count"`

	// DistinctCountCapped is true when the column had more distinct
	// values than MaxTracked, so DistinctCount is a floor rather than
	// an exact count.
	DistinctCountCapped bool `json:"distinct_count_capped"`

	// MinValue and MaxValue are the extreme values observed, compared
	// numerically when every value is numeric (integer-exact within
	// int64) and chronologically when every value is an unambiguous ISO
	// date or timestamp. Everything else compares lexicographically; in
	// particular, ambiguous slash-form dates (03/04/2026 could be March
	// 4 or April 3) keep lexicographic ordering, which is not
	// chronological for such columns (issue #80).
	MinValue *string `json:"min_value"`
	MaxValue *string `json:"max_value"`

	// TopValues lists the most frequent tracked values. When
	// DistinctCountCapped is true these can be stale: values first seen
	// after the cap was reached are never counted, so a genuinely
	// frequent late-arriving value may be missing and the reported
	// counts only cover tracked values.
	TopValues []contract.TopValue `json:"top_values"`
	Shape     ShapeSignature      `json:"shape"`
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
