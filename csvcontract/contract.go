// Package csvcontract analyzes CSV files and produces source contracts
// describing their structure, encoding, schema, and data quality.
package csvcontract

import "github.com/JacobJNilsson/data-contract-generator/profile"

// SourceContract is the complete analysis result for a CSV file.
type SourceContract struct {
	SourceFormat string     `json:"source_format"`
	SourcePath   string     `json:"source_path"`
	Encoding     string     `json:"encoding"`
	Delimiter    string     `json:"delimiter"`
	HasHeader    bool       `json:"has_header"`
	TotalRows    int        `json:"total_rows"`
	Fields       []Field    `json:"fields"`
	SampleData   [][]string `json:"sample_data"`
	Issues       []string   `json:"issues"`
}

// Field describes a single column in the CSV file.
type Field struct {
	Name     string               `json:"name"`
	DataType profile.DataType     `json:"data_type"`
	Profile  profile.FieldProfile `json:"profile"`
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
