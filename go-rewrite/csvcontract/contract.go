// Package csvcontract analyzes CSV files and produces source contracts
// describing their structure, encoding, schema, and data quality.
package csvcontract

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
	Name     string       `json:"name"`
	DataType DataType     `json:"data_type"`
	Profile  FieldProfile `json:"profile"`
}

// FieldProfile contains statistical observations about a column's values.
type FieldProfile struct {
	NullCount      int      `json:"null_count"`
	NullPercentage float64  `json:"null_percentage"`
	DistinctCount  int      `json:"distinct_count"`
	MinValue       *string  `json:"min_value"`
	MaxValue       *string  `json:"max_value"`
	SampleValues   []string `json:"sample_values"`
}

// DataType represents the inferred type of a CSV column.
type DataType string

const (
	TypeText    DataType = "text"
	TypeNumeric DataType = "numeric"
	TypeDate    DataType = "date"
	TypeEmpty   DataType = "empty"
)

// Options controls the analysis behavior. A nil Options uses defaults.
type Options struct {
	// SampleSize is the maximum number of data rows to read for type
	// inference and profiling. Zero means use the default (1000).
	SampleSize int

	// MaxSampleValues is the maximum number of distinct sample values
	// to include per field. Zero means use the default (5).
	MaxSampleValues int

	// MaxSampleRows is the maximum number of rows to include in
	// SampleData. Zero means use the default (5).
	MaxSampleRows int
}

func (o *Options) sampleSize() int {
	if o == nil || o.SampleSize <= 0 {
		return 1000
	}
	return o.SampleSize
}

func (o *Options) maxSampleValues() int {
	if o == nil || o.MaxSampleValues <= 0 {
		return 5
	}
	return o.MaxSampleValues
}

func (o *Options) maxSampleRows() int {
	if o == nil || o.MaxSampleRows <= 0 {
		return 5
	}
	return o.MaxSampleRows
}
