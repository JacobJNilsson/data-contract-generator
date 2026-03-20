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
	TotalCount     int        `json:"total_count"`
	NullCount      int        `json:"null_count"`
	NullPercentage float64    `json:"null_percentage"`
	MinValue       *string    `json:"min_value"`
	MaxValue       *string    `json:"max_value"`
	TopValues      []TopValue `json:"top_values"`
}

// TopValue pairs a value with how many times it appears in the data.
type TopValue struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// DataType represents the inferred type of a CSV column.
type DataType string

// Supported data types for CSV columns.
const (
	TypeText    DataType = "text"
	TypeNumeric DataType = "numeric"
	TypeDate    DataType = "date"
	TypeEmpty   DataType = "empty"
)

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
