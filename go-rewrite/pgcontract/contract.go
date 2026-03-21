// Package pgcontract analyzes PostgreSQL database schemas and produces
// data contracts describing table structures, types, constraints, and
// data profiles.
package pgcontract

// Options controls the analysis behaviour.
type Options struct {
	// Schema is the PostgreSQL schema to analyze (default: "public").
	Schema string

	// IncludeComments includes PostgreSQL column comments as field descriptions.
	IncludeComments bool

	// SampleSize is the maximum number of rows to sample per table for
	// profiling. Zero means use the default (10000). Rows are fetched
	// in batches of BatchSize to bound memory.
	SampleSize int

	// BatchSize is the number of rows fetched per batch during profiling.
	// Zero means use the default (1000).
	BatchSize int

	// MaxSampleRows is the maximum number of rows to include in
	// SampleData. Zero means use the default (5).
	MaxSampleRows int

	// TopN is the number of most frequent values to report per column.
	// Zero means use the default (5).
	TopN int
}

func (o *Options) schema() string {
	if o == nil || o.Schema == "" {
		return "public"
	}
	return o.Schema
}

func (o *Options) includeComments() bool {
	return o != nil && o.IncludeComments
}

func (o *Options) sampleSize() int {
	if o == nil || o.SampleSize <= 0 {
		return 10000
	}
	return o.SampleSize
}

func (o *Options) batchSize() int {
	if o == nil || o.BatchSize <= 0 {
		return 1000
	}
	return o.BatchSize
}

func (o *Options) maxSampleRows() int {
	if o == nil || o.MaxSampleRows <= 0 {
		return 5
	}
	return o.MaxSampleRows
}

func (o *Options) topN() int {
	if o == nil || o.TopN <= 0 {
		return 5
	}
	return o.TopN
}
