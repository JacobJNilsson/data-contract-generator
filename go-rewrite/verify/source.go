package verify

import (
	"encoding/json"
	"fmt"
	"math"
)

// sourceContract is the superset of fields across CSV and JSON source contracts.
// We unmarshal into this to validate structurally, then check semantics.
type sourceContract struct {
	SourceFormat string        `json:"source_format"`
	Encoding     string        `json:"encoding"`
	Delimiter    string        `json:"delimiter"`
	HasHeader    *bool         `json:"has_header"`
	TotalRows    int           `json:"total_rows"`
	Fields       []sourceField `json:"fields"`
	SampleData   [][]any       `json:"sample_data"`
	Issues       []string      `json:"issues"`
}

type sourceField struct {
	Name     string       `json:"name"`
	DataType string       `json:"data_type"`
	Profile  fieldProfile `json:"profile"`
}

type fieldProfile struct {
	TotalCount     int        `json:"total_count"`
	NullCount      int        `json:"null_count"`
	NullPercentage float64    `json:"null_percentage"`
	MinValue       *string    `json:"min_value"`
	MaxValue       *string    `json:"max_value"`
	TopValues      []topValue `json:"top_values"`
}

type topValue struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// Valid source formats.
var validSourceFormats = map[string]bool{
	"csv": true, "json": true, "ndjson": true,
}

// Valid data types for source fields.
var validSourceDataTypes = map[string]bool{
	"text": true, "numeric": true, "date": true, "boolean": true,
	"object": true, "array": true, "null": true, "empty": true,
}

// verifySource validates a source contract (CSV, JSON, NDJSON).
func verifySource(data []byte) []string {
	var sc sourceContract
	if err := json.Unmarshal(data, &sc); err != nil {
		return []string{fmt.Sprintf("failed to parse source contract: %s", err)}
	}

	var issues []string

	// --- structural checks ---

	if sc.SourceFormat == "" {
		issues = append(issues, "missing source_format")
	} else if !validSourceFormats[sc.SourceFormat] {
		issues = append(issues, fmt.Sprintf("unknown source_format: %q", sc.SourceFormat))
	}

	if sc.SourceFormat == "csv" {
		if sc.Delimiter == "" {
			issues = append(issues, "CSV contract missing delimiter")
		}
		if sc.HasHeader == nil {
			issues = append(issues, "CSV contract missing has_header")
		}
	}

	if sc.TotalRows < 0 {
		issues = append(issues, fmt.Sprintf("total_rows is negative: %d", sc.TotalRows))
	}

	if len(sc.Fields) == 0 && sc.TotalRows > 0 {
		issues = append(issues, "total_rows > 0 but no fields defined")
	}

	// Validate each field.
	fieldNames := make(map[string]bool, len(sc.Fields))
	for i, f := range sc.Fields {
		prefix := fmt.Sprintf("fields[%d]", i)

		switch {
		case f.Name == "":
			issues = append(issues, prefix+": missing name")
		case fieldNames[f.Name]:
			issues = append(issues, fmt.Sprintf("%s: duplicate field name %q", prefix, f.Name))
		default:
			fieldNames[f.Name] = true
		}

		if f.DataType == "" {
			issues = append(issues, prefix+": missing data_type")
		} else if !validSourceDataTypes[f.DataType] {
			issues = append(issues, fmt.Sprintf("%s: unknown data_type %q", prefix, f.DataType))
		}

		issues = append(issues, verifyFieldProfile(prefix, f.Profile)...)
	}

	return issues
}

// verifyFieldProfile checks a field's profile for numeric bounds and
// internal consistency.
func verifyFieldProfile(prefix string, p fieldProfile) []string {
	var issues []string

	if p.TotalCount < 0 {
		issues = append(issues, fmt.Sprintf("%s.profile: total_count is negative", prefix))
	}
	if p.NullCount < 0 {
		issues = append(issues, fmt.Sprintf("%s.profile: null_count is negative", prefix))
	}
	if p.NullCount > p.TotalCount {
		issues = append(issues, fmt.Sprintf("%s.profile: null_count (%d) > total_count (%d)", prefix, p.NullCount, p.TotalCount))
	}
	if p.NullPercentage < 0 || p.NullPercentage > 100 {
		issues = append(issues, fmt.Sprintf("%s.profile: null_percentage %.2f out of range [0, 100]", prefix, p.NullPercentage))
	}

	// Semantic: null_percentage should be consistent with null_count/total_count.
	if p.TotalCount > 0 {
		expected := float64(p.NullCount) / float64(p.TotalCount) * 100
		if math.Abs(p.NullPercentage-expected) > 1.0 {
			issues = append(issues, fmt.Sprintf(
				"%s.profile: null_percentage %.2f inconsistent with null_count/total_count (expected ~%.2f)",
				prefix, p.NullPercentage, expected))
		}
	}

	// Top values counts should be positive.
	for j, tv := range p.TopValues {
		if tv.Count <= 0 {
			issues = append(issues, fmt.Sprintf("%s.profile.top_values[%d]: count must be positive", prefix, j))
		}
	}

	return issues
}
