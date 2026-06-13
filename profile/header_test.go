package profile

import "testing"

func TestDetectHeader(t *testing.T) {
	tests := []struct {
		name string
		row  []string
		want bool
	}{
		{"text header", []string{"Name", "Age", "City"}, true},
		{"all numeric", []string{"1", "2", "3"}, false},
		{"mixed", []string{"Name", "2", "3"}, true},
		{"empty row", []string{}, false},
		{"all empty", []string{"", "", ""}, false},
		{"numeric with decimals", []string{"1.5", "2.3", "3.7"}, false},
		{"negative numbers", []string{"-1", "-2", "-3"}, false},
	}
	for _, tt := range tests {
		got := DetectHeader(tt.row)
		if got != tt.want {
			t.Errorf("DetectHeader(%s) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDetectHeaderWithRows(t *testing.T) {
	tests := []struct {
		name     string
		firstRow []string
		dataRows [][]string
		want     bool
	}{
		{
			// Exact reproduction from issue #75: every row has the same
			// value classes (identifier, text, numeric, date), so the
			// first row is data, not a header.
			name:     "homogeneous headerless rows",
			firstRow: []string{"P-600", "Kedjespannare K2", "75.50", "2026-06-07"},
			dataRows: [][]string{
				{"P-601", "Drevsats 13T", "189.00", "2026-06-07"},
				{"P-602", "Kedjelas X", "45.25", "2026-06-08"},
				{"P-603", "Bromsok F4", "320.00", "2026-06-08"},
			},
			want: false,
		},
		{
			name:     "numeric first-row cell in numeric column",
			firstRow: []string{"widget", "75.50"},
			dataRows: [][]string{
				{"gadget", "189.00"},
				{"sprocket", "45.25"},
			},
			want: false,
		},
		{
			name:     "temporal first-row cell in temporal column",
			firstRow: []string{"widget", "2026-06-07"},
			dataRows: [][]string{
				{"gadget", "2026-06-08"},
				{"sprocket", "2026-06-09"},
			},
			want: false,
		},
		{
			// Issue #80: a timestamp first-row cell over a timestamp
			// column is data evidence, exactly like dates.
			name:     "timestamp first-row cell in timestamp column",
			firstRow: []string{"widget", "2026-06-07T08:00:00"},
			dataRows: [][]string{
				{"gadget", "2026-06-08 09:30:00"},
				{"sprocket", "2026-06-09T10:15:00Z"},
			},
			want: false,
		},
		{
			// Issue #80: boolean literals are values, not header names.
			name:     "boolean first-row cell in boolean column",
			firstRow: []string{"widget", "true"},
			dataRows: [][]string{
				{"gadget", "false"},
				{"sprocket", "TRUE"},
			},
			want: false,
		},
		{
			// Issue #80: leading-zero identifiers classify as text now,
			// so an identifier-only file degrades to the documented
			// all-text ambiguity and keeps the historical header guess.
			name:     "leading-zero identifier columns stay ambiguous",
			firstRow: []string{"00501", "alpha"},
			dataRows: [][]string{
				{"00502", "beta"},
				{"00503", "gamma"},
			},
			want: true,
		},
		{
			// Regression: a classic text header over numeric data must
			// still be detected as a header.
			name:     "text header over numeric columns",
			firstRow: []string{"Name", "Age", "Score"},
			dataRows: [][]string{
				{"Alice", "30", "95.5"},
				{"Bob", "25", "87.3"},
			},
			want: true,
		},
		{
			// All-text files are genuinely ambiguous: a text header over
			// text columns and a headerless text file have identical
			// value classes. The historical behavior (header) is pinned.
			name:     "all-text rows stay ambiguous, header assumed",
			firstRow: []string{"alpha", "beta"},
			dataRows: [][]string{
				{"gamma", "delta"},
				{"epsilon", "zeta"},
			},
			want: true,
		},
		{
			// A numeric first-row cell over a text column is not
			// evidence against a header: the column class disagrees.
			name:     "numeric first-row cell over text column",
			firstRow: []string{"Name", "42"},
			dataRows: [][]string{
				{"Alice", "forty"},
				{"Bob", "fifty"},
			},
			want: true,
		},
		{
			// A numeric first-row cell over an all-empty column gives no
			// evidence either way; keep the single-row verdict.
			name:     "numeric first-row cell over empty column",
			firstRow: []string{"Name", "42"},
			dataRows: [][]string{
				{"Alice", ""},
				{"Bob"},
			},
			want: true,
		},
		{
			name:     "all numeric first row is data regardless of rows",
			firstRow: []string{"1", "2"},
			dataRows: [][]string{{"3", "4"}},
			want:     false,
		},
		{
			name:     "no data rows falls back to single-row heuristic",
			firstRow: []string{"Name", "Age"},
			dataRows: nil,
			want:     true,
		},
		{
			name:     "empty first row",
			firstRow: []string{},
			dataRows: [][]string{{"1", "2"}},
			want:     false,
		},
	}
	for _, tt := range tests {
		got := DetectHeaderWithRows(tt.firstRow, tt.dataRows)
		if got != tt.want {
			t.Errorf("DetectHeaderWithRows(%s) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestGenerateFieldNames(t *testing.T) {
	names := GenerateFieldNames(3)
	want := []string{"column_1", "column_2", "column_3"}
	if len(names) != len(want) {
		t.Fatalf("len = %d, want %d", len(names), len(want))
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("names[%d] = %q, want %q", i, names[i], want[i])
		}
	}
}
