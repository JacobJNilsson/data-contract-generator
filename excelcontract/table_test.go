package excelcontract

import "testing"

func TestIsEmptyRow(t *testing.T) {
	tests := []struct {
		name string
		row  []string
		want bool
	}{
		{"nil", nil, true},
		{"empty", []string{}, true},
		{"all empty strings", []string{"", "", ""}, true},
		{"whitespace only", []string{"  ", "\t", ""}, true},
		{"has data", []string{"", "hello", ""}, false},
		{"single value", []string{"x"}, false},
	}
	for _, tt := range tests {
		got := isEmptyRow(tt.row)
		if got != tt.want {
			t.Errorf("isEmptyRow(%s) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		input   string
		wantTL  cellRef
		wantBR  cellRef
		wantErr bool
	}{
		{"A1:C4", cellRef{0, 0}, cellRef{3, 2}, false},
		{"B2:D5", cellRef{1, 1}, cellRef{4, 3}, false},
		{"A1", cellRef{}, cellRef{}, true},     // no colon
		{"", cellRef{}, cellRef{}, true},       // empty
		{"!!!:C4", cellRef{}, cellRef{}, true}, // invalid top-left
		{"A1:!!!", cellRef{}, cellRef{}, true}, // invalid bottom-right
	}
	for _, tt := range tests {
		tl, br, err := parseRange(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseRange(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if err == nil {
			if tl != tt.wantTL || br != tt.wantBR {
				t.Errorf("parseRange(%q) = %v, %v, want %v, %v", tt.input, tl, br, tt.wantTL, tt.wantBR)
			}
		}
	}
}
