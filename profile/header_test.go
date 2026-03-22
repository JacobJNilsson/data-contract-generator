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
