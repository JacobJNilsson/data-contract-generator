package csvcontract

import "testing"

func TestClassifyCell(t *testing.T) {
	tests := []struct {
		input string
		want  DataType
	}{
		{"", TypeEmpty},
		{"  ", TypeEmpty},
		{"hello", TypeText},
		{"123", TypeNumeric},
		{"-456", TypeNumeric},
		{"3.14", TypeNumeric},
		{"1,234.56", TypeNumeric},
		{"1.234,56", TypeNumeric},
		{"1,5", TypeNumeric},
		{"1,234", TypeNumeric},
		{"2024-01-15", TypeDate},
		{"15/01/2024", TypeDate},
		{"not-a-date", TypeText},
		{"12/34/5678", TypeDate}, // passes format check
		{"-", TypeText},
		{".", TypeText},
		{"1,234,567.89", TypeNumeric},
		{"10000000.00", TypeNumeric},
	}
	for _, tt := range tests {
		got := classifyCell(tt.input)
		if got != tt.want {
			t.Errorf("classifyCell(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsDate(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"2024-01-15", true},
		{"15/01/2024", true},
		{"01/15/2024", true},
		{"2024-1-15", false},  // wrong length
		{"202X-01-15", false}, // non-digit
		{"15-01-2024", false}, // wrong separator position
		{"15/01/24", false},   // wrong length
		{"hello", false},
		{"", false},
		{"2024-13-01", true}, // we don't validate ranges, just format
		{"99/99/9999", true}, // same: format only
	}
	for _, tt := range tests {
		got := isDate(tt.input)
		if got != tt.want {
			t.Errorf("isDate(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"", false},
		{"-", false},
		{"123", true},
		{"-456", true},
		{"3.14", true},
		{"-0.5", true},
		{"1,234.56", true},      // US format
		{"1.234,56", true},      // European format
		{"1,5", true},           // European decimal
		{"1,234", true},         // US thousands
		{"1,234,567", true},     // US thousands multi
		{"1,234,567.89", true},  // US full
		{"10,000,000.00", true}, // US large
		{"abc", false},
		{"12.34.56", false},    // multiple dots without separator
		{"\"1,234.56\"", true}, // quoted
		{".", false},           // just a dot
		{"-.", false},          // negative dot
		{"\"123\"", true},      // quoted plain number
		{"1.234.567,89", true}, // European with thousands
		{"abc.def,gh", false},  // European format but non-numeric
		{"\"\"", false},        // quoted empty string
	}
	for _, tt := range tests {
		got := isNumeric(tt.input)
		if got != tt.want {
			t.Errorf("isNumeric(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsUSFormatNumber(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"1,234.56", true},
		{"1,234,567.89", true},
		{"abc.def", false},
	}
	for _, tt := range tests {
		got := isUSFormatNumber(tt.input)
		if got != tt.want {
			t.Errorf("isUSFormatNumber(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsEuropeanFormatNumber(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"1.234,56", true},
		{"1.234.567,89", true},
		{"abc,def", false},
	}
	for _, tt := range tests {
		got := isEuropeanFormatNumber(tt.input)
		if got != tt.want {
			t.Errorf("isEuropeanFormatNumber(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsUSThousandsOnly(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"1,234", true},
		{"1,234,567", true},
		{"12,34", false}, // second group not 3 digits
		{"1234", false},  // no comma -> only 1 part
		{",234", false},  // empty first part
	}
	for _, tt := range tests {
		got := isUSThousandsOnly(tt.input)
		if got != tt.want {
			t.Errorf("isUSThousandsOnly(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsEuropeanDecimalOnly(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"1,5", true},
		{"100,25", true},
		{"abc,def", false},
		{",5", false}, // empty integer part
		{"5,", false}, // empty decimal part
		{"no comma", false},
	}
	for _, tt := range tests {
		got := isEuropeanDecimalOnly(tt.input)
		if got != tt.want {
			t.Errorf("isEuropeanDecimalOnly(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsPlainNumber(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"123", true},
		{"3.14", true},
		{".", false},
		{"", false},
		{"abc", false},
		{"1.2.3", false},
	}
	for _, tt := range tests {
		got := isPlainNumber(tt.input)
		if got != tt.want {
			t.Errorf("isPlainNumber(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestAllDigits(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"123", true},
		{"", false},
		{"12a3", false},
	}
	for _, tt := range tests {
		got := allDigits(tt.input)
		if got != tt.want {
			t.Errorf("allDigits(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestAllDigitsAndSep(t *testing.T) {
	tests := []struct {
		input string
		sep   byte
		want  bool
	}{
		{"1,234", ',', true},
		{"", ',', false},
		{"1a234", ',', false},
	}
	for _, tt := range tests {
		got := allDigitsAndSep(tt.input, tt.sep)
		if got != tt.want {
			t.Errorf("allDigitsAndSep(%q, %c) = %v, want %v", tt.input, tt.sep, got, tt.want)
		}
	}
}

func TestMergeTypes(t *testing.T) {
	// Text always wins.
	if mergeTypes(TypeNumeric, TypeText) != TypeText {
		t.Error("text should override numeric")
	}
	if mergeTypes(TypeDate, TypeText) != TypeText {
		t.Error("text should override date")
	}
	// Date beats numeric.
	if mergeTypes(TypeNumeric, TypeDate) != TypeDate {
		t.Error("date should override numeric")
	}
	// Numeric beats empty.
	if mergeTypes(TypeEmpty, TypeNumeric) != TypeNumeric {
		t.Error("numeric should override empty")
	}
	// Lower priority doesn't override.
	if mergeTypes(TypeText, TypeNumeric) != TypeText {
		t.Error("numeric should not override text")
	}
}

func TestTypePriorityUnknown(t *testing.T) {
	// An unknown DataType should get priority 0 (same as empty).
	if typePriority(DataType("unknown")) != 0 {
		t.Error("unknown type should have priority 0")
	}
}

func TestInferColumnTypes(t *testing.T) {
	rows := [][]string{
		{"hello", "123", "2024-01-01", ""},
		{"world", "456", "2024-02-02", ""},
	}
	types := inferColumnTypes(rows, 4)
	expected := []DataType{TypeText, TypeNumeric, TypeDate, TypeEmpty}
	for i, want := range expected {
		if types[i] != want {
			t.Errorf("column %d: got %q, want %q", i, types[i], want)
		}
	}
}
