package profile

import (
	"testing"
	"time"
)

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
		// Issue #80: impossible dates no longer classify as date.
		{"12/34/5678", TypeText},
		{"-", TypeText},
		{".", TypeText},
		{"1,234,567.89", TypeNumeric},
		{"10000000.00", TypeNumeric},
		// Issue #80: leading-zero identifiers are not numbers; parsing
		// them would drop the zeros and corrupt join keys.
		{"00502", TypeText},
		{"007", TypeText},
		{"0", TypeNumeric},
		{"0.5", TypeNumeric},
		// Issue #80: boolean literals, case-insensitive.
		{"true", TypeBoolean},
		{"FALSE", TypeBoolean},
		{"True", TypeBoolean},
		// Issue #80: ISO 8601 date-times are timestamps, not text.
		{"2024-01-15T10:30:00", TypeTimestamp},
		{"2024-01-15 10:30:00", TypeTimestamp},
		{"2024-01-15T10:30:00.123Z", TypeTimestamp},
		{"2024-01-15T10:30:00+02:00", TypeTimestamp},
		// Issue #80: scientific notation is numeric.
		{"1e5", TypeNumeric},
		{"6.02E23", TypeNumeric},
		// Issue #80: NaN and infinities stay text.
		{"NaN", TypeText},
		{"Inf", TypeText},
		{"-Inf", TypeText},
	}
	for _, tt := range tests {
		got := ClassifyCell(tt.input)
		if got != tt.want {
			t.Errorf("ClassifyCell(%q) = %q, want %q", tt.input, got, tt.want)
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
		// Issue #80: component ranges are validated, so impossible
		// dates stop classifying as dates.
		{"2024-13-01", false}, // month 13
		{"99/99/9999", false}, // no reading has a valid month
		{"1234-56-78", false}, // month 56
		{"2024-02-29", true},  // Feb 29 allowed every year (no leap math)
		{"2024-02-30", false}, // beyond the February bound
		{"2024-04-31", false}, // April has 30 days
		{"2024-04-30", true},
		{"2024-12-31", true},
		{"2024-00-10", false}, // month 0
		{"2024-01-00", false}, // day 0
		// Slash dates are ambiguous; accepted when either the DD/MM or
		// the MM/DD reading is valid.
		{"13/05/2024", true},  // valid only as DD/MM
		{"05/13/2024", true},  // valid only as MM/DD
		{"13/13/2024", false}, // invalid under both readings
		{"31/01/2024", true},
		{"31/04/2024", false}, // April has 30 days under both readings
	}
	for _, tt := range tests {
		got := IsDate(tt.input)
		if got != tt.want {
			t.Errorf("IsDate(%q) = %v, want %v", tt.input, got, tt.want)
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
		// Issue #80: leading-zero identifiers are not numeric. A minus
		// sign signals numeric intent, so -007 still counts.
		{"00502", false},
		{"007", false},
		{"\"007\"", false},
		{"0", true},
		{"007.5", true}, // a decimal mark makes it a number again
		{"-007", true},
		// Issue #80: one definition of numeric with ParseNumeric.
		{"\"-5\"", true}, // quoted negative
		{" 42 ", true},   // surrounding whitespace
		{"1e5", true},    // scientific notation included
		{"1.5e-3", true}, // negative exponent
		{"2E+10", true},  // explicit positive exponent
		{"1e", false},    // exponent needs digits
		{"e5", false},    // mantissa needs digits
		{"1e5e5", false}, // only one exponent
		{"NaN", false},   // NaN excluded
		{"nan", false},   // case-insensitive forms too
		{"Inf", false},   // infinities excluded
		{"Infinity", false},
		{"-Inf", false},
		{"+5", false},   // explicit plus is not in the vocabulary
		{"0x10", false}, // hex is not in the vocabulary
		{"1_000", false},
		{"1e999", false},  // overflows float64, cannot be range-tracked
		{"1e-999", true},  // underflow rounds to zero, still a number
		{"1,234.", false}, // empty decimal part after a thousands group
	}
	for _, tt := range tests {
		got := IsNumeric(tt.input)
		if got != tt.want {
			t.Errorf("IsNumeric(%q) = %v, want %v", tt.input, got, tt.want)
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
		{"1234", false}, // no dot at all
	}
	for _, tt := range tests {
		got := IsUSFormatNumber(tt.input)
		if got != tt.want {
			t.Errorf("IsUSFormatNumber(%q) = %v, want %v", tt.input, got, tt.want)
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
		{"1234", false}, // no comma at all
	}
	for _, tt := range tests {
		got := IsEuropeanFormatNumber(tt.input)
		if got != tt.want {
			t.Errorf("IsEuropeanFormatNumber(%q) = %v, want %v", tt.input, got, tt.want)
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
		got := IsUSThousandsOnly(tt.input)
		if got != tt.want {
			t.Errorf("IsUSThousandsOnly(%q) = %v, want %v", tt.input, got, tt.want)
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
		got := IsEuropeanDecimalOnly(tt.input)
		if got != tt.want {
			t.Errorf("IsEuropeanDecimalOnly(%q) = %v, want %v", tt.input, got, tt.want)
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
		// Issue #80: scientific notation is part of the plain grammar.
		{"1e5", true},
		{"1E5", true},
		{"1.5e-3", true},
		{"2e+10", true},
		{".5e3", true},
		{"5.", true},
		{".5", true},
		{"1e", false},
		{"e5", false},
		{"1e5e5", false},
		{"1e1.5", false},
		{"1.2.3e5", false},
		{".e5", false},
	}
	for _, tt := range tests {
		got := IsPlainNumber(tt.input)
		if got != tt.want {
			t.Errorf("IsPlainNumber(%q) = %v, want %v", tt.input, got, tt.want)
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
		got := AllDigits(tt.input)
		if got != tt.want {
			t.Errorf("AllDigits(%q) = %v, want %v", tt.input, got, tt.want)
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
		got := AllDigitsAndSep(tt.input, tt.sep)
		if got != tt.want {
			t.Errorf("AllDigitsAndSep(%q, %c) = %v, want %v", tt.input, tt.sep, got, tt.want)
		}
	}
}

func TestMergeTypes(t *testing.T) {
	// The lattice (issue #80):
	// text > timestamp > date > numeric > boolean > empty.
	tests := []struct {
		existing, observed, want DataType
	}{
		{TypeNumeric, TypeText, TypeText},
		{TypeDate, TypeText, TypeText},
		{TypeNumeric, TypeDate, TypeDate},
		{TypeEmpty, TypeNumeric, TypeNumeric},
		{TypeText, TypeNumeric, TypeText},
		// A date column with some full timestamps reports timestamp.
		{TypeDate, TypeTimestamp, TypeTimestamp},
		{TypeTimestamp, TypeDate, TypeTimestamp},
		{TypeTimestamp, TypeText, TypeText},
		// Boolean is the lowest concrete class: anything else mixed in
		// describes the column better.
		{TypeEmpty, TypeBoolean, TypeBoolean},
		{TypeBoolean, TypeNumeric, TypeNumeric},
		{TypeBoolean, TypeText, TypeText},
		{TypeBoolean, TypeEmpty, TypeBoolean},
	}
	for _, tt := range tests {
		if got := MergeTypes(tt.existing, tt.observed); got != tt.want {
			t.Errorf("MergeTypes(%q, %q) = %q, want %q", tt.existing, tt.observed, got, tt.want)
		}
	}
}

func TestIsBoolean(t *testing.T) {
	// The vocabulary is deliberately narrow (issue #80): only true and
	// false, case-insensitive. t/f, yes/no, and 0/1 are excluded
	// because they collide with codes, prose, and numbers.
	tests := []struct {
		input string
		want  bool
	}{
		{"true", true},
		{"false", true},
		{"TRUE", true},
		{"FALSE", true},
		{"True", true},
		{"fAlSe", true},
		{"t", false},
		{"f", false},
		{"yes", false},
		{"no", false},
		{"0", false},
		{"1", false},
		{"truth", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := IsBoolean(tt.input); got != tt.want {
			t.Errorf("IsBoolean(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsTimestamp(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"2024-01-15T10:30:00", true},
		{"2024-01-15 10:30:00", true},  // space separator
		{"2024-01-15T10:30", true},     // seconds optional
		{"2024-01-15T10:30Z", true},    // zone directly after minutes
		{"2024-01-15T10:30:aa", false}, // non-digit seconds
		{"2024-01-15T10:30.5", false},  // fraction requires seconds
		{"2024-01-15T10:30:00Z", true}, // zulu zone
		{"2024-01-15T10:30:00z", true},
		{"2024-01-15T10:30:00.5", true},           // fractional seconds
		{"2024-01-15T10:30:00.123456789Z", true},  // nanosecond precision
		{"2024-01-15T10:30:00+02:00", true},       // colon offset
		{"2024-01-15T10:30:00-0530", true},        // compact offset
		{"2024-01-15T10:30:00+02", true},          // hour-only offset
		{"2024-01-15", false},                     // a date is not a timestamp
		{"10:30:00", false},                       // a time alone is not a timestamp
		{"2024-13-01T10:30:00", false},            // impossible month
		{"2024-01-15X10:30:00", false},            // bad separator
		{"2024-01-15T24:00:00", false},            // hour out of range
		{"2024-01-15T10:60:00", false},            // minute out of range
		{"2024-01-15T10:30:61", false},            // second out of range
		{"2024-01-15T10-30", false},               // bad time separator
		{"2024-01-15Taa:30", false},               // non-digit hour
		{"2024-01-15T10:aa", false},               // non-digit minute
		{"2024-01-15T10:30:00.", false},           // empty fraction
		{"2024-01-15T10:30:00.1234567890", false}, // finer than nanoseconds
		{"2024-01-15T10:30:00@", false},           // garbage zone
		{"2024-01-15T10:30:00+2", false},          // one-digit offset hour
		{"2024-01-15T10:30:00+aa", false},         // non-digit offset
		{"2024-01-15T10:30:00+aabb", false},       // non-digit compact offset
		{"2024-01-15T10:30:00+aa:bb", false},      // non-digit colon offset
		{"2024-01-15T10:30:00+02-00", false},      // bad offset separator
		{"2024-01-15T10:30:00+020000", false},     // offset too long
		{"2024-01-15T10:30:00+15:00", false},      // offset hours out of range
		{"2024-01-15T10:30:00+02:60", false},      // offset minutes out of range
		{"15/01/2024 10:30:00", false},            // slash dates are not ISO
		{"", false},
	}
	for _, tt := range tests {
		if got := IsTimestamp(tt.input); got != tt.want {
			t.Errorf("IsTimestamp(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseTemporal(t *testing.T) {
	t.Run("ISO date parses to midnight UTC", func(t *testing.T) {
		got, ok := ParseTemporal("2024-01-15")
		if !ok {
			t.Fatal("expected ok")
		}
		want := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("timestamp with offset keeps the instant", func(t *testing.T) {
		got, ok := ParseTemporal("2024-01-15T12:00:00+02:00")
		if !ok {
			t.Fatal("expected ok")
		}
		want := time.Date(2024, time.January, 15, 10, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("fractional seconds scale to nanoseconds", func(t *testing.T) {
		got, ok := ParseTemporal("2024-01-15T10:30:00.5Z")
		if !ok {
			t.Fatal("expected ok")
		}
		if got.Nanosecond() != 500000000 {
			t.Errorf("nanoseconds = %d, want 500000000", got.Nanosecond())
		}
	})
	t.Run("ambiguous slash date is rejected", func(t *testing.T) {
		// 03/04/2026 could be March 4 or April 3; chronological
		// comparison would have to guess, so it stays lexicographic.
		if _, ok := ParseTemporal("03/04/2026"); ok {
			t.Error("slash date should not parse as temporal")
		}
	})
	t.Run("ten-byte non-date is rejected", func(t *testing.T) {
		if _, ok := ParseTemporal("abcdefghij"); ok {
			t.Error("expected not ok")
		}
	})
	t.Run("short string is rejected", func(t *testing.T) {
		if _, ok := ParseTemporal("10:30"); ok {
			t.Error("expected not ok")
		}
	})
}

func TestTypePriorityUnknown(t *testing.T) {
	// An unknown DataType should get priority 0 (same as empty).
	if TypePriority(DataType("unknown")) != 0 {
		t.Error("unknown type should have priority 0")
	}
}
