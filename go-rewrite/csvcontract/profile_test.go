package csvcontract

import "testing"

func TestProfileColumnEmpty(t *testing.T) {
	p := profileColumn(nil, 5)
	if p.NullCount != 0 || p.DistinctCount != 0 {
		t.Errorf("expected zero profile, got %+v", p)
	}
}

func TestProfileColumnAllNulls(t *testing.T) {
	p := profileColumn([]string{"", "  ", ""}, 5)
	if p.NullCount != 3 {
		t.Errorf("null_count = %d, want 3", p.NullCount)
	}
	if p.NullPercentage != 100 {
		t.Errorf("null_percentage = %f, want 100", p.NullPercentage)
	}
	if p.MinValue != nil || p.MaxValue != nil {
		t.Error("expected nil min/max for all-null column")
	}
	if len(p.SampleValues) != 0 {
		t.Errorf("sample_values should be empty, got %v", p.SampleValues)
	}
}

func TestProfileColumnNumeric(t *testing.T) {
	p := profileColumn([]string{"10", "5", "20", "5"}, 5)
	if p.DistinctCount != 3 {
		t.Errorf("distinct_count = %d, want 3", p.DistinctCount)
	}
	if p.MinValue == nil || *p.MinValue != "5" {
		t.Errorf("min_value = %v, want 5", p.MinValue)
	}
	if p.MaxValue == nil || *p.MaxValue != "20" {
		t.Errorf("max_value = %v, want 20", p.MaxValue)
	}
}

func TestProfileColumnText(t *testing.T) {
	p := profileColumn([]string{"banana", "apple", "cherry"}, 5)
	if p.MinValue == nil || *p.MinValue != "apple" {
		t.Errorf("min_value = %v, want apple", p.MinValue)
	}
	if p.MaxValue == nil || *p.MaxValue != "cherry" {
		t.Errorf("max_value = %v, want cherry", p.MaxValue)
	}
}

func TestProfileColumnMaxSamples(t *testing.T) {
	p := profileColumn([]string{"a", "b", "c", "d", "e", "f"}, 3)
	if len(p.SampleValues) != 3 {
		t.Errorf("sample_values len = %d, want 3", len(p.SampleValues))
	}
}

func TestIsNull(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"", true},
		{"  ", true},
		{"\t", true},
		{"hello", false},
		{" x ", false},
	}
	for _, tt := range tests {
		got := isNull(tt.input)
		if got != tt.want {
			t.Errorf("isNull(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestRangeTrackerEmpty(t *testing.T) {
	var tracker rangeTracker
	if tracker.seen {
		t.Error("expected unseen tracker")
	}
}

func TestRangeTrackerNumeric(t *testing.T) {
	var tracker rangeTracker
	for _, v := range []string{"10", "5", "20"} {
		tracker.observe(v)
	}
	if tracker.min != "5" {
		t.Errorf("min = %q, want 5", tracker.min)
	}
	if tracker.max != "20" {
		t.Errorf("max = %q, want 20", tracker.max)
	}
}

func TestRangeTrackerText(t *testing.T) {
	var tracker rangeTracker
	for _, v := range []string{"banana", "apple", "cherry"} {
		tracker.observe(v)
	}
	if tracker.min != "apple" {
		t.Errorf("min = %q, want apple", tracker.min)
	}
	if tracker.max != "cherry" {
		t.Errorf("max = %q, want cherry", tracker.max)
	}
}

func TestRangeTrackerMixed(t *testing.T) {
	// Adversarial sequence: numeric min/max diverge from lexicographic.
	// Numeric order: 9 < 10 < 100. Lexicographic order: "10" < "100" < "9".
	// When "abc" arrives, the tracker must re-derive min/max lexicographically.
	var tracker rangeTracker
	tracker.observe("9")
	tracker.observe("10")
	tracker.observe("100")
	// At this point: numeric min="9" (9), max="100" (100).
	tracker.observe("abc")
	// After switch: lexicographic min should be "10" (not "9"), max "abc".
	tracker.observe("5")
	// "5" > "10" lexicographically, so min stays "10".

	if tracker.min != "10" {
		t.Errorf("min = %q, want \"10\" (lexicographic after switch)", tracker.min)
	}
	if tracker.max != "abc" {
		t.Errorf("max = %q, want \"abc\"", tracker.max)
	}
}

func TestRangeTrackerNumericToLexSwap(t *testing.T) {
	// Simpler case: numeric min > max lexicographically.
	// Numeric: 2 < 9. Lexicographic: "2" < "9". No swap needed.
	var tracker rangeTracker
	tracker.observe("9")
	tracker.observe("2")
	tracker.observe("hello")

	if tracker.min != "2" {
		t.Errorf("min = %q, want \"2\"", tracker.min)
	}
	if tracker.max != "hello" {
		t.Errorf("max = %q, want \"hello\"", tracker.max)
	}
}

func TestParseNumeric(t *testing.T) {
	tests := []struct {
		input string
		want  float64
		ok    bool
	}{
		{"123", 123, true},
		{"3.14", 3.14, true},
		{"1,234.56", 1234.56, true},
		{"1.234,56", 1234.56, true},
		{"1,234", 1234, true},
		{"abc", 0, false},
		{"", 0, false},
		{"  ", 0, false},
		{"\"100\"", 100, true},
		{"-3.14", -3.14, true},
		{"1.234.567,89", 1234567.89, true},
		{"-1,234.56", -1234.56, true},
		{"abc.def,gh", 0, false},
		{"-", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseNumeric(tt.input)
		if ok != tt.ok {
			t.Errorf("parseNumeric(%q) ok = %v, want %v", tt.input, ok, tt.ok)
			continue
		}
		if ok && got != tt.want {
			t.Errorf("parseNumeric(%q) = %f, want %f", tt.input, got, tt.want)
		}
	}
}

func TestSampleDistinctEmpty(t *testing.T) {
	result := sampleDistinct(map[string]struct{}{}, 5)
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}
