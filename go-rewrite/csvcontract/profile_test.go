package csvcontract

import "testing"

func TestColumnProfilerEmpty(t *testing.T) {
	p := newColumnProfiler(100)
	result := p.finish(10)
	if result.TotalCount != 0 || result.NullCount != 0 {
		t.Errorf("expected zero profile, got %+v", result)
	}
	if len(result.TopValues) != 0 {
		t.Errorf("expected empty top values, got %v", result.TopValues)
	}
}

func TestColumnProfilerAllNulls(t *testing.T) {
	p := newColumnProfiler(100)
	for _, v := range []string{"", "  ", ""} {
		p.observe(v)
	}
	result := p.finish(10)
	if result.NullCount != 3 {
		t.Errorf("null_count = %d, want 3", result.NullCount)
	}
	if result.NullPercentage != 100 {
		t.Errorf("null_percentage = %f, want 100", result.NullPercentage)
	}
	if result.MinValue != nil || result.MaxValue != nil {
		t.Error("expected nil min/max for all-null column")
	}
	if len(result.TopValues) != 0 {
		t.Errorf("expected empty top values, got %v", result.TopValues)
	}
}

func TestColumnProfilerNumeric(t *testing.T) {
	p := newColumnProfiler(100)
	for _, v := range []string{"10", "5", "20", "5"} {
		p.observe(v)
	}
	result := p.finish(10)
	if result.MinValue == nil || *result.MinValue != "5" {
		t.Errorf("min_value = %v, want 5", result.MinValue)
	}
	if result.MaxValue == nil || *result.MaxValue != "20" {
		t.Errorf("max_value = %v, want 20", result.MaxValue)
	}
	// "5" appears twice, should be top.
	if len(result.TopValues) != 3 {
		t.Fatalf("top_values count = %d, want 3", len(result.TopValues))
	}
	if result.TopValues[0].Value != "5" || result.TopValues[0].Count != 2 {
		t.Errorf("top value = %+v, want {5, 2}", result.TopValues[0])
	}
}

func TestColumnProfilerText(t *testing.T) {
	p := newColumnProfiler(100)
	for _, v := range []string{"banana", "apple", "cherry"} {
		p.observe(v)
	}
	result := p.finish(10)
	if result.MinValue == nil || *result.MinValue != "apple" {
		t.Errorf("min_value = %v, want apple", result.MinValue)
	}
	if result.MaxValue == nil || *result.MaxValue != "cherry" {
		t.Errorf("max_value = %v, want cherry", result.MaxValue)
	}
}

func TestColumnProfilerTopNLimit(t *testing.T) {
	p := newColumnProfiler(100)
	for _, v := range []string{"a", "b", "c", "d", "e", "f"} {
		p.observe(v)
	}
	result := p.finish(3)
	if len(result.TopValues) != 3 {
		t.Errorf("top_values count = %d, want 3", len(result.TopValues))
	}
}

func TestColumnProfilerTopNSortOrder(t *testing.T) {
	p := newColumnProfiler(100)
	// "b" appears 3 times, "a" appears 2 times, "c" appears 1 time.
	for _, v := range []string{"b", "a", "b", "c", "a", "b"} {
		p.observe(v)
	}
	result := p.finish(10)
	if len(result.TopValues) != 3 {
		t.Fatalf("top_values count = %d, want 3", len(result.TopValues))
	}
	// Sorted by count desc, then value asc.
	if result.TopValues[0].Value != "b" || result.TopValues[0].Count != 3 {
		t.Errorf("top[0] = %+v, want {b, 3}", result.TopValues[0])
	}
	if result.TopValues[1].Value != "a" || result.TopValues[1].Count != 2 {
		t.Errorf("top[1] = %+v, want {a, 2}", result.TopValues[1])
	}
	if result.TopValues[2].Value != "c" || result.TopValues[2].Count != 1 {
		t.Errorf("top[2] = %+v, want {c, 1}", result.TopValues[2])
	}
}

func TestOptionsMaxTracked(t *testing.T) {
	opts := &Options{MaxTracked: 42}
	if opts.maxTracked() != 42 {
		t.Errorf("maxTracked() = %d, want 42", opts.maxTracked())
	}
}

func TestColumnProfilerCapped(t *testing.T) {
	// With maxTracked=3, only 3 distinct values are tracked.
	// The 4th distinct value is ignored, but existing counters keep working.
	p := newColumnProfiler(3)
	p.observe("a")
	p.observe("b")
	p.observe("c")
	p.observe("d") // ignored -- cap reached
	p.observe("a") // "a" counter still increments

	result := p.finish(10)
	if result.TotalCount != 5 {
		t.Errorf("total_count = %d, want 5", result.TotalCount)
	}
	// "a" should have count 2, "b" and "c" have count 1.
	// "d" should not appear.
	found := map[string]int{}
	for _, tv := range result.TopValues {
		found[tv.Value] = tv.Count
	}
	if found["a"] != 2 {
		t.Errorf("a count = %d, want 2", found["a"])
	}
	if _, ok := found["d"]; ok {
		t.Error("d should not be tracked")
	}
}

func TestColumnProfilerTotalCount(t *testing.T) {
	p := newColumnProfiler(100)
	p.observe("x")
	p.observe("")
	p.observe("y")
	result := p.finish(10)
	if result.TotalCount != 3 {
		t.Errorf("total_count = %d, want 3", result.TotalCount)
	}
	if result.NullCount != 1 {
		t.Errorf("null_count = %d, want 1", result.NullCount)
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
	var tracker rangeTracker
	tracker.observe("9")
	tracker.observe("10")
	tracker.observe("100")
	tracker.observe("abc")
	tracker.observe("5")

	if tracker.min != "10" {
		t.Errorf("min = %q, want \"10\" (lexicographic after switch)", tracker.min)
	}
	if tracker.max != "abc" {
		t.Errorf("max = %q, want \"abc\"", tracker.max)
	}
}

func TestRangeTrackerNumericToLexSwap(t *testing.T) {
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
