package profile

import (
	"testing"
)

func TestColumnProfilerEmpty(t *testing.T) {
	p := NewColumnProfiler(100)
	result := p.Finish(10)
	if result.TotalCount != 0 || result.NullCount != 0 {
		t.Errorf("expected zero profile, got %+v", result)
	}
	if len(result.TopValues) != 0 {
		t.Errorf("expected empty top values, got %v", result.TopValues)
	}
}

func TestColumnProfilerAllNulls(t *testing.T) {
	p := NewColumnProfiler(100)
	for _, v := range []string{"", "  ", ""} {
		p.Observe(v)
	}
	result := p.Finish(10)
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
	p := NewColumnProfiler(100)
	for _, v := range []string{"10", "5", "20", "5"} {
		p.Observe(v)
	}
	result := p.Finish(10)
	if result.MinValue == nil || *result.MinValue != "5" {
		t.Errorf("min_value = %v, want 5", result.MinValue)
	}
	if result.MaxValue == nil || *result.MaxValue != "20" {
		t.Errorf("max_value = %v, want 20", result.MaxValue)
	}
	if result.DistinctCount != 3 {
		t.Errorf("distinct_count = %d, want 3", result.DistinctCount)
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
	p := NewColumnProfiler(100)
	for _, v := range []string{"banana", "apple", "cherry"} {
		p.Observe(v)
	}
	result := p.Finish(10)
	if result.MinValue == nil || *result.MinValue != "apple" {
		t.Errorf("min_value = %v, want apple", result.MinValue)
	}
	if result.MaxValue == nil || *result.MaxValue != "cherry" {
		t.Errorf("max_value = %v, want cherry", result.MaxValue)
	}
}

func TestColumnProfilerTopNLimit(t *testing.T) {
	p := NewColumnProfiler(100)
	for _, v := range []string{"a", "b", "c", "d", "e", "f"} {
		p.Observe(v)
	}
	result := p.Finish(3)
	if len(result.TopValues) != 3 {
		t.Errorf("top_values count = %d, want 3", len(result.TopValues))
	}
}

func TestColumnProfilerTopNSortOrder(t *testing.T) {
	p := NewColumnProfiler(100)
	// "b" appears 3 times, "a" appears 2 times, "c" appears 1 time.
	for _, v := range []string{"b", "a", "b", "c", "a", "b"} {
		p.Observe(v)
	}
	result := p.Finish(10)
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

func TestColumnProfilerCapped(t *testing.T) {
	// With maxTracked=3, only 3 distinct values are tracked.
	// The 4th distinct value is ignored, but existing counters keep working.
	p := NewColumnProfiler(3)
	p.Observe("a")
	p.Observe("b")
	p.Observe("c")
	p.Observe("d") // ignored -- cap reached
	p.Observe("a") // "a" counter still increments

	result := p.Finish(10)
	if result.TotalCount != 5 {
		t.Errorf("total_count = %d, want 5", result.TotalCount)
	}
	// DistinctCount is capped at maxTracked (3), even though 4 distinct values were observed.
	if result.DistinctCount != 3 {
		t.Errorf("distinct_count = %d, want 3", result.DistinctCount)
	}
	// Crossing the cap is surfaced: distinct_count is a floor, not exact.
	if !result.DistinctCountCapped {
		t.Error("distinct_count_capped = false, want true after crossing the cap")
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

func TestColumnProfilerNotCapped(t *testing.T) {
	// Exactly maxTracked distinct values: every value was tracked, so the
	// distinct count is exact and the capped flag stays false.
	p := NewColumnProfiler(3)
	for _, v := range []string{"a", "b", "c", "a"} {
		p.Observe(v)
	}
	result := p.Finish(10)
	if result.DistinctCount != 3 {
		t.Errorf("distinct_count = %d, want 3", result.DistinctCount)
	}
	if result.DistinctCountCapped {
		t.Error("distinct_count_capped = true, want false when the cap was never exceeded")
	}
}

func TestColumnProfilerTotalCount(t *testing.T) {
	p := NewColumnProfiler(100)
	p.Observe("x")
	p.Observe("")
	p.Observe("y")
	result := p.Finish(10)
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
		got := IsNull(tt.input)
		if got != tt.want {
			t.Errorf("IsNull(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestRangeTrackerEmpty(t *testing.T) {
	var tracker RangeTracker
	if tracker.Seen() {
		t.Error("expected unseen tracker")
	}
}

func TestRangeTrackerNumeric(t *testing.T) {
	var tracker RangeTracker
	for _, v := range []string{"10", "5", "20"} {
		tracker.Observe(v)
	}
	if tracker.Min() != "5" {
		t.Errorf("min = %q, want 5", tracker.Min())
	}
	if tracker.Max() != "20" {
		t.Errorf("max = %q, want 20", tracker.Max())
	}
}

func TestRangeTrackerText(t *testing.T) {
	var tracker RangeTracker
	for _, v := range []string{"banana", "apple", "cherry"} {
		tracker.Observe(v)
	}
	if tracker.Min() != "apple" {
		t.Errorf("min = %q, want apple", tracker.Min())
	}
	if tracker.Max() != "cherry" {
		t.Errorf("max = %q, want cherry", tracker.Max())
	}
}

func TestRangeTrackerMixed(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("9")
	tracker.Observe("10")
	tracker.Observe("100")
	tracker.Observe("abc")
	tracker.Observe("5")

	if tracker.Min() != "10" {
		t.Errorf("min = %q, want \"10\" (lexicographic after switch)", tracker.Min())
	}
	if tracker.Max() != "abc" {
		t.Errorf("max = %q, want \"abc\"", tracker.Max())
	}
}

func TestRangeTrackerNumericToLexSwap(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("9")
	tracker.Observe("2")
	tracker.Observe("hello")

	if tracker.Min() != "2" {
		t.Errorf("min = %q, want \"2\"", tracker.Min())
	}
	if tracker.Max() != "hello" {
		t.Errorf("max = %q, want \"hello\"", tracker.Max())
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
		{"1,234,567", 1234567, true},
		{"10,5", 10.5, true},
		{"9,25", 9.25, true},
		{"2,75", 2.75, true},
		{"-10,5", -10.5, true},
		{"1,2345", 1.2345, true},
		{"1,23,456", 0, false},
		{"abc", 0, false},
		{"", 0, false},
		{"  ", 0, false},
		{"\"100\"", 100, true},
		{"-3.14", -3.14, true},
		{"1.234.567,89", 1234567.89, true},
		{"-1,234.56", -1234.56, true},
		{"abc.def,gh", 0, false},
		{"-", 0, false},
		// Issue #80: ParseNumeric and IsNumeric share one definition.
		{"00502", 0, false}, // leading-zero identifier
		{"-007", -7, true},  // a minus signals numeric intent
		{"1e5", 100000, true},
		{"1.5e-3", 0.0015, true},
		{"-2E3", -2000, true},
		{"NaN", 0, false},
		{"Inf", 0, false},
		{"-Inf", 0, false},
		{"+5", 0, false},
		{"\"-5\"", -5, true}, // quoted negative
		{"1e999", 0, false},  // overflows float64
		{"1,234.", 0, false}, // malformed mixed-separator form
	}
	for _, tt := range tests {
		got, ok := ParseNumeric(tt.input)
		if ok != tt.ok {
			t.Errorf("ParseNumeric(%q) ok = %v, want %v", tt.input, ok, tt.ok)
			continue
		}
		if ok && got != tt.want {
			t.Errorf("ParseNumeric(%q) = %f, want %f", tt.input, got, tt.want)
		}
	}
}

// TestParseNumericAgreesWithIsNumeric pins that classification and
// parsing share one definition of numeric (issue #80): a value
// classifies as numeric exactly when the range tracker can parse it.
func TestParseNumericAgreesWithIsNumeric(t *testing.T) {
	inputs := []string{
		"10,5", "9,25", "2,75", "-10,5", "12,34", "1,2345",
		"1,234", "1,234,567", "1,23,456", "1,234.56", "5,", ",5",
		"00502", "007", "-007", "0", "1e5", "1.5e-3", "NaN", "Inf",
		"-Inf", "+5", "\"-5\"", "\"007\"", "1e999", "1,234.", "0x10",
		"9007199254740993", "99999999999999999999",
	}
	for _, in := range inputs {
		_, parseOK := ParseNumeric(in)
		classifyOK := IsNumeric(in)
		if parseOK != classifyOK {
			t.Errorf("disagreement on %q: ParseNumeric ok = %v, IsNumeric = %v", in, parseOK, classifyOK)
		}
	}
}

func TestRangeTrackerEuropeanDecimalComma(t *testing.T) {
	// Issue #79 reproduction: Swedish prices with decimal commas. The
	// raw spellings are preserved while comparison is numeric, so the
	// range no longer inverts (10,5 must not parse as 105).
	var tracker RangeTracker
	for _, v := range []string{"10,5", "9,25", "2,75"} {
		tracker.Observe(v)
	}
	if tracker.Min() != "2,75" {
		t.Errorf("min = %q, want \"2,75\"", tracker.Min())
	}
	if tracker.Max() != "10,5" {
		t.Errorf("max = %q, want \"10,5\"", tracker.Max())
	}
}

func TestRangeTrackerMixedCommaConventions(t *testing.T) {
	// A column mixing conventions keeps whatever each value says:
	// "10,5" reads as 10.5 (decimal comma) while "1,234" reads as 1234
	// (well-formed US thousands), so 1,234 is the maximum.
	var tracker RangeTracker
	tracker.Observe("10,5")
	tracker.Observe("1,234")
	if tracker.Min() != "10,5" {
		t.Errorf("min = %q, want \"10,5\"", tracker.Min())
	}
	if tracker.Max() != "1,234" {
		t.Errorf("max = %q, want \"1,234\"", tracker.Max())
	}
}

// TestRangeTrackerIntegerSafe pins integer-exact comparison past 2^53
// (issue #80): as float64, 9007199254740993 rounds to the same value as
// 9007199254740992, so float comparison would corrupt the ordering.
func TestRangeTrackerIntegerSafe(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("9007199254740993")
	tracker.Observe("9007199254740992")
	if tracker.Min() != "9007199254740992" {
		t.Errorf("min = %q, want 9007199254740992", tracker.Min())
	}
	if tracker.Max() != "9007199254740993" {
		t.Errorf("max = %q, want 9007199254740993", tracker.Max())
	}
}

// TestRangeTrackerIntegerSafeNegative covers the same hazard below zero.
func TestRangeTrackerIntegerSafeNegative(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("-9007199254740992")
	tracker.Observe("-9007199254740993")
	if tracker.Min() != "-9007199254740993" {
		t.Errorf("min = %q, want -9007199254740993", tracker.Min())
	}
	if tracker.Max() != "-9007199254740992" {
		t.Errorf("max = %q, want -9007199254740992", tracker.Max())
	}
}

// TestRangeTrackerIntegerBeyondInt64 pins the documented fallback:
// integers beyond int64 compare as float64.
func TestRangeTrackerIntegerBeyondInt64(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("99999999999999999999") // 20 digits, beyond int64
	tracker.Observe("1")
	if tracker.Min() != "1" {
		t.Errorf("min = %q, want 1", tracker.Min())
	}
	if tracker.Max() != "99999999999999999999" {
		t.Errorf("max = %q, want 99999999999999999999", tracker.Max())
	}
}

// TestRangeTrackerNaNExcluded is the issue #80 regression: NaN is not
// numeric, so it can no longer poison the numeric min/max (every
// comparison against NaN is false, which used to freeze the extremes).
func TestRangeTrackerNaNExcluded(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("NaN")
	tracker.Observe("5")
	tracker.Observe("10")
	// NaN demotes the column to lexicographic ordering.
	if tracker.Min() != "10" {
		t.Errorf("min = %q, want \"10\" (lexicographic)", tracker.Min())
	}
	if tracker.Max() != "NaN" {
		t.Errorf("max = %q, want \"NaN\"", tracker.Max())
	}
}

// TestRangeTrackerLeadingZeroIdentifiers pins that identifier columns
// (issue #80) order lexicographically, which preserves the spellings.
func TestRangeTrackerLeadingZeroIdentifiers(t *testing.T) {
	var tracker RangeTracker
	for _, v := range []string{"00502", "00042", "00999"} {
		tracker.Observe(v)
	}
	if tracker.Min() != "00042" {
		t.Errorf("min = %q, want 00042", tracker.Min())
	}
	if tracker.Max() != "00999" {
		t.Errorf("max = %q, want 00999", tracker.Max())
	}
}

// TestRangeTrackerChronological pins chronological ordering for ISO
// temporal columns (issue #80). Lexicographically "2026-01-02 00:00:00"
// sorts before "2026-01-01T23:00" because space sorts before 'T', so a
// byte comparison would invert this range.
func TestRangeTrackerChronological(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("2026-01-01T23:00")
	tracker.Observe("2026-01-02 00:00:00")
	if tracker.Min() != "2026-01-01T23:00" {
		t.Errorf("min = %q, want 2026-01-01T23:00", tracker.Min())
	}
	if tracker.Max() != "2026-01-02 00:00:00" {
		t.Errorf("max = %q, want 2026-01-02 00:00:00", tracker.Max())
	}
}

// TestRangeTrackerChronologicalZones pins that zone offsets order by
// instant, not by spelling.
func TestRangeTrackerChronologicalZones(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("2026-01-01T12:00:00+02:00") // 10:00 UTC
	tracker.Observe("2026-01-01T11:00:00Z")      // 11:00 UTC
	if tracker.Min() != "2026-01-01T12:00:00+02:00" {
		t.Errorf("min = %q, want the +02:00 spelling (earlier instant)", tracker.Min())
	}
	if tracker.Max() != "2026-01-01T11:00:00Z" {
		t.Errorf("max = %q, want 2026-01-01T11:00:00Z", tracker.Max())
	}
}

// TestRangeTrackerDatesAndTimestampsMix pins that ISO dates and
// timestamps share the chronological ordering (dates read as midnight).
func TestRangeTrackerDatesAndTimestampsMix(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("2026-01-02")
	tracker.Observe("2026-01-01T23:59:59")
	tracker.Observe("2026-01-03")
	if tracker.Min() != "2026-01-01T23:59:59" {
		t.Errorf("min = %q, want 2026-01-01T23:59:59", tracker.Min())
	}
	if tracker.Max() != "2026-01-03" {
		t.Errorf("max = %q, want 2026-01-03", tracker.Max())
	}
}

// TestRangeTrackerSlashDatesStayLexicographic pins the documented
// limitation (issue #80): ambiguous slash dates cannot be ordered
// chronologically, so the tracker keeps byte order for them.
func TestRangeTrackerSlashDatesStayLexicographic(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("15/01/2026")
	tracker.Observe("02/12/2025")
	if tracker.Min() != "02/12/2025" {
		t.Errorf("min = %q, want 02/12/2025 (lexicographic)", tracker.Min())
	}
	if tracker.Max() != "15/01/2026" {
		t.Errorf("max = %q, want 15/01/2026", tracker.Max())
	}
}

// TestRangeTrackerTemporalToLexSwap pins the demotion to lexicographic
// ordering when a non-temporal value lands in a temporal column.
func TestRangeTrackerTemporalToLexSwap(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("2026-01-01T23:00")
	tracker.Observe("2026-01-02 00:00:00")
	tracker.Observe("pending")
	if tracker.Min() != "2026-01-01T23:00" {
		t.Errorf("min = %q, want 2026-01-01T23:00 (lexicographic)", tracker.Min())
	}
	if tracker.Max() != "pending" {
		t.Errorf("max = %q, want pending", tracker.Max())
	}
}

// TestRangeTrackerTemporalAfterNonTemporal covers the path where the
// first value already broke the temporal mode.
func TestRangeTrackerTemporalAfterNonTemporal(t *testing.T) {
	var tracker RangeTracker
	tracker.Observe("pending")
	tracker.Observe("2026-01-01T23:00")
	if tracker.Min() != "2026-01-01T23:00" {
		t.Errorf("min = %q, want 2026-01-01T23:00 (lexicographic)", tracker.Min())
	}
	if tracker.Max() != "pending" {
		t.Errorf("max = %q, want pending", tracker.Max())
	}
}

func TestOptionsDefaults(t *testing.T) {
	var nilOpts *Options
	if nilOpts.GetTopN() != 5 {
		t.Errorf("nil GetTopN() = %d, want 5", nilOpts.GetTopN())
	}
	if nilOpts.GetMaxTracked() != 10000 {
		t.Errorf("nil GetMaxTracked() = %d, want 10000", nilOpts.GetMaxTracked())
	}
	if nilOpts.GetMaxSampleRows() != 5 {
		t.Errorf("nil GetMaxSampleRows() = %d, want 5", nilOpts.GetMaxSampleRows())
	}

	opts := &Options{TopN: 42, MaxTracked: 100, MaxSampleRows: 10}
	if opts.GetTopN() != 42 {
		t.Errorf("GetTopN() = %d, want 42", opts.GetTopN())
	}
	if opts.GetMaxTracked() != 100 {
		t.Errorf("GetMaxTracked() = %d, want 100", opts.GetMaxTracked())
	}
	if opts.GetMaxSampleRows() != 10 {
		t.Errorf("GetMaxSampleRows() = %d, want 10", opts.GetMaxSampleRows())
	}
}
