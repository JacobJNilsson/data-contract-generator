package profile

import (
	"math"
	"slices"
	"strings"
	"time"

	"github.com/JacobJNilsson/data-contract-generator/contract"
)

// ColumnProfiler collects statistics for a single column incrementally,
// one value at a time. It tracks null counts, min/max, and a capped
// frequency map for the top-N most common values.
type ColumnProfiler struct {
	totalCount int
	nullCount  int
	freqs      map[string]int
	maxTracked int
	capped     bool
	tracker    RangeTracker
	shape      shapeTracker
}

// NewColumnProfiler creates a profiler that tracks up to maxTracked
// distinct values for frequency counting.
func NewColumnProfiler(maxTracked int) *ColumnProfiler {
	return &ColumnProfiler{
		freqs:      make(map[string]int),
		maxTracked: maxTracked,
	}
}

// Observe records a single cell value.
func (p *ColumnProfiler) Observe(value string) {
	p.totalCount++

	if IsNull(value) {
		p.nullCount++
		return
	}

	p.tracker.Observe(value)
	p.shape.observe(value)

	if count, exists := p.freqs[value]; exists {
		p.freqs[value] = count + 1
	} else if len(p.freqs) < p.maxTracked {
		p.freqs[value] = 1
	} else {
		p.capped = true
	}
}

// Finish computes the final FieldProfile from the accumulated state.
func (p *ColumnProfiler) Finish(topN int) FieldProfile {
	var minVal, maxVal *string
	if p.tracker.seen {
		mn := p.tracker.Min()
		mx := p.tracker.Max()
		minVal = &mn
		maxVal = &mx
	}

	nullPct := 0.0
	if p.totalCount > 0 {
		nullPct = math.Round(float64(p.nullCount)/float64(p.totalCount)*10000) / 100
	}

	return FieldProfile{
		TotalCount:          p.totalCount,
		NullCount:           p.nullCount,
		NullPercentage:      nullPct,
		DistinctCount:       len(p.freqs),
		DistinctCountCapped: p.capped,
		MinValue:            minVal,
		MaxValue:            maxVal,
		TopValues:           p.topValues(topN),
		Shape:               p.shape.signature(),
	}
}

// topValues returns the topN most frequent values, sorted by count
// descending, then by value ascending for stable ordering.
func (p *ColumnProfiler) topValues(topN int) []contract.TopValue {
	if len(p.freqs) == 0 {
		return []contract.TopValue{}
	}

	entries := make([]contract.TopValue, 0, len(p.freqs))
	for v, c := range p.freqs {
		entries = append(entries, contract.TopValue{Value: v, Count: c})
	}

	slices.SortFunc(entries, func(a, b contract.TopValue) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Value, b.Value)
	})

	if len(entries) > topN {
		entries = entries[:topN]
	}
	return entries
}

// RangeTracker tracks the minimum and maximum values seen so far, using
// type-aware comparison. When all observed values are numeric, it
// compares numerically (so "9" < "10"), integer-safely when both sides
// are plain integers within int64 (issue #80: beyond 2^53 float64
// corrupts ordering). When all observed values are unambiguous ISO
// dates or timestamps, it compares chronologically. Otherwise it falls
// back to lexicographic comparison; in particular, ambiguous slash-form
// dates (03/04/2026) stay lexicographic because their reading is
// unknown.
//
// All three orderings are tracked simultaneously so that switching
// modes does not require storing all observed values.
type RangeTracker struct {
	// Numeric min/max (used when allNumeric is true).
	minNum, maxNum       numericValue
	minNumStr, maxNumStr string // raw spellings of the numeric extremes
	// Chronological min/max (used when allTemporal is true).
	minTime, maxTime       time.Time
	minTimeStr, maxTimeStr string // raw spellings of the temporal extremes
	// Lexicographic min/max (always tracked).
	lexMin, lexMax                string
	seen, allNumeric, allTemporal bool
}

// Observe records a value for range tracking.
func (t *RangeTracker) Observe(v string) {
	num, isNum := parseNumericValue(v)
	var temp time.Time
	isTemp := false
	if !isNum {
		temp, isTemp = ParseTemporal(v)
	}

	if !t.seen {
		t.seen = true
		t.lexMin, t.lexMax = v, v
		t.allNumeric = isNum
		t.allTemporal = isTemp
		if isNum {
			t.minNum, t.maxNum = num, num
			t.minNumStr, t.maxNumStr = v, v
		}
		if isTemp {
			t.minTime, t.maxTime = temp, temp
			t.minTimeStr, t.maxTimeStr = v, v
		}
		return
	}

	if !isNum {
		t.allNumeric = false
	}
	if !isTemp {
		t.allTemporal = false
	}

	// Always track lexicographic min/max.
	if v < t.lexMin {
		t.lexMin = v
	}
	if v > t.lexMax {
		t.lexMax = v
	}

	// Track numeric min/max while the column is still all-numeric.
	if t.allNumeric {
		if numLess(num, t.minNum) {
			t.minNum = num
			t.minNumStr = v
		}
		if numLess(t.maxNum, num) {
			t.maxNum = num
			t.maxNumStr = v
		}
	}

	// Track chronological min/max while the column is still all-temporal.
	if t.allTemporal {
		if temp.Before(t.minTime) {
			t.minTime = temp
			t.minTimeStr = v
		}
		if temp.After(t.maxTime) {
			t.maxTime = temp
			t.maxTimeStr = v
		}
	}
}

// numLess compares two numeric values, exactly via int64 when both are
// plain integers in range, otherwise via float64.
func numLess(a, b numericValue) bool {
	if a.isInt && b.isInt {
		return a.i < b.i
	}
	return a.f < b.f
}

// Min returns the minimum value observed: numeric comparison when all
// values were numeric, chronological when all values were ISO dates or
// timestamps, otherwise lexicographic.
func (t *RangeTracker) Min() string {
	switch {
	case t.allNumeric:
		return t.minNumStr
	case t.allTemporal:
		return t.minTimeStr
	default:
		return t.lexMin
	}
}

// Max returns the maximum value observed.
func (t *RangeTracker) Max() string {
	switch {
	case t.allNumeric:
		return t.maxNumStr
	case t.allTemporal:
		return t.maxTimeStr
	default:
		return t.lexMax
	}
}

// Seen returns whether any values have been observed.
func (t *RangeTracker) Seen() bool {
	return t.seen
}

// IsNull returns true if the value is empty or whitespace-only.
func IsNull(v string) bool {
	return strings.TrimSpace(v) == ""
}

// ParseNumeric attempts to parse a string as a float64, handling both
// US (1,234.56) and European (1.234,56) number formats. It is a view
// over parseNumericValue, the single definition of numeric shared with
// IsNumeric and RangeTracker (issue #80), so a value classifies as
// numeric exactly when it parses.
//
// Comma-only values (commas, no dots) are disambiguated by shape, since
// the profiler has no file-level context such as the delimiter:
//   - A well-formed thousands pattern (1,234 or 1,234,567) keeps US
//     thousands semantics, so "1,234" alone stays 1234.
//   - Any other single-comma form (10,5 / 9,25) is treated as a decimal
//     comma, the common European spelling, so "10,5" is 10.5.
//
// The residual ambiguity is real: "1,234" in a Swedish file is still
// read as one thousand two hundred thirty-four, and a column mixing
// both conventions keeps whatever each individual value says.
func ParseNumeric(s string) (float64, bool) {
	nv, ok := parseNumericValue(s)
	return nv.f, ok
}
