package profile

import (
	"math"
	"slices"
	"strings"
	"unicode"
)

// maxTrackedMasks bounds the per-column mask map so adversarial data
// cannot bloat it; once exceeded, further unseen masks land in the
// overflowMask bucket.
const maxTrackedMasks = 32

// overflowMask is the bucket that absorbs mask classes beyond the
// tracked bound. It never participates in dominance decisions: an
// overflowing column has no clear dominant by definition.
const overflowMask = "..."

// dominantShareFloor is the share a mask class needs before the
// comparator treats it as the column's clear dominant. Below it the
// column is too mixed for a swap verdict, and DriftsFrom stays quiet.
const dominantShareFloor = 0.6

// MaskCount is one coarse format mask and how many values matched it.
type MaskCount struct {
	Mask  string `json:"mask"`
	Count int    `json:"count"`
}

// ShapeSignature is a column's value-shape evidence: the distribution
// of coarse format masks over its non-null values, the share of the
// most common mask, and the observed length range. It serves two
// consumers at once: an authoring agent reading the contract gets
// explicit shape evidence beside the samples, and the ingestion
// platform compares a landed file's signatures against the ones
// recorded when the pipeline was authored, to catch files whose
// columns swapped meaning under an unchanged schema.
type ShapeSignature struct {
	// Masks holds the mask classes seen, sorted by count descending
	// then mask ascending, so identical input always serializes
	// identically.
	Masks []MaskCount `json:"masks"`

	// DominantShare is the fraction of non-null values matching the
	// most common mask, rounded to two decimals for JSON stability.
	// Zero when the column had no non-null values.
	DominantShare float64 `json:"dominant_share"`

	// LengthMin and LengthMax bound the rune lengths of non-null
	// values. Both zero when the column had no non-null values.
	LengthMin int `json:"length_min"`
	LengthMax int `json:"length_max"`
}

// FormatMask compresses a value into its coarse shape: runs of letters
// become "A", runs of digits become "9", runs of whitespace become
// "_", and any other rune is kept verbatim with identical neighbours
// collapsed. "P-600" masks to "A-9", "Kedjespannare K2" to "A_A9",
// "75.50" to "9.9", "2026-06-07" to "9-9-9".
func FormatMask(value string) string {
	var b strings.Builder
	var last rune
	hasLast := false
	for _, r := range value {
		var class rune
		switch {
		case unicode.IsLetter(r):
			class = 'A'
		case unicode.IsDigit(r):
			class = '9'
		case unicode.IsSpace(r):
			class = '_'
		default:
			class = r
		}
		if hasLast && class == last {
			continue
		}
		b.WriteRune(class)
		last = class
		hasLast = true
	}
	return b.String()
}

// shapeTracker accumulates mask statistics inside ColumnProfiler's
// existing single pass.
type shapeTracker struct {
	masks    map[string]int
	total    int
	lenMin   int
	lenMax   int
	seenAny  bool
	overflow int
}

// observe records one non-null value's mask and length.
func (t *shapeTracker) observe(value string) {
	if t.masks == nil {
		t.masks = make(map[string]int)
	}
	t.total++
	length := len([]rune(value))
	if !t.seenAny || length < t.lenMin {
		t.lenMin = length
	}
	if !t.seenAny || length > t.lenMax {
		t.lenMax = length
	}
	t.seenAny = true

	mask := FormatMask(value)
	if _, exists := t.masks[mask]; exists {
		t.masks[mask]++
		return
	}
	if len(t.masks) < maxTrackedMasks {
		t.masks[mask] = 1
		return
	}
	t.overflow++
}

// signature renders the accumulated state deterministically.
func (t *shapeTracker) signature() ShapeSignature {
	masks := make([]MaskCount, 0, len(t.masks)+1)
	for m, c := range t.masks {
		masks = append(masks, MaskCount{Mask: m, Count: c})
	}
	if t.overflow > 0 {
		masks = append(masks, MaskCount{Mask: overflowMask, Count: t.overflow})
	}
	slices.SortFunc(masks, func(a, b MaskCount) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Mask, b.Mask)
	})

	share := 0.0
	if t.total > 0 && len(masks) > 0 && masks[0].Mask != overflowMask {
		share = math.Round(float64(masks[0].Count)/float64(t.total)*100) / 100
	}
	return ShapeSignature{
		Masks:         masks,
		DominantShare: share,
		LengthMin:     t.lenMin,
		LengthMax:     t.lenMax,
	}
}

// dominant returns the signature's clear dominant mask, or false when
// the column has none: no values, an overflow bucket on top, or a
// share below the floor.
func (s ShapeSignature) dominant() (string, bool) {
	if len(s.Masks) == 0 || s.Masks[0].Mask == overflowMask || s.DominantShare < dominantShareFloor {
		return "", false
	}
	return s.Masks[0].Mask, true
}

// DriftsFrom reports whether the observed signature s has drifted from
// a baseline recorded earlier for the same column. It is a TRIPWIRE
// for wholesale semantic swaps (a column of "A-9" identifiers turning
// into free text), not a statistical drift detector: it fires only
// when both signatures have a clear dominant mask and the dominants
// differ, or when the observed dominant never appeared in the baseline
// at all. Rare new masks, length shifts, and share wobble stay quiet
// on purpose; a false positive parks a run and erodes trust in the
// signal.
func (s ShapeSignature) DriftsFrom(baseline ShapeSignature) bool {
	observed, ok := s.dominant()
	if !ok {
		return false
	}
	baselineDominant, baseOK := baseline.dominant()
	if baseOK && observed != baselineDominant {
		return true
	}
	if baseOK {
		return false
	}
	// The baseline has no clear dominant: only an observed dominant the
	// baseline NEVER saw counts as drift.
	for _, m := range baseline.Masks {
		if m.Mask == observed {
			return false
		}
	}
	return len(baseline.Masks) > 0
}
