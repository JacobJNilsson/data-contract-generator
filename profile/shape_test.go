package profile

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestFormatMask(t *testing.T) {
	cases := []struct{ value, want string }{
		{"P-600", "A-9"},
		{"Kedjespannare K2", "A_A9"},
		{"75.50", "9.9"},
		{"2026-06-07", "9-9-9"},
		{"Åsa Öberg", "A_A"},
		{"  spaced  out  ", "_A_A_"},
		{"!!??!!", "!?!"},
		{"", ""},
		{"42", "9"},
		{"a1b2", "A9A9"},
		{"SE45 5000 0000", "A9_9_9"},
	}
	for _, c := range cases {
		if got := FormatMask(c.value); got != c.want {
			t.Errorf("FormatMask(%q) = %q, want %q", c.value, got, c.want)
		}
	}
}

// observeAll runs values through a fresh tracker and returns its
// signature.
func observeAll(values ...string) ShapeSignature {
	var tr shapeTracker
	for _, v := range values {
		tr.observe(v)
	}
	return tr.signature()
}

func TestShapeSignature(t *testing.T) {
	sig := observeAll("P-600", "P-601", "Kedjespannare K2", "P-700")
	if len(sig.Masks) != 2 || sig.Masks[0].Mask != "A-9" || sig.Masks[0].Count != 3 {
		t.Fatalf("Masks = %+v, want A-9 x3 first", sig.Masks)
	}
	if sig.DominantShare != 0.75 {
		t.Errorf("DominantShare = %v, want 0.75", sig.DominantShare)
	}
	if sig.LengthMin != 5 || sig.LengthMax != 16 {
		t.Errorf("(LengthMin, LengthMax) = (%d, %d), want (5, 16)", sig.LengthMin, sig.LengthMax)
	}

	empty := observeAll()
	if len(empty.Masks) != 0 || empty.DominantShare != 0 || empty.LengthMin != 0 || empty.LengthMax != 0 {
		t.Errorf("empty signature = %+v, want zero values", empty)
	}
}

// TestShapeSignatureRuneLengths: lengths count runes, not bytes, so
// Swedish text measures honestly.
func TestShapeSignatureRuneLengths(t *testing.T) {
	sig := observeAll("Åäö")
	if sig.LengthMin != 3 || sig.LengthMax != 3 {
		t.Errorf("(LengthMin, LengthMax) = (%d, %d), want (3, 3)", sig.LengthMin, sig.LengthMax)
	}
}

func TestShapeSignatureOverflowBucket(t *testing.T) {
	var tr shapeTracker
	// 32 distinct masks fill the table; two more land in the bucket.
	for i := 0; i < maxTrackedMasks; i++ {
		// Distinct run patterns: "9", "9-9", "9-9-9", ... give unique masks.
		v := "1"
		for j := 0; j < i; j++ {
			v += "-" + fmt.Sprint(j%10)
		}
		tr.observe(v)
	}
	tr.observe("!@#$%^")
	tr.observe("([{}])")
	sig := tr.signature()
	var overflow *MaskCount
	for i := range sig.Masks {
		if sig.Masks[i].Mask == overflowMask {
			overflow = &sig.Masks[i]
		}
	}
	if overflow == nil || overflow.Count != 2 {
		t.Fatalf("overflow bucket = %+v, want count 2", overflow)
	}

	// A signature whose top entry is the overflow bucket has no clear
	// dominant and never drifts.
	var flooded shapeTracker
	for i := 0; i < maxTrackedMasks; i++ {
		v := "1"
		for j := 0; j < i; j++ {
			v += "-" + fmt.Sprint(j%10)
		}
		flooded.observe(v)
	}
	for i := 0; i < 50; i++ {
		flooded.observe("!@#$%^")
	}
	fsig := flooded.signature()
	if fsig.Masks[0].Mask != overflowMask {
		t.Fatalf("flooded top mask = %q, want the overflow bucket", fsig.Masks[0].Mask)
	}
	if fsig.DominantShare != 0 {
		t.Errorf("flooded DominantShare = %v, want 0 (no dominance from the bucket)", fsig.DominantShare)
	}
	if fsig.DriftsFrom(observeAll("P-1", "P-2")) {
		t.Error("a signature without a clear dominant must never report drift")
	}
}

func TestShapeSignatureDeterminism(t *testing.T) {
	values := []string{"P-600", "Kedjespannare K2", "75.50", "P-601", "2026-06-07", "P-602"}
	a, _ := json.Marshal(observeAll(values...))
	b, _ := json.Marshal(observeAll(values...))
	if string(a) != string(b) {
		t.Errorf("signature JSON differs across runs:\n%s\n%s", a, b)
	}
}

func TestDriftsFrom(t *testing.T) {
	ids := observeAll("P-600", "P-601", "P-602", "P-603")              // dominant A-9
	names := observeAll("Kullager djupspar", "Drivrem bred", "Axel x") // dominant A_A
	cases := []struct {
		name               string
		observed, baseline ShapeSignature
		want               bool
	}{
		{"identifiers swapped to names", names, ids, true},
		{"names swapped to identifiers", ids, names, true},
		{"identical stays quiet", ids, observeAll("P-700", "P-701", "P-702"), false},
		{"rare new masks stay quiet", observeAll("P-1", "P-2", "P-3", "P-4", "P-5", "P-6", "P-7", "free text here"), ids, false},
		{"observed too mixed stays quiet", observeAll("P-1", "alpha beta", "9.9", "x-1", "yy zz", "7.5"), ids, false},
		{
			"baseline too mixed, observed dominant present, stays quiet",
			ids, observeAll("P-1", "alpha beta", "9.9", "P-2", "yy zz", "7.5"), false,
		},
		{
			"baseline too mixed, observed dominant absent, drifts",
			names, observeAll("P-1", "alpha", "9.9", "P-2", "01/02", "7.5"), true,
		},
		{"empty baseline never drifts", ids, observeAll(), false},
		{"empty observed never drifts", observeAll(), ids, false},
	}
	for _, c := range cases {
		if got := c.observed.DriftsFrom(c.baseline); got != c.want {
			t.Errorf("%s: DriftsFrom = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestDriftShareFloorBothSides pins the dominance floor: at exactly
// the floor dominance holds; just below it the comparator stays quiet.
func TestDriftShareFloorBothSides(t *testing.T) {
	ids := observeAll("P-600", "P-601", "P-602")
	// 3 of 5 = 0.6: exactly at the floor, dominance holds, drift fires.
	atFloor := observeAll("alpha beta", "gamma d", "epsilon z", "P-1", "9.9")
	if !atFloor.DriftsFrom(ids) {
		t.Error("dominance at exactly the floor must count")
	}
	// 3 of 6 = 0.5: below the floor, no clear dominant, no drift.
	belowFloor := observeAll("alpha beta", "gamma d", "epsilon z", "P-1", "9.9", "1-2")
	if belowFloor.DriftsFrom(ids) {
		t.Error("no drift verdict without a clear dominant")
	}
}
