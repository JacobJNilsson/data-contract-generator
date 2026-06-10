package fingerprint

import (
	"testing"
)

func object(format Format, profile *ParseProfile, fields ...Field) Object {
	return Object{AlgoVersion: AlgoVersion, Format: format, ParseProfile: profile, Fields: fields}
}

func csvProfile(delimiter string, hasHeader bool) *ParseProfile {
	encoding := "utf-8"
	return &ParseProfile{Delimiter: &delimiter, Encoding: &encoding, HasHeader: &hasHeader}
}

func TestCompareFormatMismatchDisqualifies(t *testing.T) {
	a := object(FormatCSV, csvProfile(",", true), Field{Name: "a", Type: TypeString})
	b := object(FormatJSON, nil, Field{Name: "a", Type: TypeString})
	if d := Compare(a, b); !d.Disqualified {
		t.Errorf("format mismatch did not disqualify")
	}
}

func TestCompareComponents(t *testing.T) {
	base := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
		Field{Name: "c", Type: TypeTemporal},
	)

	identical := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
		Field{Name: "c", Type: TypeTemporal},
	)
	d := Compare(base, identical)
	if d.Disqualified || d.ParseProfileDiffs != 0 || d.Jaccard != 0 || d.TypeChanges != 0 || d.AddedRemoved != 0 {
		t.Errorf("identical objects have nonzero distance: %+v", d)
	}

	differentDelimiter := object(FormatCSV, csvProfile(";", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
		Field{Name: "c", Type: TypeTemporal},
	)
	if d := Compare(base, differentDelimiter); d.ParseProfileDiffs != 1 {
		t.Errorf("delimiter diff: ParseProfileDiffs = %d, want 1", d.ParseProfileDiffs)
	}

	typeChanged := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeString},
		Field{Name: "c", Type: TypeTemporal},
	)
	if d := Compare(base, typeChanged); d.TypeChanges != 1 || d.AddedRemoved != 0 {
		t.Errorf("type change: %+v, want TypeChanges=1 AddedRemoved=0", d)
	}

	fieldAdded := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
		Field{Name: "c", Type: TypeTemporal},
		Field{Name: "d", Type: TypeString},
	)
	d = Compare(base, fieldAdded)
	if d.AddedRemoved != 1 {
		t.Errorf("added field: AddedRemoved = %d, want 1", d.AddedRemoved)
	}
	if want := 1 - 3.0/4.0; d.Jaccard != want {
		t.Errorf("added field: Jaccard = %v, want %v", d.Jaccard, want)
	}

	nilVsProfile := Compare(
		object(FormatXLSX, nil, Field{Name: "a", Type: TypeString}),
		object(FormatXLSX, csvProfile(",", true), Field{Name: "a", Type: TypeString}),
	)
	if nilVsProfile.ParseProfileDiffs != 3 {
		t.Errorf("nil vs full profile: ParseProfileDiffs = %d, want 3", nilVsProfile.ParseProfileDiffs)
	}

	profileVsNil := Compare(
		object(FormatXLSX, csvProfile(",", true), Field{Name: "a", Type: TypeString}),
		object(FormatXLSX, nil, Field{Name: "a", Type: TypeString}),
	)
	if profileVsNil.ParseProfileDiffs != 3 {
		t.Errorf("full profile vs nil: ParseProfileDiffs = %d, want 3", profileVsNil.ParseProfileDiffs)
	}

	empty := Compare(
		object(FormatXLSX, nil),
		object(FormatXLSX, nil),
	)
	if empty.Jaccard != 0 {
		t.Errorf("empty field sets: Jaccard = %v, want 0", empty.Jaccard)
	}
}

func TestDistanceLessPriorityOrder(t *testing.T) {
	cases := []struct {
		name    string
		closer  Distance
		farther Distance
	}{
		{"qualified beats disqualified", Distance{Jaccard: 0.9}, Distance{Disqualified: true}},
		{"parse profile beats jaccard", Distance{ParseProfileDiffs: 0, Jaccard: 0.9}, Distance{ParseProfileDiffs: 1, Jaccard: 0.1}},
		{"jaccard beats type changes", Distance{Jaccard: 0.1, TypeChanges: 5}, Distance{Jaccard: 0.2, TypeChanges: 0}},
		{"type changes beat added/removed", Distance{TypeChanges: 1, AddedRemoved: 5}, Distance{TypeChanges: 2, AddedRemoved: 0}},
		{"added/removed last", Distance{AddedRemoved: 1}, Distance{AddedRemoved: 2}},
	}
	for _, tc := range cases {
		if !tc.closer.Less(tc.farther) {
			t.Errorf("%s: closer not Less than farther", tc.name)
		}
		if tc.farther.Less(tc.closer) {
			t.Errorf("%s: farther Less than closer", tc.name)
		}
	}

	equal := Distance{Jaccard: 0.5}
	if equal.Less(equal) {
		t.Errorf("equal distances compare Less")
	}
	bothDisqualified := Distance{Disqualified: true}
	if bothDisqualified.Less(Distance{Disqualified: true}) {
		t.Errorf("disqualified compares Less than disqualified")
	}
}

// Acceptance: the near-neighbour ranking is deterministic and order-stable.
func TestNearNeighbourOrder(t *testing.T) {
	target := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
	)
	exact := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
	)
	near := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
		Field{Name: "c", Type: TypeString},
	)
	far := object(FormatCSV, csvProfile(";", false),
		Field{Name: "x", Type: TypeString},
	)
	wrongFormat := object(FormatJSON, nil,
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
	)
	tieWithNear := object(FormatCSV, csvProfile(",", true),
		Field{Name: "a", Type: TypeString},
		Field{Name: "b", Type: TypeNumber},
		Field{Name: "d", Type: TypeString},
	)

	candidates := []Object{far, near, wrongFormat, exact, tieWithNear}
	got := Rank(target, candidates)
	want := []int{3, 1, 4, 0} // exact, near, tie (original order), far; wrongFormat excluded
	if len(got) != len(want) {
		t.Fatalf("Rank returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Rank returned %v, want %v", got, want)
		}
	}

	for range 10 {
		again := Rank(target, candidates)
		for i := range want {
			if again[i] != want[i] {
				t.Fatalf("Rank not deterministic: %v then %v", got, again)
			}
		}
	}
}
