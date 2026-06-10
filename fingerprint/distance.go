package fingerprint

import "sort"

// Distance is the deterministic structural distance between two fingerprint
// objects, used to rank near-neighbour pipelines as authoring references on
// a cache miss. It is advice for the authoring agent, never an auto-match:
// only exact identity (hash + Match) dispatches a pipeline.
//
// Components are compared lexicographically in declaration order, mirroring
// the spec's priority: format mismatch disqualifies, then parse-profile
// diffs, then field-set Jaccard distance, then per-field type changes, then
// added/removed field count.
type Distance struct {
	Disqualified      bool
	ParseProfileDiffs int
	Jaccard           float64
	TypeChanges       int
	AddedRemoved      int
}

// Compare computes the structural distance between two objects. It is
// deliberately AlgoVersion-agnostic: distance is advice for reference
// selection, never an auto-match, and the re-fingerprint migration that
// accompanies an algo bump recomputes objects under the new rules.
func Compare(a, b Object) Distance {
	return compare(fieldTypes(a.Fields), a, b)
}

func compare(aTypes map[string]CanonicalType, a, b Object) Distance {
	if a.Format != b.Format {
		return Distance{Disqualified: true}
	}
	d := Distance{ParseProfileDiffs: parseProfileDiffs(a.ParseProfile, b.ParseProfile)}

	bTypes := fieldTypes(b.Fields)
	shared := 0
	for name, aType := range aTypes {
		bType, ok := bTypes[name]
		if !ok {
			continue
		}
		shared++
		if aType != bType {
			d.TypeChanges++
		}
	}
	union := len(aTypes) + len(bTypes) - shared
	d.AddedRemoved = union - shared
	if union > 0 {
		d.Jaccard = 1 - float64(shared)/float64(union)
	}
	return d
}

// Less reports whether d is strictly closer than other, comparing components
// in priority order. Disqualified distances are never closer than anything.
func (d Distance) Less(other Distance) bool {
	if d.Disqualified != other.Disqualified {
		return !d.Disqualified
	}
	if d.Disqualified {
		return false
	}
	if d.ParseProfileDiffs != other.ParseProfileDiffs {
		return d.ParseProfileDiffs < other.ParseProfileDiffs
	}
	if d.Jaccard != other.Jaccard {
		return d.Jaccard < other.Jaccard
	}
	if d.TypeChanges != other.TypeChanges {
		return d.TypeChanges < other.TypeChanges
	}
	return d.AddedRemoved < other.AddedRemoved
}

// Rank orders candidates by distance to target, closest first, and returns
// their indices into the candidates slice. Format-mismatched candidates are
// disqualified and excluded. Ties preserve candidate order, so the ranking
// is fully deterministic and order-stable.
func Rank(target Object, candidates []Object) []int {
	type ranked struct {
		index    int
		distance Distance
	}
	targetTypes := fieldTypes(target.Fields)
	rankedCandidates := make([]ranked, 0, len(candidates))
	for i, candidate := range candidates {
		d := compare(targetTypes, target, candidate)
		if d.Disqualified {
			continue
		}
		rankedCandidates = append(rankedCandidates, ranked{index: i, distance: d})
	}
	sort.SliceStable(rankedCandidates, func(i, j int) bool {
		return rankedCandidates[i].distance.Less(rankedCandidates[j].distance)
	})
	indices := make([]int, len(rankedCandidates))
	for i, r := range rankedCandidates {
		indices[i] = r.index
	}
	return indices
}

func parseProfileDiffs(a, b *ParseProfile) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		a = &ParseProfile{}
	}
	if b == nil {
		b = &ParseProfile{}
	}
	diffs := 0
	if !strPtrEqual(a.Delimiter, b.Delimiter) {
		diffs++
	}
	if !strPtrEqual(a.Encoding, b.Encoding) {
		diffs++
	}
	if !boolPtrEqual(a.HasHeader, b.HasHeader) {
		diffs++
	}
	return diffs
}

// fieldTypes keys fields by name. Canonical field names are unique by
// construction (canonicalFields disambiguates duplicates), so no entry is
// ever silently overwritten.
func fieldTypes(fields []Field) map[string]CanonicalType {
	types := make(map[string]CanonicalType, len(fields))
	for _, f := range fields {
		types[f.Name] = f.Type
	}
	return types
}
