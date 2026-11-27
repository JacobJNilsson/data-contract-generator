package excelcontract

import (
	"reflect"
	"testing"
)

// wideTable is a six-column table: a header and two data rows.
var wideTable = [][]string{
	{"Code", "Name", "Q1", "Q2", "Q3", "Q4"},
	{"10", "Roads", "1", "2", "3", "4"},
	{"11", "Parks", "5", "6", "7", "8"},
}

// TestSegmentLabelPairCaption: two adjacent text cells above a wide
// header are caption, so the header names come from the header row.
func TestSegmentLabelPairCaption(t *testing.T) {
	rows := append([][]string{{"ID", "Plan 2021"}}, wideTable...)
	got := SegmentSheet(rows, nil)
	want := Segmentation{Tables: []Segment{{
		Detection: DetectionHeuristic,
		Caption:   "ID Plan 2021",
		HasHeader: true,
		StartRow:  1,
		EndRow:    3,
		StartCol:  0,
		EndCol:    5,
	}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SegmentSheet = %+v, want %+v", got, want)
	}
}

// TestSegmentTwoCellHeaderStays: a two-column table keeps its header,
// because its rows below are too narrow for a label pair to be caption.
func TestSegmentTwoCellHeaderStays(t *testing.T) {
	cases := []struct {
		name string
		rows [][]string
	}{
		{"number column", [][]string{{"Item", "Count"}, {"Bolt", "120"}, {"Nut", "118"}}},
		{"text columns", [][]string{{"Name", "Team"}, {"Alice", "Platform"}, {"Bob", "Data"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := SegmentSheet(c.rows, nil)
			want := Segmentation{Tables: []Segment{{
				Detection: DetectionHeuristic,
				HasHeader: true,
				StartRow:  0,
				EndRow:    2,
				StartCol:  0,
				EndCol:    1,
			}}}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("SegmentSheet = %+v, want %+v", got, want)
			}
		})
	}
}

// TestSegmentSplitLabelsAreNoCaption: two label cells with a gap between
// them are no label pair, so the row stays in the table.
func TestSegmentSplitLabelsAreNoCaption(t *testing.T) {
	rows := append([][]string{{"ID", "", "Plan 2021"}}, wideTable...)
	got := SegmentSheet(rows, nil)
	want := Segmentation{Tables: []Segment{{
		Detection: DetectionHeuristic,
		HasHeader: true,
		StartRow:  0,
		EndRow:    3,
		StartCol:  0,
		EndCol:    5,
	}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SegmentSheet = %+v, want %+v", got, want)
	}
}
