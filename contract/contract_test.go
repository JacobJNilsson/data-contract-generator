package contract

import (
	"encoding/json"
	"testing"
)

// TestFieldProfileWithoutShape: a profile without a shape writes no shape
// key, so contracts from analyzers that record no shape keep their bytes.
func TestFieldProfileWithoutShape(t *testing.T) {
	got, err := json.Marshal(FieldProfile{})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"null_count":0,"null_percentage":0,"distinct_count":0,"distinct_count_capped":false,"min_value":null,"max_value":null,"sample_size":0}`
	if string(got) != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}
}

func TestFieldProfileWithShape(t *testing.T) {
	p := FieldProfile{Shape: &ShapeSignature{
		Masks:         []MaskCount{{Mask: "9", Count: 2}},
		DominantShare: 1,
		LengthMin:     1,
		LengthMax:     2,
	}}
	got, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"null_count":0,"null_percentage":0,"distinct_count":0,"distinct_count_capped":false,"min_value":null,"max_value":null,"sample_size":0,"shape":{"masks":[{"mask":"9","count":2}],"dominant_share":1,"length_min":1,"length_max":2}}`
	if string(got) != want {
		t.Errorf("JSON = %s, want %s", got, want)
	}
}
