package odcs

import (
	"encoding/json"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func ptrBool(b bool) *bool        { return &b }
func ptrInt(i int) *int           { return &i }
func ptrFloat(f float64) *float64 { return &f }

// fullContract exercises every field of every struct so a round-trip test
// over it proves each tag survives marshalling in both directions.
func fullContract() Contract {
	return Contract{
		APIVersion: APIVersion,
		Kind:       KindDataContract,
		ID:         "orders.public.orders",
		Version:    "1.0.0",
		Schema: []SchemaObject{
			{
				Name:         "orders",
				PhysicalName: "orders",
				PhysicalType: "table",
				Properties: []Property{
					{
						Name:               "id",
						LogicalType:        LogicalInteger,
						PhysicalType:       "bigint",
						Required:           ptrBool(true),
						PrimaryKey:         ptrBool(true),
						PrimaryKeyPosition: ptrInt(1),
						Unique:             ptrBool(true),
					},
					{
						Name:         "code",
						LogicalType:  LogicalString,
						PhysicalType: "text",
						Required:     ptrBool(false),
						LogicalTypeOptions: &LogicalTypeOptions{
							MinLength: ptrInt(1),
							MaxLength: ptrInt(32),
							Pattern:   "^[A-Z]+$",
							Format:    "uuid",
						},
					},
					{
						Name:        "amount",
						LogicalType: LogicalNumber,
						LogicalTypeOptions: &LogicalTypeOptions{
							Minimum:    ptrFloat(0),
							Maximum:    ptrFloat(1000),
							MultipleOf: ptrFloat(0.01),
						},
					},
					{
						Name:         "tags",
						LogicalType:  LogicalArray,
						PhysicalType: "text[]",
						Items: &Property{
							LogicalType:  LogicalString,
							PhysicalType: "text",
						},
					},
					{
						Name:         "status",
						LogicalType:  LogicalString,
						PhysicalType: "account_status",
						Quality: []Quality{
							{
								ID:     "status_valid_values",
								Type:   QualityLibrary,
								Metric: MetricInvalidValues,
								Arguments: map[string]any{
									"validValues": []any{"active", "closed", "pending"},
								},
								MustBe: 0,
								Unit:   "rows",
							},
						},
					},
					{
						Name:         "extras",
						LogicalType:  LogicalObject,
						PhysicalType: "jsonb",
					},
					{
						Name:        "created_on",
						LogicalType: LogicalDate,
						LogicalTypeOptions: &LogicalTypeOptions{
							Format: "date",
						},
						Quality: []Quality{
							{
								ID:                "freshness",
								Type:              QualityText,
								Metric:            MetricRowCount,
								MustNotBe:         0,
								MustBeGreaterThan: 1,
								MustBeLessThan:    100,
							},
						},
					},
					{
						Name:        "seen_at",
						LogicalType: LogicalTimestamp,
						LogicalTypeOptions: &LogicalTypeOptions{
							Format: "date-time",
						},
					},
					{
						Name:        "seen_time",
						LogicalType: LogicalTime,
					},
					{
						Name:        "ok",
						LogicalType: LogicalBoolean,
						Quality: []Quality{
							{
								ID:     "no_nulls",
								Type:   QualitySQL,
								Metric: MetricNullValues,
								MustBe: 0,
								Unit:   "rows",
							},
							{
								ID:     "no_dupes",
								Type:   QualityCustom,
								Metric: MetricDuplicateValues,
							},
							{
								ID:     "no_missing",
								Metric: MetricMissingValues,
							},
						},
					},
				},
			},
		},
	}
}

func TestJSONRoundTripBytes(t *testing.T) {
	c := fullContract()
	data, err := c.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	got, err := UnmarshalJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	// any-typed fields (MustBe family, Arguments values) decode to
	// float64/[]any, so re-marshal and compare bytes rather than the Go
	// values: a stable byte form is the contract this package guarantees.
	again, err := got.MarshalJSON()
	if err != nil {
		t.Fatalf("re-MarshalJSON: %v", err)
	}
	if string(data) != string(again) {
		t.Errorf("JSON round-trip not byte-stable:\nfirst:  %s\nsecond: %s", data, again)
	}
}

func TestYAMLRoundTripBytes(t *testing.T) {
	c := fullContract()
	data, err := c.MarshalYAML()
	if err != nil {
		t.Fatalf("MarshalYAML: %v", err)
	}
	got, err := UnmarshalYAML(data)
	if err != nil {
		t.Fatalf("UnmarshalYAML: %v", err)
	}
	again, err := got.MarshalYAML()
	if err != nil {
		t.Fatalf("re-MarshalYAML: %v", err)
	}
	if string(data) != string(again) {
		t.Errorf("YAML round-trip not byte-stable:\nfirst:  %s\nsecond: %s", data, again)
	}
}

func TestEmptyContractOmitsOptionalKeys(t *testing.T) {
	c := Contract{APIVersion: APIVersion, Kind: KindDataContract, ID: "x"}
	data, err := c.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	got := string(data)
	want := `{"apiVersion":"v3.1.0","kind":"DataContract","id":"x"}`
	if got != want {
		t.Errorf("empty contract JSON = %s, want %s", got, want)
	}
}

func TestJSONMarshalerSatisfied(t *testing.T) {
	// Encoding a Contract nested in another value must route through the
	// Contract's MarshalJSON without recursing.
	wrapper := struct {
		C Contract `json:"c"`
	}{C: Contract{APIVersion: APIVersion, Kind: KindDataContract, ID: "y"}}
	data, err := json.Marshal(wrapper)
	if err != nil {
		t.Fatalf("json.Marshal wrapper: %v", err)
	}
	want := `{"c":{"apiVersion":"v3.1.0","kind":"DataContract","id":"y"}}`
	if string(data) != want {
		t.Errorf("nested marshal = %s, want %s", data, want)
	}
}

func TestUnmarshalYAMLError(t *testing.T) {
	if _, err := UnmarshalYAML([]byte("\tnot: valid: yaml:")); err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestUnmarshalJSONError(t *testing.T) {
	if _, err := UnmarshalJSON([]byte("{not json")); err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestMarshalYAMLParsesBack(t *testing.T) {
	// Guard the YAML path specifically: marshal, then parse with a raw
	// yaml.Unmarshal to confirm the document is well-formed YAML and the
	// enum quality rule survives with its ordered labels.
	c := fullContract()
	data, err := c.MarshalYAML()
	if err != nil {
		t.Fatalf("MarshalYAML: %v", err)
	}
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatalf("raw yaml.Unmarshal: %v", err)
	}
	got, err := UnmarshalYAML(data)
	if err != nil {
		t.Fatalf("UnmarshalYAML: %v", err)
	}
	status := got.Schema[0].Properties[4]
	if status.Name != "status" {
		t.Fatalf("expected status property, got %q", status.Name)
	}
	labels, ok := status.Quality[0].Arguments["validValues"].([]any)
	if !ok {
		t.Fatalf("validValues not a list: %T", status.Quality[0].Arguments["validValues"])
	}
	want := []any{"active", "closed", "pending"}
	if !reflect.DeepEqual(labels, want) {
		t.Errorf("validValues = %v, want %v", labels, want)
	}
}
