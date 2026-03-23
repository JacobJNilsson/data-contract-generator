package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
)

// --- helpers ----------------------------------------------------------------

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func validPipeline() *Contract {
	return &Contract{
		ContractType: "pipeline",
		PipelineID:   "test-pipeline",
		Steps: []Step{
			{
				ID:    "src",
				Type:  StepSource,
				Label: "CSV orders",
				Config: mustJSON(SourceConfig{
					ContractRef: "orders.csv",
				}),
			},
			{
				ID:        "map",
				Type:      StepMapping,
				Label:     "Map to API",
				DependsOn: []string{"src"},
				Config: mustJSON(MappingConfig{
					SourceRefs: []string{"orders.csv"},
				}),
			},
			{
				ID:        "dst",
				Type:      StepDestination,
				Label:     "POST /orders",
				DependsOn: []string{"map"},
				Config: mustJSON(DestinationConfig{
					ContractRef: "api-orders",
				}),
			},
		},
	}
}

func assertValid(t *testing.T, c *Contract) {
	t.Helper()
	valid, issues := VerifyContract(c)
	if !valid {
		t.Errorf("expected valid, got issues: %v", issues)
	}
}

func assertInvalid(t *testing.T, c *Contract, substring string) {
	t.Helper()
	valid, issues := VerifyContract(c)
	if valid {
		t.Fatalf("expected invalid, but got valid")
	}
	for _, issue := range issues {
		if strings.Contains(issue, substring) {
			return
		}
	}
	t.Errorf("expected issue containing %q, got: %v", substring, issues)
}

// --- valid pipelines --------------------------------------------------------

func TestVerify_ValidPipeline(t *testing.T) {
	assertValid(t, validPipeline())
}

func TestVerify_ValidComplexPipeline(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "complex",
		Steps: []Step{
			{ID: "csv", Type: StepSource, Label: "CSV data", Config: mustJSON(SourceConfig{ContractRef: "data.csv"})},
			{ID: "label", Type: StepManualLabel, Label: "Categorize", DependsOn: []string{"csv"}, Config: mustJSON(ManualLabelConfig{Field: "category", Options: []string{"A", "B"}})},
			{ID: "classify", Type: StepLLMClassify, Label: "LLM classify", DependsOn: []string{"label"}, Config: mustJSON(LLMClassifyConfig{Field: "type", Categories: []string{"X", "Y"}, Prompt: "Classify this", Model: "gpt-4o-mini"})},
			{ID: "lookup", Type: StepLookup, Label: "Enrich", DependsOn: []string{"classify"}, Config: mustJSON(LookupConfig{SourceRef: "countries", KeyField: "country_name", ValueField: "country_code"})},
			{ID: "map", Type: StepMapping, Label: "Map fields", DependsOn: []string{"lookup"}, Config: mustJSON(MappingConfig{SourceRefs: []string{"data.csv"}})},
			{ID: "api", Type: StepAPICall, Label: "POST /data", DependsOn: []string{"map"}, Config: mustJSON(APICallConfig{Method: "POST", URL: "https://api.example.com/data"})},
			{ID: "dst", Type: StepDestination, Label: "Target DB", DependsOn: []string{"api"}, Config: mustJSON(DestinationConfig{ContractRef: "target-db"})},
		},
	}
	assertValid(t, c)
}

func TestVerify_ParallelSources(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "parallel",
		Steps: []Step{
			{ID: "src1", Type: StepSource, Label: "CSV 1", Config: mustJSON(SourceConfig{ContractRef: "a.csv"})},
			{ID: "src2", Type: StepSource, Label: "CSV 2", Config: mustJSON(SourceConfig{ContractRef: "b.csv"})},
			{ID: "map", Type: StepMapping, Label: "Combine", DependsOn: []string{"src1", "src2"}, Config: mustJSON(MappingConfig{SourceRefs: []string{"a.csv", "b.csv"}})},
			{ID: "dst", Type: StepDestination, Label: "DB", DependsOn: []string{"map"}, Config: mustJSON(DestinationConfig{ContractRef: "db"})},
		},
	}
	assertValid(t, c)
}

func TestVerify_InputsFrom(t *testing.T) {
	c := validPipeline()
	c.Steps[1].InputsFrom = map[string][]string{
		"src": {"order_id", "customer_name"},
	}
	assertValid(t, c)
}

func TestVerify_WithPosition(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Position = &Position{X: 100, Y: 200}
	assertValid(t, c)
}

// --- structural errors ------------------------------------------------------

func TestVerify_BadJSON(t *testing.T) {
	valid, issues := Verify([]byte("not json"))
	if valid {
		t.Fatal("expected invalid")
	}
	if !strings.Contains(issues[0], "failed to parse") {
		t.Errorf("unexpected issue: %s", issues[0])
	}
}

func TestVerify_WrongContractType(t *testing.T) {
	c := validPipeline()
	c.ContractType = "transformation"
	assertInvalid(t, c, "contract_type")
}

func TestVerify_MissingPipelineID(t *testing.T) {
	c := validPipeline()
	c.PipelineID = ""
	assertInvalid(t, c, "missing pipeline_id")
}

func TestVerify_NoSteps(t *testing.T) {
	c := &Contract{ContractType: "pipeline", PipelineID: "p", Steps: []Step{}}
	assertInvalid(t, c, "no steps defined")
}

func TestVerify_MissingStepID(t *testing.T) {
	c := validPipeline()
	c.Steps[0].ID = ""
	assertInvalid(t, c, "missing id")
}

func TestVerify_DuplicateStepID(t *testing.T) {
	c := validPipeline()
	c.Steps[1].ID = "src" // duplicate
	assertInvalid(t, c, "duplicate id")
}

func TestVerify_UnknownStepType(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Type = "magic"
	assertInvalid(t, c, "unknown type")
}

func TestVerify_MissingLabel(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Label = ""
	assertInvalid(t, c, "missing label")
}

func TestVerify_UnknownDependency(t *testing.T) {
	c := validPipeline()
	c.Steps[1].DependsOn = []string{"nonexistent"}
	assertInvalid(t, c, "unknown step")
}

func TestVerify_UnknownInputsFrom(t *testing.T) {
	c := validPipeline()
	c.Steps[1].InputsFrom = map[string][]string{"ghost": {"field"}}
	assertInvalid(t, c, "unknown step")
}

func TestVerify_EmptyInputsFromFields(t *testing.T) {
	c := validPipeline()
	c.Steps[1].InputsFrom = map[string][]string{"src": {}}
	assertInvalid(t, c, "has no fields")
}

func TestVerify_SourceWithDependencies(t *testing.T) {
	c := validPipeline()
	c.Steps[0].DependsOn = []string{"map"}
	assertInvalid(t, c, "source steps should not have dependencies")
}

func TestVerify_NoSourceSteps(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "p",
		Steps: []Step{
			{ID: "dst", Type: StepDestination, Label: "DB", Config: mustJSON(DestinationConfig{ContractRef: "db"})},
		},
	}
	assertInvalid(t, c, "no source steps")
}

func TestVerify_NoDestinationSteps(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "p",
		Steps: []Step{
			{ID: "src", Type: StepSource, Label: "CSV", Config: mustJSON(SourceConfig{ContractRef: "data.csv"})},
		},
	}
	assertInvalid(t, c, "no destination steps")
}

func TestVerify_MissingConfig(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Config = nil
	assertInvalid(t, c, "missing config")
}

func TestVerify_NullConfig(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Config = json.RawMessage("null")
	assertInvalid(t, c, "missing config")
}

// --- cycle detection --------------------------------------------------------

func TestVerify_DirectCycle(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "p",
		Steps: []Step{
			{ID: "a", Type: StepSource, Label: "A", DependsOn: []string{"b"}, Config: mustJSON(SourceConfig{ContractRef: "a"})},
			{ID: "b", Type: StepDestination, Label: "B", DependsOn: []string{"a"}, Config: mustJSON(DestinationConfig{ContractRef: "b"})},
		},
	}
	assertInvalid(t, c, "cycle")
}

func TestVerify_IndirectCycle(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "p",
		Steps: []Step{
			{ID: "a", Type: StepSource, Label: "A", Config: mustJSON(SourceConfig{ContractRef: "a"})},
			{ID: "b", Type: StepMapping, Label: "B", DependsOn: []string{"a", "c"}, Config: mustJSON(MappingConfig{SourceRefs: []string{"a"}})},
			{ID: "c", Type: StepMapping, Label: "C", DependsOn: []string{"b"}, Config: mustJSON(MappingConfig{SourceRefs: []string{"a"}})},
			{ID: "d", Type: StepDestination, Label: "D", DependsOn: []string{"c"}, Config: mustJSON(DestinationConfig{ContractRef: "d"})},
		},
	}
	assertInvalid(t, c, "cycle")
}

func TestVerify_SelfCycle(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "p",
		Steps: []Step{
			{ID: "src", Type: StepSource, Label: "S", Config: mustJSON(SourceConfig{ContractRef: "s"})},
			{ID: "a", Type: StepMapping, Label: "A", DependsOn: []string{"a"}, Config: mustJSON(MappingConfig{SourceRefs: []string{"s"}})},
			{ID: "dst", Type: StepDestination, Label: "D", DependsOn: []string{"a"}, Config: mustJSON(DestinationConfig{ContractRef: "d"})},
		},
	}
	assertInvalid(t, c, "cycle")
}

// --- step-specific config validation ----------------------------------------

func TestVerify_SourceMissingContractRef(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Config = mustJSON(SourceConfig{ContractRef: ""})
	assertInvalid(t, c, "missing contract_ref")
}

func TestVerify_DestinationMissingContractRef(t *testing.T) {
	c := validPipeline()
	c.Steps[2].Config = mustJSON(DestinationConfig{ContractRef: ""})
	assertInvalid(t, c, "missing contract_ref")
}

func TestVerify_ManualLabelMissingField(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "label", Type: StepManualLabel, Label: "Label", DependsOn: []string{"src"},
		Config: mustJSON(ManualLabelConfig{Field: "", Options: []string{"A"}}),
	}
	assertInvalid(t, c, "missing field")
}

func TestVerify_ManualLabelNoOptionsNoCustom(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "label", Type: StepManualLabel, Label: "Label", DependsOn: []string{"src"},
		Config: mustJSON(ManualLabelConfig{Field: "cat", Options: nil, AllowCustom: false}),
	}
	assertInvalid(t, c, "must have options or allow_custom")
}

func TestVerify_ManualLabelAllowCustomNoOptions(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "label", Type: StepManualLabel, Label: "Label", DependsOn: []string{"src"},
		Config: mustJSON(ManualLabelConfig{Field: "cat", AllowCustom: true}),
	}
	c.Steps[2].DependsOn = []string{"label"} // fix dest dependency
	// Should be valid: allow_custom without predefined options is fine.
	assertValid(t, c)
}

func TestVerify_LLMClassifyMissingField(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "llm", Type: StepLLMClassify, Label: "Classify", DependsOn: []string{"src"},
		Config: mustJSON(LLMClassifyConfig{Field: "", Categories: []string{"A"}, Prompt: "p"}),
	}
	assertInvalid(t, c, "missing field")
}

func TestVerify_LLMClassifyNoCategories(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "llm", Type: StepLLMClassify, Label: "Classify", DependsOn: []string{"src"},
		Config: mustJSON(LLMClassifyConfig{Field: "f", Categories: nil, Prompt: "p"}),
	}
	assertInvalid(t, c, "at least one category")
}

func TestVerify_LLMClassifyMissingPrompt(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "llm", Type: StepLLMClassify, Label: "Classify", DependsOn: []string{"src"},
		Config: mustJSON(LLMClassifyConfig{Field: "f", Categories: []string{"A"}, Prompt: ""}),
	}
	assertInvalid(t, c, "missing prompt")
}

func TestVerify_APICallMissingMethod(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "api", Type: StepAPICall, Label: "Call", DependsOn: []string{"src"},
		Config: mustJSON(APICallConfig{Method: "", URL: "https://x.com"}),
	}
	assertInvalid(t, c, "missing method")
}

func TestVerify_APICallMissingURL(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "api", Type: StepAPICall, Label: "Call", DependsOn: []string{"src"},
		Config: mustJSON(APICallConfig{Method: "POST", URL: ""}),
	}
	assertInvalid(t, c, "missing url")
}

func TestVerify_LookupMissingSourceRef(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "lk", Type: StepLookup, Label: "Lookup", DependsOn: []string{"src"},
		Config: mustJSON(LookupConfig{SourceRef: "", KeyField: "k", ValueField: "v"}),
	}
	assertInvalid(t, c, "missing source_ref")
}

func TestVerify_LookupMissingKeyField(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "lk", Type: StepLookup, Label: "Lookup", DependsOn: []string{"src"},
		Config: mustJSON(LookupConfig{SourceRef: "ref", KeyField: "", ValueField: "v"}),
	}
	assertInvalid(t, c, "missing key_field")
}

func TestVerify_LookupMissingValueField(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{
		ID: "lk", Type: StepLookup, Label: "Lookup", DependsOn: []string{"src"},
		Config: mustJSON(LookupConfig{SourceRef: "ref", KeyField: "k", ValueField: ""}),
	}
	assertInvalid(t, c, "missing value_field")
}

func TestVerify_MappingMissingSourceRefs(t *testing.T) {
	c := validPipeline()
	c.Steps[1].Config = mustJSON(MappingConfig{SourceRefs: nil})
	assertInvalid(t, c, "missing source_refs")
}

func TestVerify_InvalidSourceConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[0].Config = json.RawMessage(`{bad}`)
	assertInvalid(t, c, "invalid source config")
}

func TestVerify_InvalidDestConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[2].Config = json.RawMessage(`{bad}`)
	assertInvalid(t, c, "invalid destination config")
}

func TestVerify_InvalidManualLabelConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{ID: "ml", Type: StepManualLabel, Label: "ML", DependsOn: []string{"src"}, Config: json.RawMessage(`{bad}`)}
	c.Steps[2].DependsOn = []string{"ml"}
	assertInvalid(t, c, "invalid manual_label config")
}

func TestVerify_InvalidLLMClassifyConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{ID: "llm", Type: StepLLMClassify, Label: "LLM", DependsOn: []string{"src"}, Config: json.RawMessage(`{bad}`)}
	c.Steps[2].DependsOn = []string{"llm"}
	assertInvalid(t, c, "invalid llm_classify config")
}

func TestVerify_InvalidAPICallConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{ID: "api", Type: StepAPICall, Label: "API", DependsOn: []string{"src"}, Config: json.RawMessage(`{bad}`)}
	c.Steps[2].DependsOn = []string{"api"}
	assertInvalid(t, c, "invalid api_call config")
}

func TestVerify_InvalidLookupConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[1] = Step{ID: "lk", Type: StepLookup, Label: "LK", DependsOn: []string{"src"}, Config: json.RawMessage(`{bad}`)}
	c.Steps[2].DependsOn = []string{"lk"}
	assertInvalid(t, c, "invalid lookup config")
}

func TestVerify_InvalidMappingConfigJSON(t *testing.T) {
	c := validPipeline()
	c.Steps[1].Config = json.RawMessage(`{bad}`)
	assertInvalid(t, c, "invalid mapping config")
}

// --- JSON roundtrip ---------------------------------------------------------

func TestVerify_JSONRoundtrip(t *testing.T) {
	c := validPipeline()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	valid, issues := Verify(data)
	if !valid {
		t.Errorf("roundtrip failed: %v", issues)
	}
}

func TestVerify_ComplexJSONRoundtrip(t *testing.T) {
	c := &Contract{
		ContractType: "pipeline",
		PipelineID:   "complex",
		Steps: []Step{
			{
				ID: "src", Type: StepSource, Label: "Data",
				Config:   mustJSON(SourceConfig{ContractRef: "data.csv"}),
				Position: &Position{X: 0, Y: 0},
			},
			{
				ID: "label", Type: StepManualLabel, Label: "Categorize",
				DependsOn: []string{"src"},
				Config:    mustJSON(ManualLabelConfig{Field: "cat", Options: []string{"A", "B"}, Instructions: "Pick one", AllowCustom: true}),
				Position:  &Position{X: 200, Y: 0},
			},
			{
				ID: "api", Type: StepAPICall, Label: "POST",
				DependsOn:  []string{"label"},
				InputsFrom: map[string][]string{"src": {"id"}, "label": {"cat"}},
				Config:     mustJSON(APICallConfig{Method: "POST", URL: "https://api.example.com", Headers: map[string]string{"Authorization": "Bearer xyz"}, ResponseFields: []string{"created_id"}}),
				Position:   &Position{X: 400, Y: 0},
			},
			{
				ID: "dst", Type: StepDestination, Label: "DB",
				DependsOn: []string{"api"},
				Config:    mustJSON(DestinationConfig{ContractRef: "target"}),
				Position:  &Position{X: 600, Y: 0},
			},
		},
		Metadata: map[string]any{"created_by": "test"},
	}

	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	valid, issues := Verify(data)
	if !valid {
		t.Errorf("roundtrip failed: %v", issues)
	}

	// Verify the JSON has the expected structure.
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw["contract_type"] != "pipeline" {
		t.Errorf("contract_type = %v", raw["contract_type"])
	}
	if raw["pipeline_id"] != "complex" {
		t.Errorf("pipeline_id = %v", raw["pipeline_id"])
	}
	steps := raw["steps"].([]any)
	if len(steps) != 4 {
		t.Errorf("steps = %d, want 4", len(steps))
	}
}
