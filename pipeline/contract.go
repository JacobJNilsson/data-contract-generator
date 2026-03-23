// Package pipeline defines multi-step data ingestion pipelines as directed
// acyclic graphs (DAGs) of typed steps. Each step can be a source, mapping,
// API call, manual label, LLM classification, lookup, or destination.
//
// The pipeline contract is a declarative specification -- it describes what
// to do, not how. Execution is handled by the ingestion agent.
package pipeline

import "encoding/json"

// Contract is a multi-step data ingestion pipeline.
type Contract struct {
	ContractType string         `json:"contract_type"` // always "pipeline"
	PipelineID   string         `json:"pipeline_id"`
	Steps        []Step         `json:"steps"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// Step is a single node in the pipeline DAG.
type Step struct {
	ID         string              `json:"id"`
	Type       StepType            `json:"type"`
	Label      string              `json:"label"`
	DependsOn  []string            `json:"depends_on"`
	InputsFrom map[string][]string `json:"inputs_from,omitempty"`
	Config     json.RawMessage     `json:"config"`
	Position   *Position           `json:"position,omitempty"`
}

// Position stores the visual position of a step on the DAG canvas.
type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// StepType identifies the kind of processing a step performs.
type StepType string

// Supported step types.
const (
	StepSource      StepType = "source"
	StepMapping     StepType = "mapping"
	StepAPICall     StepType = "api_call"
	StepManualLabel StepType = "manual_label"
	StepLLMClassify StepType = "llm_classify"
	StepLookup      StepType = "lookup"
	StepDestination StepType = "destination"
)

// SourceConfig configures a source step that loads data from an analyzed
// contract (CSV, JSON, Excel, API, PostgreSQL, Supabase).
type SourceConfig struct {
	ContractRef string `json:"contract_ref"`
}

// MappingConfig configures a mapping step that transforms fields using
// an existing transform contract.
type MappingConfig struct {
	SourceRefs      []string        `json:"source_refs"`
	DestinationRefs []string        `json:"destination_refs"`
	MappingGroups   json.RawMessage `json:"mapping_groups"`
}

// APICallConfig configures an API call step that sends data to an
// endpoint and captures response fields.
type APICallConfig struct {
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Headers        map[string]string `json:"headers,omitempty"`
	RequestMapping json.RawMessage   `json:"request_mapping,omitempty"`
	ResponseFields []string          `json:"response_fields,omitempty"`
}

// ManualLabelConfig configures a step that pauses for human labeling.
type ManualLabelConfig struct {
	Field        string   `json:"field"`
	Options      []string `json:"options"`
	Instructions string   `json:"instructions"`
	AllowCustom  bool     `json:"allow_custom"`
}

// LLMClassifyConfig configures a step that uses an LLM to classify
// field values into categories.
type LLMClassifyConfig struct {
	Field      string   `json:"field"`
	Categories []string `json:"categories"`
	Prompt     string   `json:"prompt"`
	Model      string   `json:"model"`
}

// LookupConfig configures a step that enriches data by looking up
// values from another source.
type LookupConfig struct {
	SourceRef  string `json:"source_ref"`
	KeyField   string `json:"key_field"`
	ValueField string `json:"value_field"`
}

// DestinationConfig configures a destination step that writes data
// to a target system.
type DestinationConfig struct {
	ContractRef string `json:"contract_ref"`
}
