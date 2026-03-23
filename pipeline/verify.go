package pipeline

import (
	"encoding/json"
	"fmt"
)

// validStepTypes enumerates all recognized step types.
var validStepTypes = map[StepType]bool{
	StepSource:      true,
	StepMapping:     true,
	StepAPICall:     true,
	StepManualLabel: true,
	StepLLMClassify: true,
	StepLookup:      true,
	StepDestination: true,
}

// Verify validates a pipeline contract structurally and checks for
// DAG integrity (no cycles, valid references, step-type-specific rules).
func Verify(data []byte) (valid bool, issues []string) {
	var c Contract
	if err := json.Unmarshal(data, &c); err != nil {
		return false, []string{fmt.Sprintf("failed to parse pipeline contract: %s", err)}
	}

	return VerifyContract(&c)
}

// VerifyContract validates an already-parsed pipeline contract.
func VerifyContract(c *Contract) (valid bool, issues []string) {
	if c.ContractType != "pipeline" {
		issues = append(issues, fmt.Sprintf("contract_type is %q, want \"pipeline\"", c.ContractType))
	}
	if c.PipelineID == "" {
		issues = append(issues, "missing pipeline_id")
	}
	if len(c.Steps) == 0 {
		issues = append(issues, "no steps defined")
		return false, issues
	}

	stepIDs := make(map[string]int) // id → index
	for i, s := range c.Steps {
		if s.ID == "" {
			issues = append(issues, fmt.Sprintf("step[%d]: missing id", i))
			continue
		}
		if _, dup := stepIDs[s.ID]; dup {
			issues = append(issues, fmt.Sprintf("step %q: duplicate id", s.ID))
		}
		stepIDs[s.ID] = i
	}

	hasSrc := false
	hasDst := false

	for _, s := range c.Steps {
		prefix := fmt.Sprintf("step %q", s.ID)
		if s.ID == "" {
			continue
		}

		if !validStepTypes[s.Type] {
			issues = append(issues, fmt.Sprintf("%s: unknown type %q", prefix, s.Type))
		}

		if s.Label == "" {
			issues = append(issues, fmt.Sprintf("%s: missing label", prefix))
		}

		// Validate dependency references.
		for _, dep := range s.DependsOn {
			if _, ok := stepIDs[dep]; !ok {
				issues = append(issues, fmt.Sprintf("%s: depends_on references unknown step %q", prefix, dep))
			}
		}

		// Validate inputs_from references.
		for ref, fields := range s.InputsFrom {
			if _, ok := stepIDs[ref]; !ok {
				issues = append(issues, fmt.Sprintf("%s: inputs_from references unknown step %q", prefix, ref))
			}
			if len(fields) == 0 {
				issues = append(issues, fmt.Sprintf("%s: inputs_from[%q] has no fields", prefix, ref))
			}
		}

		// Source steps should not have dependencies.
		if s.Type == StepSource && len(s.DependsOn) > 0 {
			issues = append(issues, fmt.Sprintf("%s: source steps should not have dependencies", prefix))
		}

		// Track source/destination presence.
		if s.Type == StepSource {
			hasSrc = true
		}
		if s.Type == StepDestination {
			hasDst = true
		}

		// Step-type-specific config validation.
		issues = append(issues, verifyStepConfig(prefix, s)...)
	}

	if !hasSrc {
		issues = append(issues, "pipeline has no source steps")
	}
	if !hasDst {
		issues = append(issues, "pipeline has no destination steps")
	}

	// Detect cycles.
	if cycle := detectCycle(c.Steps, stepIDs); cycle != "" {
		issues = append(issues, fmt.Sprintf("dependency cycle detected: %s", cycle))
	}

	return len(issues) == 0, issues
}

// verifyStepConfig validates the config field based on the step type.
func verifyStepConfig(prefix string, s Step) []string {
	var issues []string

	if len(s.Config) == 0 || string(s.Config) == "null" {
		issues = append(issues, fmt.Sprintf("%s: missing config", prefix))
		return issues
	}

	switch s.Type {
	case StepSource:
		var cfg SourceConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid source config: %s", prefix, err))
		} else if cfg.ContractRef == "" {
			issues = append(issues, fmt.Sprintf("%s: source config missing contract_ref", prefix))
		}

	case StepDestination:
		var cfg DestinationConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid destination config: %s", prefix, err))
		} else if cfg.ContractRef == "" {
			issues = append(issues, fmt.Sprintf("%s: destination config missing contract_ref", prefix))
		}

	case StepManualLabel:
		var cfg ManualLabelConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid manual_label config: %s", prefix, err))
		} else {
			if cfg.Field == "" {
				issues = append(issues, fmt.Sprintf("%s: manual_label config missing field", prefix))
			}
			if len(cfg.Options) == 0 && !cfg.AllowCustom {
				issues = append(issues, fmt.Sprintf("%s: manual_label config must have options or allow_custom", prefix))
			}
		}

	case StepLLMClassify:
		var cfg LLMClassifyConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid llm_classify config: %s", prefix, err))
		} else {
			if cfg.Field == "" {
				issues = append(issues, fmt.Sprintf("%s: llm_classify config missing field", prefix))
			}
			if len(cfg.Categories) == 0 {
				issues = append(issues, fmt.Sprintf("%s: llm_classify config must have at least one category", prefix))
			}
			if cfg.Prompt == "" {
				issues = append(issues, fmt.Sprintf("%s: llm_classify config missing prompt", prefix))
			}
		}

	case StepAPICall:
		var cfg APICallConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid api_call config: %s", prefix, err))
		} else {
			if cfg.Method == "" {
				issues = append(issues, fmt.Sprintf("%s: api_call config missing method", prefix))
			}
			if cfg.URL == "" {
				issues = append(issues, fmt.Sprintf("%s: api_call config missing url", prefix))
			}
		}

	case StepLookup:
		var cfg LookupConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid lookup config: %s", prefix, err))
		} else {
			if cfg.SourceRef == "" {
				issues = append(issues, fmt.Sprintf("%s: lookup config missing source_ref", prefix))
			}
			if cfg.KeyField == "" {
				issues = append(issues, fmt.Sprintf("%s: lookup config missing key_field", prefix))
			}
			if cfg.ValueField == "" {
				issues = append(issues, fmt.Sprintf("%s: lookup config missing value_field", prefix))
			}
		}

	case StepMapping:
		// MappingConfig uses json.RawMessage for mapping_groups, so we
		// just check that it parses and has source/dest refs.
		var cfg MappingConfig
		if err := json.Unmarshal(s.Config, &cfg); err != nil {
			issues = append(issues, fmt.Sprintf("%s: invalid mapping config: %s", prefix, err))
		} else if len(cfg.SourceRefs) == 0 {
			issues = append(issues, fmt.Sprintf("%s: mapping config missing source_refs", prefix))
		}
	}

	return issues
}

// detectCycle uses DFS to find cycles in the step dependency graph.
// Returns a description of the cycle or "" if none.
func detectCycle(steps []Step, stepIDs map[string]int) string {
	const (
		white = 0 // not visited
		gray  = 1 // in current DFS path
		black = 2 // fully processed
	)

	color := make(map[string]int, len(steps))

	var dfs func(id string) string
	dfs = func(id string) string {
		color[id] = gray

		idx, ok := stepIDs[id]
		if !ok {
			color[id] = black
			return ""
		}

		for _, dep := range steps[idx].DependsOn {
			switch color[dep] {
			case gray:
				// Found a cycle: dep is an ancestor of id in the DFS tree.
				return fmt.Sprintf("%s -> %s", id, dep)
			case white:
				if cycle := dfs(dep); cycle != "" {
					return cycle
				}
			}
		}

		color[id] = black
		return ""
	}

	for _, s := range steps {
		if s.ID != "" && color[s.ID] == white {
			if cycle := dfs(s.ID); cycle != "" {
				return cycle
			}
		}
	}

	return ""
}
