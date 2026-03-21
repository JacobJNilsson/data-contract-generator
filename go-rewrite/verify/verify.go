// Package verify validates data contracts (source, destination) for
// structural correctness and semantic coherence. Structural checks
// ensure required fields and valid types. Semantic checks catch
// cross-reference errors like validation rules referencing fields
// that don't exist in the schema.
package verify

import (
	"encoding/json"
	"fmt"
	"io"
)

// Result is the outcome of validating a contract.
type Result struct {
	Valid        bool     `json:"valid"`
	ContractType string   `json:"contract_type"`
	Issues       []string `json:"issues,omitempty"`
}

// Verify validates a contract from raw JSON bytes. It performs both
// structural and semantic validation.
func Verify(data []byte) Result {
	// Step 1: valid JSON?
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return Result{
			Valid:  false,
			Issues: []string{fmt.Sprintf("invalid JSON: %s", err)},
		}
	}

	// Step 2: contract_type present?
	ct, ok := raw["contract_type"].(string)
	if !ok || ct == "" {
		return Result{
			Valid:  false,
			Issues: []string{"missing or empty contract_type field"},
		}
	}

	// Step 3: dispatch to type-specific validator.
	var issues []string
	switch ct {
	case "source":
		issues = verifySource(data)
	case "destination":
		issues = verifyDestination(data)
	default:
		issues = []string{fmt.Sprintf("unknown contract_type: %q", ct)}
	}

	return Result{
		Valid:        len(issues) == 0,
		ContractType: ct,
		Issues:       issues,
	}
}

// Reader reads all bytes from r and validates the contract.
func Reader(r io.Reader) (Result, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Result{}, fmt.Errorf("read contract: %w", err)
	}
	return Verify(data), nil
}
