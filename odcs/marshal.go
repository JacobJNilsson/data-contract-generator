package odcs

import (
	"encoding/json"

	"gopkg.in/yaml.v3"
)

// MarshalYAML renders the contract as an ODCS YAML document.
func (c Contract) MarshalYAML() ([]byte, error) {
	return yaml.Marshal(c)
}

// UnmarshalYAML parses an ODCS YAML document into a contract.
func UnmarshalYAML(data []byte) (Contract, error) {
	var c Contract
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Contract{}, err
	}
	return c, nil
}

// MarshalJSON renders the contract as an ODCS JSON document. It satisfies
// json.Marshaler, so json.Marshal of a Contract (including a Contract
// nested in another value) routes through here. The alias type strips the
// method set before the inner Marshal call, so the struct tags drive the
// encoding and there is no recursion.
func (c Contract) MarshalJSON() ([]byte, error) {
	type alias Contract
	return json.Marshal(alias(c))
}

// UnmarshalJSON parses an ODCS JSON document into a contract.
func UnmarshalJSON(data []byte) (Contract, error) {
	var c Contract
	if err := json.Unmarshal(data, &c); err != nil {
		return Contract{}, err
	}
	return c, nil
}
