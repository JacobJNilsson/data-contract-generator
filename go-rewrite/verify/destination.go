package verify

import (
	"encoding/json"
	"fmt"
)

// destinationContract mirrors contract.DataContract for validation.
type destinationContract struct {
	ID       string         `json:"id"`
	Schemas  []destSchema   `json:"schemas"`
	Metadata map[string]any `json:"metadata"`
}

type destSchema struct {
	Name            string         `json:"name"`
	Namespace       string         `json:"namespace"`
	RowCount        *int64         `json:"row_count"`
	Fields          []destField    `json:"fields"`
	ValidationRules destValidation `json:"validation_rules"`
	Issues          []string       `json:"issues"`
}

type destField struct {
	Name        string           `json:"name"`
	DataType    string           `json:"data_type"`
	Nullable    bool             `json:"nullable"`
	Description *string          `json:"description"`
	Constraints []destConstraint `json:"constraints"`
}

type destConstraint struct {
	Type           string  `json:"type"`
	ReferredTable  *string `json:"referred_table"`
	ReferredColumn *string `json:"referred_column"`
}

type destValidation struct {
	RequiredFields    []string `json:"required_fields"`
	UniqueConstraints []string `json:"unique_constraints"`
}

// Valid constraint types.
var validConstraintTypes = map[string]bool{
	"not_null": true, "unique": true, "primary_key": true, "foreign_key": true,
}

// Valid destination data types.
var validDestDataTypes = map[string]bool{
	"integer": true, "bigint": true, "smallint": true,
	"text": true, "varchar": true, "char": true,
	"boolean": true,
	"date":    true, "timestamp": true, "timestamptz": true,
	"time": true, "timetz": true,
	"numeric": true, "real": true, "double": true,
	"json": true, "jsonb": true,
	"uuid": true, "bytea": true,
}

// verifyDestination validates a destination contract (DatabaseContract).
func verifyDestination(data []byte) []string {
	var dc destinationContract
	if err := json.Unmarshal(data, &dc); err != nil {
		return []string{fmt.Sprintf("failed to parse destination contract: %s", err)}
	}

	var issues []string

	// --- structural checks ---

	if dc.ID == "" {
		issues = append(issues, "missing id")
	}

	if len(dc.Schemas) == 0 {
		issues = append(issues, "no schemas defined")
	}

	schemaNames := make(map[string]bool, len(dc.Schemas))
	for i, schema := range dc.Schemas {
		prefix := fmt.Sprintf("schemas[%d]", i)

		switch {
		case schema.Name == "":
			issues = append(issues, prefix+": missing name")
		case schemaNames[schema.Name]:
			issues = append(issues, fmt.Sprintf("%s: duplicate name %q", prefix, schema.Name))
		default:
			schemaNames[schema.Name] = true
		}

		if schema.RowCount != nil && *schema.RowCount < 0 {
			issues = append(issues, fmt.Sprintf("%s: row_count is negative", prefix))
		}

		issues = append(issues, verifyDestFields(prefix, schema)...)
	}

	return issues
}

// verifyDestFields validates a schema's fields and cross-references.
func verifyDestFields(prefix string, schema destSchema) []string {
	var issues []string

	fieldNames := make(map[string]bool, len(schema.Fields))
	for i, f := range schema.Fields {
		fp := fmt.Sprintf("%s.fields[%d]", prefix, i)

		switch {
		case f.Name == "":
			issues = append(issues, fp+": missing name")
		case fieldNames[f.Name]:
			issues = append(issues, fmt.Sprintf("%s: duplicate field name %q", fp, f.Name))
		default:
			fieldNames[f.Name] = true
		}

		if f.DataType == "" {
			issues = append(issues, fp+": missing data_type")
		} else if !validDestDataTypes[f.DataType] {
			issues = append(issues, fmt.Sprintf("%s: unknown data_type %q", fp, f.DataType))
		}

		// Validate constraints.
		for j, c := range f.Constraints {
			cp := fmt.Sprintf("%s.constraints[%d]", fp, j)
			issues = append(issues, verifyConstraint(cp, c)...)
		}
	}

	// --- semantic checks ---

	// required_fields must reference real field names.
	for _, name := range schema.ValidationRules.RequiredFields {
		if !fieldNames[name] {
			issues = append(issues, fmt.Sprintf(
				"%s.validation_rules: required_fields references unknown field %q", prefix, name))
		}
	}

	// unique_constraints must reference real field names.
	for _, name := range schema.ValidationRules.UniqueConstraints {
		if !fieldNames[name] {
			issues = append(issues, fmt.Sprintf(
				"%s.validation_rules: unique_constraints references unknown field %q", prefix, name))
		}
	}

	// A non-nullable field should not have a profile showing nulls.
	for _, f := range schema.Fields {
		if !f.Nullable {
			// Check if there's a not_null constraint (should be present).
			hasNotNull := false
			for _, c := range f.Constraints {
				if c.Type == "not_null" {
					hasNotNull = true
					break
				}
			}
			if len(f.Constraints) > 0 && !hasNotNull {
				issues = append(issues, fmt.Sprintf(
					"%s: field %q is not nullable but has no not_null constraint",
					prefix, f.Name))
			}
		}
	}

	return issues
}

// verifyConstraint validates a single constraint for coherence.
func verifyConstraint(prefix string, c destConstraint) []string {
	var issues []string

	if c.Type == "" {
		issues = append(issues, prefix+": missing type")
		return issues
	}

	if !validConstraintTypes[c.Type] {
		issues = append(issues, fmt.Sprintf("%s: unknown constraint type %q", prefix, c.Type))
		return issues
	}

	// Foreign key constraints must have both referred_table and referred_column.
	if c.Type == "foreign_key" {
		if c.ReferredTable == nil || *c.ReferredTable == "" {
			issues = append(issues, prefix+": foreign_key constraint missing referred_table")
		}
		if c.ReferredColumn == nil || *c.ReferredColumn == "" {
			issues = append(issues, prefix+": foreign_key constraint missing referred_column")
		}
	}

	return issues
}
