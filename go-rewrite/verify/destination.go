package verify

import (
	"encoding/json"
	"fmt"

	"github.com/jacobjnilsson/contract-gen/contract"
)

// Valid constraint types.
var validConstraintTypes = map[contract.ConstraintType]bool{
	contract.ConstraintNotNull:    true,
	contract.ConstraintUnique:     true,
	contract.ConstraintPrimaryKey: true,
	contract.ConstraintForeignKey: true,
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

// verifyDestination validates a destination DataContract.
func verifyDestination(data []byte) []string {
	var dc contract.DataContract
	if err := json.Unmarshal(data, &dc); err != nil {
		return []string{fmt.Sprintf("failed to parse destination contract: %s", err)}
	}

	var issues []string

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

		issues = append(issues, verifySchemaFields(prefix, schema)...)
	}

	return issues
}

// verifySchemaFields validates a schema's fields and cross-references.
func verifySchemaFields(prefix string, schema contract.SchemaContract) []string {
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

		for j, c := range f.Constraints {
			cp := fmt.Sprintf("%s.constraints[%d]", fp, j)
			issues = append(issues, verifyConstraint(cp, c)...)
		}
	}

	// --- semantic checks ---

	for _, name := range schema.ValidationRules.RequiredFields {
		if !fieldNames[name] {
			issues = append(issues, fmt.Sprintf(
				"%s.validation_rules: required_fields references unknown field %q", prefix, name))
		}
	}

	for _, name := range schema.ValidationRules.UniqueConstraints {
		if !fieldNames[name] {
			issues = append(issues, fmt.Sprintf(
				"%s.validation_rules: unique_constraints references unknown field %q", prefix, name))
		}
	}

	for _, f := range schema.Fields {
		if !f.Nullable {
			hasNotNull := false
			for _, c := range f.Constraints {
				if c.Type == contract.ConstraintNotNull {
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
func verifyConstraint(prefix string, c contract.FieldConstraint) []string {
	var issues []string

	if c.Type == "" {
		issues = append(issues, prefix+": missing type")
		return issues
	}

	if !validConstraintTypes[c.Type] {
		issues = append(issues, fmt.Sprintf("%s: unknown constraint type %q", prefix, c.Type))
		return issues
	}

	if c.Type == contract.ConstraintForeignKey {
		if c.ReferredTable == nil || *c.ReferredTable == "" {
			issues = append(issues, prefix+": foreign_key constraint missing referred_table")
		}
		if c.ReferredColumn == nil || *c.ReferredColumn == "" {
			issues = append(issues, prefix+": foreign_key constraint missing referred_column")
		}
	}

	return issues
}
