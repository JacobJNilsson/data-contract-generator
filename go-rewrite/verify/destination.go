package verify

import (
	"encoding/json"
	"fmt"
)

// destinationContract mirrors pgcontract.DatabaseContract for validation.
type destinationContract struct {
	DatabaseID string         `json:"database_id"`
	Tables     []destTable    `json:"tables"`
	Metadata   map[string]any `json:"metadata"`
}

type destTable struct {
	TableName       string         `json:"table_name"`
	Schema          string         `json:"schema"`
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

	if dc.DatabaseID == "" {
		issues = append(issues, "missing database_id")
	}

	if len(dc.Tables) == 0 {
		issues = append(issues, "no tables defined")
	}

	tableNames := make(map[string]bool, len(dc.Tables))
	for i, table := range dc.Tables {
		prefix := fmt.Sprintf("tables[%d]", i)

		switch {
		case table.TableName == "":
			issues = append(issues, prefix+": missing table_name")
		case tableNames[table.TableName]:
			issues = append(issues, fmt.Sprintf("%s: duplicate table_name %q", prefix, table.TableName))
		default:
			tableNames[table.TableName] = true
		}

		if table.RowCount != nil && *table.RowCount < 0 {
			issues = append(issues, fmt.Sprintf("%s: row_count is negative", prefix))
		}

		issues = append(issues, verifyDestFields(prefix, table)...)
	}

	return issues
}

// verifyDestFields validates a table's fields and cross-references.
func verifyDestFields(prefix string, table destTable) []string {
	var issues []string

	fieldNames := make(map[string]bool, len(table.Fields))
	for i, f := range table.Fields {
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
	for _, name := range table.ValidationRules.RequiredFields {
		if !fieldNames[name] {
			issues = append(issues, fmt.Sprintf(
				"%s.validation_rules: required_fields references unknown field %q", prefix, name))
		}
	}

	// unique_constraints must reference real field names.
	for _, name := range table.ValidationRules.UniqueConstraints {
		if !fieldNames[name] {
			issues = append(issues, fmt.Sprintf(
				"%s.validation_rules: unique_constraints references unknown field %q", prefix, name))
		}
	}

	// A non-nullable field should not have a profile showing nulls.
	for _, f := range table.Fields {
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
