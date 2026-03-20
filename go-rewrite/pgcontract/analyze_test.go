package pgcontract

import (
	"reflect"
	"testing"
)

func TestNormalizeDataType(t *testing.T) {
	tests := []struct {
		name   string
		pgType string
		want   string
	}{
		{"integer", "integer", "integer"},
		{"int", "int", "integer"},
		{"int4", "int4", "integer"},
		{"bigint", "bigint", "bigint"},
		{"int8", "int8", "bigint"},
		{"smallint", "smallint", "smallint"},
		{"text", "text", "text"},
		{"character varying", "character varying", "varchar"},
		{"varchar with length", "varchar(255)", "varchar"},
		{"character", "character", "char"},
		{"char with length", "char(10)", "char"},
		{"boolean", "boolean", "boolean"},
		{"bool", "bool", "boolean"},
		{"date", "date", "date"},
		{"timestamp", "timestamp", "timestamp"},
		{"timestamp without time zone", "timestamp without time zone", "timestamp"},
		{"timestamp with time zone", "timestamp with time zone", "timestamptz"},
		{"timestamptz", "timestamptz", "timestamptz"},
		{"numeric", "numeric", "numeric"},
		{"numeric with precision", "numeric(10,2)", "numeric"},
		{"decimal", "decimal", "numeric"},
		{"real", "real", "real"},
		{"double precision", "double precision", "double"},
		{"json", "json", "json"},
		{"jsonb", "jsonb", "jsonb"},
		{"uuid", "uuid", "uuid"},
		{"bytea", "bytea", "bytea"},
		{"unknown type", "custom_type", "custom_type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeDataType(tt.pgType)
			if got != tt.want {
				t.Errorf("normalizeDataType(%q) = %v, want %v", tt.pgType, got, tt.want)
			}
		})
	}
}

func TestSanitizeConnString(t *testing.T) {
	tests := []struct {
		name       string
		connString string
		want       string
	}{
		{
			name:       "standard connection string",
			connString: "postgres://myuser:mypassword@localhost:5432/mydb",
			want:       "myuser@localhost:5432/mydb",
		},
		{
			name:       "connection string with different port",
			connString: "postgres://admin:secret@example.com:5433/testdb",
			want:       "admin@example.com:5433/testdb",
		},
		{
			name:       "invalid connection string",
			connString: "not a valid connection string",
			want:       "[connection string]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeConnString(tt.connString)
			if got != tt.want {
				t.Errorf("sanitizeConnString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyConstraints(t *testing.T) {
	tests := []struct {
		name        string
		fields      []FieldDefinition
		constraints []constraintInfo
		want        []FieldDefinition
	}{
		{
			name: "primary key constraint",
			fields: []FieldDefinition{
				{Name: "id", DataType: "integer", Nullable: false},
			},
			constraints: []constraintInfo{
				{ColumnName: "id", ConstraintType: "PRIMARY KEY"},
			},
			want: []FieldDefinition{
				{
					Name:     "id",
					DataType: "integer",
					Nullable: false,
					Constraints: []FieldConstraint{
						{Type: ConstraintPrimaryKey},
						{Type: ConstraintNotNull},
					},
				},
			},
		},
		{
			name: "foreign key constraint",
			fields: []FieldDefinition{
				{Name: "user_id", DataType: "integer", Nullable: true},
			},
			constraints: []constraintInfo{
				{
					ColumnName:     "user_id",
					ConstraintType: "FOREIGN KEY",
					RefTable:       strPtr("users"),
					RefColumn:      strPtr("id"),
				},
			},
			want: []FieldDefinition{
				{
					Name:     "user_id",
					DataType: "integer",
					Nullable: true,
					Constraints: []FieldConstraint{
						{
							Type:           ConstraintForeignKey,
							ReferredTable:  strPtr("users"),
							ReferredColumn: strPtr("id"),
						},
					},
				},
			},
		},
		{
			name: "unique constraint",
			fields: []FieldDefinition{
				{Name: "email", DataType: "text", Nullable: false},
			},
			constraints: []constraintInfo{
				{ColumnName: "email", ConstraintType: "UNIQUE"},
			},
			want: []FieldDefinition{
				{
					Name:     "email",
					DataType: "text",
					Nullable: false,
					Constraints: []FieldConstraint{
						{Type: ConstraintUnique},
						{Type: ConstraintNotNull},
					},
				},
			},
		},
		{
			name: "no constraints but not null",
			fields: []FieldDefinition{
				{Name: "name", DataType: "text", Nullable: false},
			},
			constraints: []constraintInfo{},
			want: []FieldDefinition{
				{
					Name:     "name",
					DataType: "text",
					Nullable: false,
					Constraints: []FieldConstraint{
						{Type: ConstraintNotNull},
					},
				},
			},
		},
		{
			name: "nullable field with no constraints",
			fields: []FieldDefinition{
				{Name: "bio", DataType: "text", Nullable: true},
			},
			constraints: []constraintInfo{},
			want: []FieldDefinition{
				{
					Name:        "bio",
					DataType:    "text",
					Nullable:    true,
					Constraints: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := make([]FieldDefinition, len(tt.fields))
			copy(fields, tt.fields)

			applyConstraints(fields, tt.constraints)

			if !reflect.DeepEqual(fields, tt.want) {
				t.Errorf("applyConstraints() got:\n%+v\nwant:\n%+v", fields, tt.want)
			}
		})
	}
}

func TestBuildValidationRules(t *testing.T) {
	tests := []struct {
		name        string
		fields      []FieldDefinition
		constraints []constraintInfo
		want        ValidationRules
	}{
		{
			name: "required and unique fields",
			fields: []FieldDefinition{
				{Name: "id", Nullable: false},
				{Name: "email", Nullable: false},
				{Name: "bio", Nullable: true},
			},
			constraints: []constraintInfo{
				{ColumnName: "id", ConstraintType: "PRIMARY KEY"},
				{ColumnName: "email", ConstraintType: "UNIQUE"},
			},
			want: ValidationRules{
				RequiredFields:    []string{"id", "email"},
				UniqueConstraints: []string{"id", "email"},
			},
		},
		{
			name: "no constraints",
			fields: []FieldDefinition{
				{Name: "col1", Nullable: true},
				{Name: "col2", Nullable: true},
			},
			constraints: []constraintInfo{},
			want: ValidationRules{
				RequiredFields:    nil,
				UniqueConstraints: nil,
			},
		},
		{
			name:   "empty fields",
			fields: []FieldDefinition{},
			constraints: []constraintInfo{
				{ColumnName: "id", ConstraintType: "PRIMARY KEY"},
			},
			want: ValidationRules{
				RequiredFields:    nil,
				UniqueConstraints: []string{"id"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildValidationRules(tt.fields, tt.constraints)

			// Compare RequiredFields (order matters)
			if !equalStringSlices(got.RequiredFields, tt.want.RequiredFields) {
				t.Errorf("buildValidationRules() RequiredFields = %v, want %v", got.RequiredFields, tt.want.RequiredFields)
			}
			// Compare UniqueConstraints as sets (order doesn't matter)
			if !equalStringSets(got.UniqueConstraints, tt.want.UniqueConstraints) {
				t.Errorf("buildValidationRules() UniqueConstraints = %v, want %v", got.UniqueConstraints, tt.want.UniqueConstraints)
			}
		})
	}
}

func TestTopNValues(t *testing.T) {
	tests := []struct {
		name  string
		freqs map[string]int
		n     int
		want  []TopValue
	}{
		{
			name:  "empty",
			freqs: map[string]int{},
			n:     5,
			want:  nil,
		},
		{
			name:  "nil",
			freqs: nil,
			n:     5,
			want:  nil,
		},
		{
			name:  "top 2 of 3",
			freqs: map[string]int{"a": 10, "b": 5, "c": 1},
			n:     2,
			want:  []TopValue{{Value: "a", Count: 10}, {Value: "b", Count: 5}},
		},
		{
			name:  "n larger than map",
			freqs: map[string]int{"x": 3},
			n:     5,
			want:  []TopValue{{Value: "x", Count: 3}},
		},
		{
			name:  "tie broken by key ascending",
			freqs: map[string]int{"b": 5, "a": 5},
			n:     2,
			want:  []TopValue{{Value: "a", Count: 5}, {Value: "b", Count: 5}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := topNValues(tt.freqs, tt.n)
			if tt.want == nil && got != nil {
				t.Errorf("topNValues() = %v, want nil", got)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("topNValues() len = %d, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("topNValues()[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestOptions_sampleSize(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want int
	}{
		{"nil", nil, 10000},
		{"zero", &Options{SampleSize: 0}, 10000},
		{"custom", &Options{SampleSize: 500}, 500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.sampleSize(); got != tt.want {
				t.Errorf("sampleSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestOptions_batchSize(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want int
	}{
		{"nil", nil, 1000},
		{"zero", &Options{BatchSize: 0}, 1000},
		{"custom", &Options{BatchSize: 200}, 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.batchSize(); got != tt.want {
				t.Errorf("batchSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestOptions_maxSampleRows(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want int
	}{
		{"nil", nil, 5},
		{"zero", &Options{MaxSampleRows: 0}, 5},
		{"custom", &Options{MaxSampleRows: 10}, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.maxSampleRows(); got != tt.want {
				t.Errorf("maxSampleRows() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestOptions_topN(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want int
	}{
		{"nil", nil, 5},
		{"zero", &Options{TopN: 0}, 5},
		{"custom", &Options{TopN: 3}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.topN(); got != tt.want {
				t.Errorf("topN() = %d, want %d", got, tt.want)
			}
		})
	}
}

// Helper functions

func strPtr(s string) *string {
	return &s
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Convert to maps for set comparison
	aMap := make(map[string]bool)
	for _, s := range a {
		aMap[s] = true
	}

	for _, s := range b {
		if !aMap[s] {
			return false
		}
	}
	return true
}
