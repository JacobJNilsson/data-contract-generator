package supacontract

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// sampleOpenAPISpec returns a realistic PostgREST OpenAPI spec for testing.
func sampleOpenAPISpec() openAPISpec {
	return openAPISpec{
		Paths: map[string]any{
			"/users":   map[string]any{},
			"/orders":  map[string]any{},
			"/rpc/foo": map[string]any{}, // should be skipped
		},
		Definitions: map[string]schemaObject{
			"users": {
				Description: "App users",
				Required:    []string{"id", "email"},
				Properties: map[string]propertyObject{
					"id":         {Type: "integer", Format: "integer", Description: "Primary key"},
					"email":      {Type: "string", Format: "character varying"},
					"name":       {Type: "string", Format: "text"},
					"created_at": {Type: "string", Format: "timestamp with time zone"},
					"is_active":  {Type: "boolean", Format: "boolean"},
				},
			},
			"orders": {
				Required: []string{"id", "user_id", "total"},
				Properties: map[string]propertyObject{
					"id":      {Type: "integer", Format: "bigint"},
					"user_id": {Type: "integer", Format: "integer"},
					"total":   {Type: "number", Format: "numeric"},
					"status":  {Type: "string", Format: "text"},
					"tags":    {Type: "array", Items: &propertyObject{Type: "string", Format: "text"}},
				},
			},
		},
	}
}

func TestValidateProjectURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"valid", "https://abcdef.supabase.co", false},
		{"valid trailing slash", "https://abcdef.supabase.co/", false},
		{"missing https", "http://abcdef.supabase.co", true},
		{"not supabase domain", "https://example.com", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateProjectURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateProjectURL(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestExtractProjectRef(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://abcdef.supabase.co", "abcdef"},
		{"https://abcdef.supabase.co/", "abcdef"},
		{"https://my-project-ref.supabase.co", "my-project-ref"},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			got := extractProjectRef(tt.url)
			if got != tt.want {
				t.Errorf("extractProjectRef(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestExtractTableNames(t *testing.T) {
	tests := []struct {
		name  string
		paths map[string]any
		want  []string
	}{
		{
			name: "normal tables and rpc",
			paths: map[string]any{
				"/users":   nil,
				"/orders":  nil,
				"/rpc/foo": nil,
				"/rpc/bar": nil,
			},
			want: []string{"orders", "users"},
		},
		{
			name:  "empty paths",
			paths: map[string]any{},
			want:  nil,
		},
		{
			name: "paths without slash prefix are skipped",
			paths: map[string]any{
				"no_slash": nil,
				"/valid":   nil,
			},
			want: []string{"valid"},
		},
		{
			name: "nested paths are skipped",
			paths: map[string]any{
				"/a/b": nil,
				"/c":   nil,
			},
			want: []string{"c"},
		},
		{
			name: "root path skipped",
			paths: map[string]any{
				"/": nil,
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTableNames(tt.paths)
			if len(got) != len(tt.want) {
				t.Fatalf("extractTableNames() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseTables_MissingDefinition(t *testing.T) {
	// Table in paths but not in definitions should be skipped
	spec := openAPISpec{
		Paths: map[string]any{
			"/exists":  nil,
			"/missing": nil,
		},
		Definitions: map[string]schemaObject{
			"exists": {Properties: map[string]propertyObject{
				"id": {Type: "integer", Format: "integer"},
			}},
		},
	}

	tables := parseTables(&spec)
	if len(tables) != 1 {
		t.Fatalf("got %d tables, want 1", len(tables))
	}
	if tables[0].TableName != "exists" {
		t.Errorf("table = %q, want exists", tables[0].TableName)
	}
}

func TestMapOpenAPIType(t *testing.T) {
	tests := []struct {
		name string
		prop propertyObject
		want string
	}{
		{"integer format", propertyObject{Format: "integer"}, "integer"},
		{"bigint format", propertyObject{Format: "bigint"}, "bigint"},
		{"smallint format", propertyObject{Format: "smallint"}, "smallint"},
		{"numeric format", propertyObject{Format: "numeric"}, "numeric"},
		{"real format", propertyObject{Format: "real"}, "real"},
		{"double format", propertyObject{Format: "double precision"}, "double"},
		{"boolean format", propertyObject{Format: "boolean"}, "boolean"},
		{"date format", propertyObject{Format: "date"}, "date"},
		{"timestamp format", propertyObject{Format: "timestamp without time zone"}, "timestamp"},
		{"timestamptz format", propertyObject{Format: "timestamp with time zone"}, "timestamptz"},
		{"timestamptz short", propertyObject{Format: "timestamptz"}, "timestamptz"},
		{"uuid format", propertyObject{Format: "uuid"}, "uuid"},
		{"json format", propertyObject{Format: "json"}, "json"},
		{"jsonb format", propertyObject{Format: "jsonb"}, "jsonb"},
		{"text format", propertyObject{Format: "text"}, "text"},
		{"varchar format", propertyObject{Format: "character varying"}, "varchar"},
		{"char format", propertyObject{Format: "character"}, "char"},
		{"bytea format", propertyObject{Format: "bytea"}, "bytea"},
		{"time format", propertyObject{Format: "time without time zone"}, "time"},
		{"timetz format", propertyObject{Format: "time with time zone"}, "timetz"},
		{"integer type fallback", propertyObject{Type: "integer"}, "integer"},
		{"number type fallback", propertyObject{Type: "number"}, "numeric"},
		{"boolean type fallback", propertyObject{Type: "boolean"}, "boolean"},
		{"object type fallback", propertyObject{Type: "object"}, "jsonb"},
		{"array type", propertyObject{Type: "array", Items: &propertyObject{Type: "string", Format: "text"}}, "array[text]"},
		{"array no items", propertyObject{Type: "array"}, "array[text]"},
		{"unknown format", propertyObject{Format: "custom_thing"}, "custom_thing"},
		{"unknown type", propertyObject{Type: "custom"}, "custom"},
		{"empty", propertyObject{}, "text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapOpenAPIType(tt.prop)
			if got != tt.want {
				t.Errorf("mapOpenAPIType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildField(t *testing.T) {
	prop := propertyObject{
		Type:        "integer",
		Format:      "integer",
		Description: "Primary key",
	}

	field := buildField("id", prop, true)

	if field.Name != "id" {
		t.Errorf("Name = %q, want id", field.Name)
	}
	if field.DataType != "integer" {
		t.Errorf("DataType = %q, want integer", field.DataType)
	}
	if field.Nullable {
		t.Error("Nullable should be false for required field")
	}
	if field.Description == nil || *field.Description != "Primary key" {
		t.Errorf("Description = %v, want 'Primary key'", field.Description)
	}
	if len(field.Constraints) != 1 || field.Constraints[0].Type != ConstraintNotNull {
		t.Errorf("Constraints = %v, want [not_null]", field.Constraints)
	}
}

func TestBuildField_Nullable(t *testing.T) {
	prop := propertyObject{Type: "string", Format: "text"}
	field := buildField("bio", prop, false)

	if !field.Nullable {
		t.Error("Nullable should be true for non-required field")
	}
	if field.Description != nil {
		t.Errorf("Description should be nil, got %v", field.Description)
	}
	if len(field.Constraints) != 0 {
		t.Errorf("Constraints should be empty, got %v", field.Constraints)
	}
}

func TestParseTables(t *testing.T) {
	spec := sampleOpenAPISpec()

	tables := parseTables(&spec)

	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2", len(tables))
	}

	// Tables should be sorted
	if tables[0].TableName != "orders" {
		t.Errorf("tables[0].TableName = %q, want orders", tables[0].TableName)
	}
	if tables[1].TableName != "users" {
		t.Errorf("tables[1].TableName = %q, want users", tables[1].TableName)
	}

	// Verify users table
	users := tables[1]
	if len(users.Fields) != 5 {
		t.Errorf("users fields = %d, want 5", len(users.Fields))
	}
	if len(users.ValidationRules.RequiredFields) != 2 {
		t.Errorf("users required = %v, want 2 fields", users.ValidationRules.RequiredFields)
	}

	// Verify orders table has array type
	orders := tables[0]
	var tagsField *FieldDefinition
	for i := range orders.Fields {
		if orders.Fields[i].Name == "tags" {
			tagsField = &orders.Fields[i]
			break
		}
	}
	if tagsField == nil {
		t.Fatal("tags field not found")
	}
	if tagsField.DataType != "array[text]" {
		t.Errorf("tags DataType = %q, want array[text]", tagsField.DataType)
	}
}

// newTestServer returns an httptest server that serves the given OpenAPI spec.
// It verifies auth headers are present.
func newTestServer(spec openAPISpec) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") == "" {
			http.Error(w, `{"message":"No API key found"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(spec)
	}))
}

func TestAnalyzeFromURL_Success(t *testing.T) {
	ts := newTestServer(sampleOpenAPISpec())
	defer ts.Close()

	contract, err := analyzeFromURL(context.Background(), ts.URL, "test-key", "test-project")
	if err != nil {
		t.Fatalf("analyzeFromURL() error = %v", err)
	}

	if contract.ContractType != "destination" {
		t.Errorf("ContractType = %q, want destination", contract.ContractType)
	}
	if contract.DatabaseID != "test-project" {
		t.Errorf("DatabaseID = %q, want test-project", contract.DatabaseID)
	}
	if len(contract.Tables) != 2 {
		t.Fatalf("got %d tables, want 2", len(contract.Tables))
	}
	if contract.Metadata["table_count"] != 2 {
		t.Errorf("table_count = %v, want 2", contract.Metadata["table_count"])
	}
}

func TestAnalyzeFromURL_Unauthorized(t *testing.T) {
	ts := newTestServer(sampleOpenAPISpec())
	defer ts.Close()

	// Empty apikey header triggers 401 from our test server
	_, err := analyzeFromURL(context.Background(), ts.URL, "", "test")
	if err == nil {
		t.Fatal("expected error for empty API key")
	}
}

func TestAnalyzeFromURL_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	_, err := analyzeFromURL(context.Background(), ts.URL, "key", "test")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestAnalyzeFromURL_Forbidden(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer ts.Close()

	_, err := analyzeFromURL(context.Background(), ts.URL, "key", "test")
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}

func TestAnalyzeFromURL_BadJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer ts.Close()

	_, err := analyzeFromURL(context.Background(), ts.URL, "key", "test")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestAnalyzeFromURL_ConnectionRefused(t *testing.T) {
	_, err := analyzeFromURL(context.Background(), "http://127.0.0.1:1", "key", "test")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

func TestAnalyzeDatabase_EmptyAPIKey(t *testing.T) {
	_, err := AnalyzeDatabase(context.Background(), "https://test.supabase.co", "", nil)
	if err == nil {
		t.Error("expected error for empty API key")
	}
}

func TestAnalyzeDatabase_InvalidURL(t *testing.T) {
	_, err := AnalyzeDatabase(context.Background(), "http://not-supabase.com", "key", nil)
	if err == nil {
		t.Error("expected error for invalid URL")
	}
}
