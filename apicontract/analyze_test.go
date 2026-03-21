package apicontract

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
)

// --- OpenAPI 3.x tests ------------------------------------------------------

func TestAnalyzeSpec_OpenAPI3(t *testing.T) {
	spec := openAPI3Spec()
	dc, err := AnalyzeSpec(spec, "https://api.example.com/openapi.json")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	if dc.ContractType != "destination" {
		t.Errorf("ContractType = %q, want destination", dc.ContractType)
	}
	if dc.ID != "Pet Store" {
		t.Errorf("ID = %q, want 'Pet Store'", dc.ID)
	}
	if len(dc.Schemas) == 0 {
		t.Fatal("expected at least one schema")
	}

	// POST /pets should use request body schema
	postPets := findSchema(dc.Schemas, "POST /pets")
	if postPets == nil {
		t.Fatal("POST /pets schema not found")
	}
	if len(postPets.Fields) != 2 {
		t.Errorf("POST /pets fields = %d, want 2", len(postPets.Fields))
	}

	nameField := findField(postPets.Fields, "name")
	if nameField == nil {
		t.Fatal("name field not found")
	}
	if nameField.DataType != "text" {
		t.Errorf("name type = %q, want text", nameField.DataType)
	}
	if nameField.Nullable {
		t.Error("name should not be nullable (required)")
	}

	tagField := findField(postPets.Fields, "tag")
	if tagField == nil {
		t.Fatal("tag field not found")
	}
	if !tagField.Nullable {
		t.Error("tag should be nullable (not required)")
	}

	// GET /pets should use response schema
	getPets := findSchema(dc.Schemas, "GET /pets")
	if getPets == nil {
		t.Fatal("GET /pets schema not found")
	}
	if len(getPets.Fields) != 3 {
		t.Errorf("GET /pets fields = %d, want 3 (array items resolved)", len(getPets.Fields))
	}

	// Verify validation rules
	if len(postPets.ValidationRules.RequiredFields) != 1 {
		t.Errorf("required = %v, want [name]", postPets.ValidationRules.RequiredFields)
	}
}

func TestAnalyzeSpec_OpenAPI3_WithRef(t *testing.T) {
	spec := openAPI3SpecWithRef()
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	postUsers := findSchema(dc.Schemas, "POST /users")
	if postUsers == nil {
		t.Fatal("POST /users not found")
	}
	if len(postUsers.Fields) != 2 {
		t.Errorf("fields = %d, want 2", len(postUsers.Fields))
	}

	emailField := findField(postUsers.Fields, "email")
	if emailField == nil {
		t.Fatal("email field not found")
	}
	if emailField.Description == nil || *emailField.Description != "User email" {
		t.Errorf("email description = %v, want 'User email'", emailField.Description)
	}
}

// --- Swagger 2.0 tests ------------------------------------------------------

func TestAnalyzeSpec_Swagger2(t *testing.T) {
	spec := swagger2Spec()
	dc, err := AnalyzeSpec(spec, "https://api.example.com/swagger.json")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}

	if dc.ID != "Pet Store" {
		t.Errorf("ID = %q, want 'Pet Store'", dc.ID)
	}

	postPets := findSchema(dc.Schemas, "POST /pets")
	if postPets == nil {
		t.Fatal("POST /pets not found")
	}
	if len(postPets.Fields) != 2 {
		t.Errorf("fields = %d, want 2", len(postPets.Fields))
	}

	// GET /pets uses response with array schema
	getPets := findSchema(dc.Schemas, "GET /pets")
	if getPets == nil {
		t.Fatal("GET /pets not found")
	}
	if len(getPets.Fields) != 2 {
		t.Errorf("GET /pets fields = %d, want 2", len(getPets.Fields))
	}
}

// --- Analyze (HTTP fetch) tests ---------------------------------------------

func TestAnalyze_Success(t *testing.T) {
	spec := openAPI3Spec()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(spec)
	}))
	defer ts.Close()

	dc, err := Analyze(context.Background(), ts.URL)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if len(dc.Schemas) == 0 {
		t.Error("expected schemas")
	}
}

func TestAnalyze_EmptyURL(t *testing.T) {
	_, err := Analyze(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestAnalyze_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	_, err := Analyze(context.Background(), ts.URL)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestAnalyze_BadJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer ts.Close()

	_, err := Analyze(context.Background(), ts.URL)
	if err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

func TestAnalyze_ConnectionRefused(t *testing.T) {
	_, err := Analyze(context.Background(), "http://127.0.0.1:1/spec")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

func TestAnalyze_NotOpenAPI(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"foo": "bar"}`))
	}))
	defer ts.Close()

	_, err := Analyze(context.Background(), ts.URL)
	if err == nil {
		t.Fatal("expected error for non-OpenAPI document")
	}
}

// --- Type mapping tests -----------------------------------------------------

func TestMapOpenAPIType(t *testing.T) {
	tests := []struct {
		name string
		prop map[string]any
		want string
	}{
		{"string", map[string]any{"type": "string"}, "text"},
		{"integer", map[string]any{"type": "integer"}, "integer"},
		{"int32", map[string]any{"type": "integer", "format": "int32"}, "integer"},
		{"int64", map[string]any{"type": "integer", "format": "int64"}, "integer"},
		{"number", map[string]any{"type": "number"}, "numeric"},
		{"float", map[string]any{"type": "number", "format": "float"}, "numeric"},
		{"double", map[string]any{"type": "number", "format": "double"}, "numeric"},
		{"boolean", map[string]any{"type": "boolean"}, "boolean"},
		{"date", map[string]any{"type": "string", "format": "date"}, "date"},
		{"date-time", map[string]any{"type": "string", "format": "date-time"}, "timestamptz"},
		{"uuid", map[string]any{"type": "string", "format": "uuid"}, "uuid"},
		{"byte", map[string]any{"type": "string", "format": "byte"}, "bytea"},
		{"binary", map[string]any{"type": "string", "format": "binary"}, "bytea"},
		{"object", map[string]any{"type": "object"}, "jsonb"},
		{"array", map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "array[text]"},
		{"array no items", map[string]any{"type": "array"}, "array[text]"},
		{"empty", map[string]any{}, "text"},
		{"unknown format", map[string]any{"format": "custom"}, "custom"},
		{"unknown type", map[string]any{"type": "custom"}, "custom"},
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

// --- Helper tests -----------------------------------------------------------

func TestDetectVersion(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{"openapi 3", map[string]any{"openapi": "3.0.0"}, "3.0.0"},
		{"swagger 2", map[string]any{"swagger": "2.0"}, "2.0"},
		{"neither", map[string]any{"foo": "bar"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectVersion(tt.raw)
			if got != tt.want {
				t.Errorf("detectVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsHTTPMethod(t *testing.T) {
	for _, m := range []string{"get", "post", "put", "patch", "delete", "head", "options"} {
		if !isHTTPMethod(m) {
			t.Errorf("isHTTPMethod(%q) = false, want true", m)
		}
	}
	for _, m := range []string{"parameters", "summary", "x-extension", ""} {
		if isHTTPMethod(m) {
			t.Errorf("isHTTPMethod(%q) = true, want false", m)
		}
	}
}

func TestIsWriteMethod(t *testing.T) {
	for _, m := range []string{"post", "put", "patch"} {
		if !isWriteMethod(m) {
			t.Errorf("isWriteMethod(%q) = false, want true", m)
		}
	}
	for _, m := range []string{"get", "delete", "head", "options"} {
		if isWriteMethod(m) {
			t.Errorf("isWriteMethod(%q) = true, want false", m)
		}
	}
}

func TestAnalyzeSpec_NoTitle(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/items": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id": map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "https://example.com/spec.json")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if dc.ID != "https://example.com/spec.json" {
		t.Errorf("ID = %q, want URL fallback", dc.ID)
	}
}

func TestAnalyzeSpec_UnsupportedVersion(t *testing.T) {
	spec := map[string]any{"openapi": "1.0"}
	_, err := AnalyzeSpec(spec, "test")
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

// --- Edge case tests --------------------------------------------------------

func TestAnalyzeSpec_NonJSONMediaType(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "XML API"},
		"paths": map[string]any{
			"/data": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/xml": map[string]any{
								"schema": map[string]any{
									"type": "object",
									"properties": map[string]any{
										"id": map[string]any{"type": "integer"},
									},
								},
							},
						},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// Should fall back to first available media type
	postData := findSchema(dc.Schemas, "POST /data")
	if postData == nil {
		t.Fatal("POST /data not found")
	}
	if len(postData.Fields) != 1 {
		t.Errorf("fields = %d, want 1", len(postData.Fields))
	}
}

func TestAnalyzeSpec_NoRequestBody(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Read API"},
		"paths": map[string]any{
			"/items": map[string]any{
				"post": map[string]any{
					"responses": map[string]any{
						"201": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id": map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// POST without request body should fall back to response
	postItems := findSchema(dc.Schemas, "POST /items")
	if postItems == nil {
		t.Fatal("POST /items not found")
	}
	if len(postItems.Fields) != 1 {
		t.Errorf("fields = %d, want 1", len(postItems.Fields))
	}
}

func TestAnalyzeSpec_EmptyRequestBodyContent(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// No parseable content -> no schema generated
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0 (no parseable content)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_NonObjectPath(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/ok": "not a map",
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_NonObjectMethod(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/ok": map[string]any{
				"get": "not a map",
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_UnresolvableRef(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Broken"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/NonExistent",
								},
							},
						},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
		"components": map[string]any{"schemas": map[string]any{}},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// Unresolvable ref -> no fields extracted -> no schema
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0 (ref unresolvable)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_ResponseNonJSONFallback(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"text/xml": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"id": map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Errorf("schemas = %d, want 1 (fallback to first media type)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_Swagger2_NonBodyParam(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"parameters": []any{
						map[string]any{"in": "query", "name": "q", "type": "string"},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"schema": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id": map[string]any{"type": "integer"},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	// No body param -> falls back to response
	postX := findSchema(dc.Schemas, "POST /x")
	if postX == nil {
		t.Fatal("POST /x not found")
	}
}

func TestAnalyzeSpec_Swagger2_InvalidParam(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"parameters": []any{"not a map"},
					"responses": map[string]any{
						"200": map[string]any{
							"schema": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id": map[string]any{"type": "integer"},
								},
							},
						},
					},
				},
			},
		},
	}

	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 1 {
		t.Errorf("schemas = %d, want 1", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_Swagger2_NoResponses(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_Swagger2_ResponseNoSchema(t *testing.T) {
	spec := map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
					},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_OpenAPI3_EmptyResponses(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_OpenAPI3_NoMediaTypeSchema(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{},
						},
					},
					"responses": map[string]any{"200": map[string]any{}},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	if len(dc.Schemas) != 0 {
		t.Errorf("schemas = %d, want 0 (no schema in media type)", len(dc.Schemas))
	}
}

func TestAnalyzeSpec_SchemaPropertyNotMap(t *testing.T) {
	spec := map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Test"},
		"paths": map[string]any{
			"/x": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"broken": "not a map",
											"ok":     map[string]any{"type": "string"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	dc, err := AnalyzeSpec(spec, "test")
	if err != nil {
		t.Fatalf("AnalyzeSpec: %v", err)
	}
	getX := findSchema(dc.Schemas, "GET /x")
	if getX == nil {
		t.Fatal("GET /x not found")
	}
	if len(getX.Fields) != 1 {
		t.Errorf("fields = %d, want 1 (broken prop skipped)", len(getX.Fields))
	}
}

// --- Test fixtures ----------------------------------------------------------

func openAPI3Spec() map[string]any {
	return map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "Pet Store", "version": "1.0.0"},
		"paths": map[string]any{
			"/pets": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "array",
										"items": map[string]any{
											"type":     "object",
											"required": []any{"id", "name"},
											"properties": map[string]any{
												"id":   map[string]any{"type": "integer", "format": "int64"},
												"name": map[string]any{"type": "string"},
												"tag":  map[string]any{"type": "string"},
											},
										},
									},
								},
							},
						},
					},
				},
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"type":     "object",
									"required": []any{"name"},
									"properties": map[string]any{
										"name": map[string]any{"type": "string"},
										"tag":  map[string]any{"type": "string"},
									},
								},
							},
						},
					},
					"responses": map[string]any{
						"201": map[string]any{},
					},
				},
			},
		},
	}
}

func openAPI3SpecWithRef() map[string]any {
	return map[string]any{
		"openapi": "3.0.0",
		"info":    map[string]any{"title": "User API"},
		"paths": map[string]any{
			"/users": map[string]any{
				"post": map[string]any{
					"requestBody": map[string]any{
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/User",
								},
							},
						},
					},
					"responses": map[string]any{"201": map[string]any{}},
				},
			},
		},
		"components": map[string]any{
			"schemas": map[string]any{
				"User": map[string]any{
					"type":     "object",
					"required": []any{"name", "email"},
					"properties": map[string]any{
						"name":  map[string]any{"type": "string"},
						"email": map[string]any{"type": "string", "description": "User email"},
					},
				},
			},
		},
	}
}

func swagger2Spec() map[string]any {
	return map[string]any{
		"swagger": "2.0",
		"info":    map[string]any{"title": "Pet Store", "version": "1.0.0"},
		"paths": map[string]any{
			"/pets": map[string]any{
				"get": map[string]any{
					"responses": map[string]any{
						"200": map[string]any{
							"schema": map[string]any{
								"type": "array",
								"items": map[string]any{
									"$ref": "#/definitions/Pet",
								},
							},
						},
					},
				},
				"post": map[string]any{
					"parameters": []any{
						map[string]any{
							"in":   "body",
							"name": "body",
							"schema": map[string]any{
								"$ref": "#/definitions/Pet",
							},
						},
					},
					"responses": map[string]any{"201": map[string]any{}},
				},
			},
		},
		"definitions": map[string]any{
			"Pet": map[string]any{
				"type":     "object",
				"required": []any{"name"},
				"properties": map[string]any{
					"name": map[string]any{"type": "string"},
					"tag":  map[string]any{"type": "string"},
				},
			},
		},
	}
}

// --- Helpers ----------------------------------------------------------------

func findSchema(schemas []contract.SchemaContract, name string) *contract.SchemaContract {
	for i := range schemas {
		if schemas[i].Name == name {
			return &schemas[i]
		}
	}
	return nil
}

func findField(fields []contract.FieldDefinition, name string) *contract.FieldDefinition {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}
