package pgcontract

import (
	"testing"
)

func TestOptions_schema(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want string
	}{
		{
			name: "nil options returns public",
			opts: nil,
			want: "public",
		},
		{
			name: "empty schema returns public",
			opts: &Options{Schema: ""},
			want: "public",
		},
		{
			name: "custom schema",
			opts: &Options{Schema: "myschema"},
			want: "myschema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opts.schema()
			if got != tt.want {
				t.Errorf("schema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOptions_includeComments(t *testing.T) {
	tests := []struct {
		name string
		opts *Options
		want bool
	}{
		{
			name: "nil options returns false",
			opts: nil,
			want: false,
		},
		{
			name: "false",
			opts: &Options{IncludeComments: false},
			want: false,
		},
		{
			name: "true",
			opts: &Options{IncludeComments: true},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.opts.includeComments()
			if got != tt.want {
				t.Errorf("includeComments() = %v, want %v", got, tt.want)
			}
		})
	}
}
