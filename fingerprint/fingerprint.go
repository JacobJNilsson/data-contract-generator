// Package fingerprint computes structural fingerprints of analyzed sources:
// a deterministic projection of analyzer output into a canonical structural
// schema, hashed into the pipeline cache key. A fingerprint match must
// guarantee the matched pipeline can process the file, so identity errs
// toward specificity: any structural change produces a different hash, while
// observation-unstable signals (nullability, profiling) are excluded.
package fingerprint

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/jsoncontract"
	"golang.org/x/text/unicode/norm"
)

// AlgoVersion identifies the canonicalisation rules used to build and hash
// fingerprint objects. Any change to those rules must bump it; old hashes are
// never reinterpreted under new rules.
const AlgoVersion = "fp1"

// CanonicalType is the coarse type lattice used for identity. The analyzer
// vocabulary is already coarse, so strict equality on these tokens is stable
// across files of the same shape.
type CanonicalType string

// The canonical type tokens. STRING, NUMBER, TEMPORAL, BOOLEAN, OBJECT, and
// ARRAY cover every observable analyzer type.
const (
	TypeString   CanonicalType = "STRING"
	TypeNumber   CanonicalType = "NUMBER"
	TypeTemporal CanonicalType = "TEMPORAL"
	TypeBoolean  CanonicalType = "BOOLEAN"
	TypeObject   CanonicalType = "OBJECT"
	TypeArray    CanonicalType = "ARRAY"
	// TypeUnknown marks columns whose type could not be observed (all-null or
	// empty). When a later file resolves the type, the hash changes: a
	// documented cache miss, not a silent re-route.
	TypeUnknown CanonicalType = "UNKNOWN"
)

// Format is the source format kind. Different formats use different
// analyzers and parsers, so format is always identity-bearing.
type Format string

// The supported source format kinds.
const (
	FormatCSV    Format = "csv"
	FormatJSON   Format = "json"
	FormatNDJSON Format = "ndjson"
	FormatXLSX   Format = "xlsx"
	FormatAPI    Format = "api"
)

// ParseProfile holds the parse-level settings that change how bytes become
// records. Fields that do not apply to a format are nil and serialize as
// null, so absence is itself canonical.
type ParseProfile struct {
	Delimiter *string `json:"delimiter"`
	Encoding  *string `json:"encoding"`
	HasHeader *bool   `json:"has_header"`
}

// Field is one named, canonically typed column of the structural schema.
// Names are NFC-normalised and trimmed with case preserved; volatile
// per-field data (nullability, profiling) is deliberately absent.
type Field struct {
	Name string        `json:"name"`
	Type CanonicalType `json:"type"`
}

// Object is the canonical, inspectable fingerprint: everything that goes
// into identity and nothing that does not. Hash() derives the cache-key hash
// from its canonical serialization; the object itself is stored beside the
// hash as the collision guard.
type Object struct {
	AlgoVersion  string        `json:"algo_version"`
	Format       Format        `json:"format"`
	ParseProfile *ParseProfile `json:"parse_profile"`
	Fields       []Field       `json:"fields"`
}

// Unit is one structural unit of a multi-schema source: the schema locator
// (sheet name, "METHOD /path" endpoint) plus its fingerprint. The locator
// extends the intake channel into the cache key's channel_path so units
// within one file do not collide.
type Unit struct {
	Locator string
	Object  Object
}

// FromCSV fingerprints a CSV analysis. Delimiter, header presence, and
// encoding are identity-bearing parts of the parse profile.
func FromCSV(sc *csvcontract.SourceContract) (Object, error) {
	if sc == nil {
		return Object{}, errors.New("fingerprint: nil CSV contract")
	}
	fields, err := canonicalFields(len(sc.Fields), func(i int) (string, string) {
		return sc.Fields[i].Name, string(sc.Fields[i].DataType)
	})
	if err != nil {
		return Object{}, err
	}
	delimiter, encoding, hasHeader := sc.Delimiter, sc.Encoding, sc.HasHeader
	return Object{
		AlgoVersion:  AlgoVersion,
		Format:       FormatCSV,
		ParseProfile: &ParseProfile{Delimiter: &delimiter, Encoding: &encoding, HasHeader: &hasHeader},
		Fields:       fields,
	}, nil
}

// FromJSON fingerprints a JSON or NDJSON analysis. The two formats parse
// differently, so the contract's source format is identity-bearing.
func FromJSON(sc *jsoncontract.SourceContract) (Object, error) {
	if sc == nil {
		return Object{}, errors.New("fingerprint: nil JSON contract")
	}
	var format Format
	switch sc.SourceFormat {
	case "json":
		format = FormatJSON
	case "ndjson":
		format = FormatNDJSON
	default:
		return Object{}, fmt.Errorf("fingerprint: unsupported JSON source format %q", sc.SourceFormat)
	}
	fields, err := canonicalFields(len(sc.Fields), func(i int) (string, string) {
		return sc.Fields[i].Name, string(sc.Fields[i].DataType)
	})
	if err != nil {
		return Object{}, err
	}
	encoding := sc.Encoding
	return Object{
		AlgoVersion:  AlgoVersion,
		Format:       format,
		ParseProfile: &ParseProfile{Encoding: &encoding},
		Fields:       fields,
	}, nil
}

// FromDataContract fans a multi-schema contract (Excel workbook, API spec)
// out into one structural unit per schema, each fingerprinted independently.
// The schema name becomes the unit's channel-path locator.
func FromDataContract(dc *contract.DataContract) ([]Unit, error) {
	if dc == nil {
		return nil, errors.New("fingerprint: nil data contract")
	}
	format, err := dataContractFormat(dc)
	if err != nil {
		return nil, err
	}
	if len(dc.Schemas) == 0 {
		return nil, errors.New("fingerprint: data contract has no schemas")
	}
	units := make([]Unit, 0, len(dc.Schemas))
	for _, schema := range dc.Schemas {
		fields, err := canonicalFields(len(schema.Fields), func(i int) (string, string) {
			return schema.Fields[i].Name, schema.Fields[i].DataType
		})
		if err != nil {
			return nil, fmt.Errorf("schema %q: %w", schema.Name, err)
		}
		units = append(units, Unit{
			Locator: schema.Name,
			Object: Object{
				AlgoVersion: AlgoVersion,
				Format:      format,
				Fields:      fields,
			},
		})
	}
	return units, nil
}

// dataContractFormat determines the source format kind from contract
// metadata. Unrecognized contracts are an error, never a guess: an unknown
// format must not silently share identity space with a known one.
func dataContractFormat(dc *contract.DataContract) (Format, error) {
	if sf, ok := dc.Metadata["source_format"].(string); ok && sf == "xlsx" {
		return FormatXLSX, nil
	}
	if src, ok := dc.Metadata["source"].(string); ok && src == "openapi" {
		return FormatAPI, nil
	}
	return "", errors.New("fingerprint: cannot determine source format from data contract metadata")
}

// canonicalFields normalizes, maps, and sorts a schema's fields into
// canonical form. Named fields sort by name so column reordering does not
// change identity; headerless CSV columns carry synthesized positional
// names, which keeps their order intrinsic to identity.
func canonicalFields(n int, at func(i int) (name, dataType string)) ([]Field, error) {
	if n == 0 {
		return nil, errors.New("fingerprint: schema has no fields")
	}
	fields := make([]Field, 0, n)
	for i := range n {
		name, dataType := at(i)
		canonical, err := mapDataType(dataType)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		fields = append(fields, Field{Name: canonicalName(name), Type: canonical})
	}
	sort.SliceStable(fields, func(i, j int) bool {
		if fields[i].Name != fields[j].Name {
			return fields[i].Name < fields[j].Name
		}
		return fields[i].Type < fields[j].Type
	})
	return fields, nil
}

// canonicalName normalizes a field name for identity: Unicode NFC plus
// surrounding-whitespace trim, case preserved. Anything beyond that (a
// rename) is a real structural change and must miss.
func canonicalName(name string) string {
	return norm.NFC.String(strings.TrimSpace(name))
}

// mapDataType projects an analyzer type token onto the canonical lattice.
// The mapping is strict: a token outside the known analyzer vocabularies is
// an error, because silently coercing it could let two different shapes
// share a fingerprint.
func mapDataType(dataType string) (CanonicalType, error) {
	switch dataType {
	case "text", "uuid", "bytea":
		return TypeString, nil
	case "numeric", "integer":
		return TypeNumber, nil
	case "date", "timestamptz":
		return TypeTemporal, nil
	case "boolean":
		return TypeBoolean, nil
	case "object", "jsonb":
		return TypeObject, nil
	case "array":
		return TypeArray, nil
	case "null", "empty":
		return TypeUnknown, nil
	}
	if strings.HasPrefix(dataType, "array[") && strings.HasSuffix(dataType, "]") {
		return TypeArray, nil
	}
	return "", fmt.Errorf("fingerprint: unmapped analyzer type %q", dataType)
}
