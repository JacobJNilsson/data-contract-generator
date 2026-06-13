// Package fingerprint computes structural fingerprints of analyzed sources:
// a deterministic projection of analyzer output into a canonical structural
// schema, hashed into the pipeline cache key. A fingerprint match must
// guarantee the matched pipeline can process the file, so identity errs
// toward specificity: any structural change produces a different hash, while
// observation-unstable signals (nullability, profiling) are excluded.
package fingerprint

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	// TypeBinary marks declared binary payloads (OpenAPI byte/binary). Binary
	// vs text changes how bytes become records, so it never collapses into
	// STRING.
	TypeBinary CanonicalType = "BINARY"
	// TypeArray is the element-opaque array token, used when the analyzer
	// does not expose the element type (JSON sources). When it does (API
	// specs), the element is preserved as ARRAY<inner>, e.g. ARRAY<NUMBER>:
	// identity is as fine as the analyzer exposes, never coarser.
	TypeArray CanonicalType = "ARRAY"
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
//
// Struct fields are declared in canonical (alphabetical JSON key) order:
// CanonicalBytes serializes them in declaration order, and the golden test
// pins the resulting byte layout.
type Object struct {
	AlgoVersion string  `json:"algo_version"`
	Fields      []Field `json:"fields"`
	Format      Format  `json:"format"`
	// Nesting carries recursive structure when a future analyzer exposes it
	// (spec limitation L1). Always null under fp1; present so the stored
	// collision-guard object represents every key the hash covers.
	Nesting      json.RawMessage `json:"nesting"`
	ParseProfile *ParseProfile   `json:"parse_profile"`
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

// SkippedSchema records one schema of a multi-schema contract that could not
// be fingerprinted, so a single bad sheet or endpoint never costs the other
// units their cache identity (per-unit blast-radius isolation).
type SkippedSchema struct {
	Locator string
	Err     error
}

// FromDataContract fans a multi-schema contract (Excel workbook, API spec)
// out into one structural unit per schema, each fingerprinted independently.
// The schema name becomes the unit's channel-path locator. Schemas that fail
// to fingerprint are returned in skipped rather than failing the fanout; the
// error is reserved for contract-level problems.
func FromDataContract(dc *contract.DataContract) (units []Unit, skipped []SkippedSchema, err error) {
	if dc == nil {
		return nil, nil, errors.New("fingerprint: nil data contract")
	}
	format, err := dataContractFormat(dc)
	if err != nil {
		return nil, nil, err
	}
	if len(dc.Schemas) == 0 {
		return nil, nil, errors.New("fingerprint: data contract has no schemas")
	}
	for _, schema := range dc.Schemas {
		fields, fieldErr := canonicalFields(len(schema.Fields), func(i int) (string, string) {
			return schema.Fields[i].Name, schema.Fields[i].DataType
		})
		if fieldErr != nil {
			skipped = append(skipped, SkippedSchema{Locator: schema.Name, Err: fieldErr})
			continue
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
	return units, skipped, nil
}

// dataContractFormat determines the source format kind from contract
// metadata. Unrecognized contracts are an error, never a guess: an unknown
// format must not silently share identity space with a known one.
//
// Identity limitation, mirroring spec L1: Excel header detection is per
// sheet and not exposed on the contract, so a sheet whose literal header row
// happens to equal the synthesized positional names of a headerless sheet
// would collide. Resolving it needs the analyzer to expose the detection
// outcome — a future algo_version, flagged rather than hidden.
func dataContractFormat(dc *contract.DataContract) (Format, error) {
	if dc.ContractType == "destination" {
		return "", errors.New("fingerprint: destination contracts are not fingerprinted")
	}
	if sf, ok := dc.Metadata["source_format"].(string); ok && sf == "xlsx" {
		return FormatXLSX, nil
	}
	if src, ok := dc.Metadata["source"].(string); ok && src == "openapi" {
		return FormatAPI, nil
	}
	return "", errors.New("fingerprint: cannot determine source format from data contract metadata")
}

// canonicalFields normalizes, maps, disambiguates, and sorts a schema's
// fields into canonical form. Named fields sort by name so column reordering
// does not change identity; headerless CSV columns carry synthesized
// positional names, which keeps their order intrinsic to identity.
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
	disambiguateDuplicates(fields)
	// Names are unique after disambiguation, so name order is total.
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	return fields, nil
}

// disambiguateDuplicates makes canonical names unique by suffixing duplicate
// occurrences with their column-order ordinal ("id" → "id#1", "id#2").
// Without this, sorting would erase which duplicate-named column carries
// which type, letting two files with swapped column types share a
// fingerprint — a catastrophic false positive. Ambiguous names degrade to
// positional identity instead, so reordering duplicate-named columns is a
// miss, exactly like headerless CSV. canonicalName escapes literal '#' as
// '##' in every name, so a real header can never collide with a synthesized
// suffix.
func disambiguateDuplicates(fields []Field) {
	counts := make(map[string]int, len(fields))
	for _, f := range fields {
		counts[f.Name]++
	}
	seen := make(map[string]int, len(fields))
	for i, f := range fields {
		if counts[f.Name] < 2 {
			continue
		}
		seen[f.Name]++
		fields[i].Name = f.Name + "#" + strconv.Itoa(seen[f.Name])
	}
}

// canonicalName normalizes a field name for identity: invalid UTF-8 replaced
// deterministically, Unicode NFC, surrounding-whitespace trim, case
// preserved, and literal '#' escaped as '##' to keep the namespace of
// disambiguation suffixes private. The UTF-8 replacement keeps the stored
// object consistent with its serialized form, so encoding garbage cannot
// manufacture hash-equal-but-object-unequal collision alerts. Anything
// beyond that (a rename) is a real structural change and must miss.
func canonicalName(name string) string {
	canonical := norm.NFC.String(strings.TrimSpace(strings.ToValidUTF8(name, "�")))
	return strings.ReplaceAll(canonical, "#", "##")
}

// mapDataType projects an analyzer type token onto the canonical lattice.
// The mapping is strict in both directions: a token outside the known
// analyzer vocabularies is an error (silent coercion could let two shapes
// share a fingerprint), and a distinction the analyzer exposes is preserved
// (array element types, binary vs text) rather than coarsened. Only the
// spec's deliberate collapses apply: int/float to NUMBER, date/timestamp to
// TEMPORAL, uuid to STRING, jsonb to OBJECT.
func mapDataType(dataType string) (CanonicalType, error) {
	switch dataType {
	case "text", "uuid":
		return TypeString, nil
	case "bytea":
		return TypeBinary, nil
	case "numeric", "integer":
		return TypeNumber, nil
	case "date", "timestamp", "timestamptz":
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
	if inner, ok := strings.CutPrefix(dataType, "array["); ok && strings.HasSuffix(inner, "]") {
		element, err := mapDataType(strings.TrimSuffix(inner, "]"))
		if err != nil {
			return "", err
		}
		return CanonicalType("ARRAY<" + string(element) + ">"), nil
	}
	return "", fmt.Errorf("fingerprint: unmapped analyzer type %q", dataType)
}
