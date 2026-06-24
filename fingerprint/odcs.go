package fingerprint

import (
	"errors"
	"fmt"
	"strings"

	"github.com/JacobJNilsson/data-contract-generator/odcs"
)

// FromODCS derives structural fingerprint units from an ODCS contract,
// producing the same canonical Object the native From* functions produce
// from the analyser contracts. It is the read-side of the build-alongside
// migration: the orchestrator can switch its fingerprint input from the
// native contract to the ODCS document only because this path is proven
// byte-identical to the native one (the conformance corpus). While they
// stay byte-identical, AlgoVersion stays "fp1" and no cache is invalidated.
//
// Each schema object becomes one unit, located by its name (matching the
// native multi-schema fanout). The source format and parse facts are read
// from the schema object's dcg custom properties, where the emitter put
// them, because the ODCS column model has no home for delimiter, encoding,
// or header presence. A schema object that cannot be fingerprinted is
// returned in skipped rather than failing the whole contract, mirroring
// FromDataContract's per-unit blast-radius isolation; a contract-level
// problem (nil, no schema) is an error.
func FromODCS(c odcs.Contract) (units []Unit, skipped []SkippedSchema, err error) {
	if len(c.Schema) == 0 {
		return nil, nil, errors.New("fingerprint: ODCS contract has no schema objects")
	}
	for _, obj := range c.Schema {
		object, objErr := objectFromODCSSchema(obj)
		if objErr != nil {
			skipped = append(skipped, SkippedSchema{Locator: obj.Name, Err: objErr})
			continue
		}
		units = append(units, Unit{Locator: obj.Name, Object: object})
	}
	if len(units) == 0 {
		// Every schema object failed: this is no longer a per-unit problem
		// but a contract that yields no identity at all, so surface it as an
		// error rather than an empty success the caller might cache.
		return nil, skipped, errors.New("fingerprint: no ODCS schema object could be fingerprinted")
	}
	return units, skipped, nil
}

// objectFromODCSSchema builds one canonical Object from a single ODCS
// schema object: its format and parse profile from the dcg custom
// properties, its fields from the properties' logical/physical types.
func objectFromODCSSchema(obj odcs.SchemaObject) (Object, error) {
	format, err := odcsFormat(obj)
	if err != nil {
		return Object{}, err
	}
	parseProfile, err := odcsParseProfile(obj, format)
	if err != nil {
		return Object{}, err
	}
	fields, err := canonicalFields(len(obj.Properties), func(i int) (string, string) {
		// canonicalFields maps a single analyzer-type token, but ODCS splits
		// type across logicalType and physicalType. odcsCanonicalToken
		// collapses the pair back into the exact token mapDataType expects,
		// so both paths run through the identical field canonicalisation.
		return obj.Properties[i].Name, odcsCanonicalToken(obj.Properties[i])
	})
	if err != nil {
		return Object{}, err
	}
	return Object{
		AlgoVersion:  AlgoVersion,
		Format:       format,
		ParseProfile: parseProfile,
		Fields:       fields,
	}, nil
}

// odcsFormat reads the source-format kind the emitter stamped onto the
// schema object. An unrecognised or absent format is an error: identity
// must never be minted under a guessed format, exactly as the native
// dataContractFormat refuses to guess.
func odcsFormat(obj odcs.SchemaObject) (Format, error) {
	raw, ok := odcs.CustomProp(obj.CustomProperties, odcs.CustomKeySourceFormat)
	if !ok {
		return "", fmt.Errorf("fingerprint: ODCS schema object %q carries no %s", obj.Name, odcs.CustomKeySourceFormat)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("fingerprint: ODCS schema object %q has non-string %s", obj.Name, odcs.CustomKeySourceFormat)
	}
	switch Format(s) {
	case FormatCSV, FormatJSON, FormatNDJSON, FormatXLSX, FormatAPI:
		return Format(s), nil
	}
	return "", fmt.Errorf("fingerprint: ODCS schema object %q has unrecognised source format %q", obj.Name, s)
}

// odcsParseProfile reconstructs the parse profile per format, matching what
// the native From* functions set. CSV carries delimiter, encoding, and
// header presence; JSON and NDJSON carry only encoding (delimiter and
// header stay nil so the canonical bytes match FromJSON's); Excel and API
// carry no parse profile at all (nil, matching FromDataContract). The
// custom-property values come back from YAML/JSON as their natural scalar
// types, so each is type-asserted and a wrong type is an error rather than
// a silent default that would forge a different fingerprint.
func odcsParseProfile(obj odcs.SchemaObject, format Format) (*ParseProfile, error) {
	switch format {
	case FormatCSV:
		delimiter, err := odcsString(obj, odcs.CustomKeyDelimiter)
		if err != nil {
			return nil, err
		}
		encoding, err := odcsString(obj, odcs.CustomKeyEncoding)
		if err != nil {
			return nil, err
		}
		hasHeader, err := odcsBool(obj, odcs.CustomKeyHasHeader)
		if err != nil {
			return nil, err
		}
		return &ParseProfile{Delimiter: &delimiter, Encoding: &encoding, HasHeader: &hasHeader}, nil
	case FormatJSON, FormatNDJSON:
		encoding, err := odcsString(obj, odcs.CustomKeyEncoding)
		if err != nil {
			return nil, err
		}
		return &ParseProfile{Encoding: &encoding}, nil
	default:
		// Excel and API set no parse profile on the native path.
		return nil, nil
	}
}

// odcsString reads a required string custom property. Absence and a
// non-string value are both errors: the native path always set these, so a
// missing or mistyped value means the document cannot reproduce the
// fingerprint and must fail rather than substitute a default.
func odcsString(obj odcs.SchemaObject, key string) (string, error) {
	raw, ok := odcs.CustomProp(obj.CustomProperties, key)
	if !ok {
		return "", fmt.Errorf("fingerprint: ODCS schema object %q missing %s", obj.Name, key)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("fingerprint: ODCS schema object %q has non-string %s", obj.Name, key)
	}
	return s, nil
}

// odcsBool reads a required boolean custom property. YAML and JSON both
// decode a boolean scalar to a Go bool, so a single assertion covers both
// interchange forms.
func odcsBool(obj odcs.SchemaObject, key string) (bool, error) {
	raw, ok := odcs.CustomProp(obj.CustomProperties, key)
	if !ok {
		return false, fmt.Errorf("fingerprint: ODCS schema object %q missing %s", obj.Name, key)
	}
	b, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("fingerprint: ODCS schema object %q has non-bool %s", obj.Name, key)
	}
	return b, nil
}

// odcsCanonicalToken collapses an ODCS property's logicalType + physicalType
// back into the single analyzer-type token mapDataType consumes, applying
// the spec section 5 rules. The physicalType is authoritative where ODCS's
// nine logical types are too coarse: a string property is BINARY when its
// physicalType is bytea (ODCS has no binary logical type) and OBJECT when
// json/jsonb rides under a string, while uuid stays a plain string. An
// array's element type comes from items so the token carries the inner type
// the native path preserved. An empty logical type with an empty/null
// physical token is the unobservable column the analysers emit as empty,
// which maps to UNKNOWN.
//
// The returned token is fed straight to mapDataType, so any pairing the
// rules below do not recognise reaches mapDataType and fails closed there:
// no silent coercion, the same discipline as the native path.
func odcsCanonicalToken(p odcs.Property) string {
	// Native type names arrive through physicalType in whatever case the
	// producing tool used: pg_dump and "datacontract export odcs" routinely
	// emit mixed or upper case. Fold before matching so an upper-case BYTEA
	// or JSONB is recognised rather than silently collapsing to the wrong
	// canonical type. Silent coercion stays forbidden: an unrecognised
	// pairing reaches mapDataType and fails closed there.
	phys := strings.ToLower(strings.TrimSpace(p.PhysicalType))

	// bytea is binary whatever logical type ODCS pairs it with. ODCS has no
	// binary logical type, so the physical type is authoritative and is
	// checked here, before the logical-type switch: a tool that types a
	// bytea column as object/array/etc. still fingerprints it as BINARY
	// rather than losing the binary distinction.
	if phys == "bytea" {
		return "bytea"
	}

	switch p.LogicalType {
	case "":
		// Unobservable column: the analysers carry it as the empty (or, for
		// JSON, null) token, both of which mapDataType reads as UNKNOWN.
		switch phys {
		case "empty", "null", "":
			return "empty"
		}
		return phys
	case odcs.LogicalString:
		switch phys {
		case "json", "jsonb":
			// json/jsonb sometimes rides under a string logical type; it
			// collapses to OBJECT just like the object logical type.
			return phys
		}
		// uuid and every other string physical type collapse to STRING, the
		// analyzer "text" token. mapDataType already folds uuid into STRING,
		// so handing it "text" yields the identical canonical type.
		return "text"
	case odcs.LogicalNumber:
		return "numeric"
	case odcs.LogicalInteger:
		return "integer"
	case odcs.LogicalBoolean:
		return "boolean"
	case odcs.LogicalDate:
		return "date"
	case odcs.LogicalTimestamp:
		return "timestamp"
	case odcs.LogicalTime:
		return "time"
	case odcs.LogicalObject:
		return "object"
	case odcs.LogicalArray:
		// Element-opaque unless items names the inner logical type, matching
		// the native ARRAY vs ARRAY<inner> distinction. The inner token is
		// recursed through odcsCanonicalToken and wrapped in the array[...]
		// form mapDataType decodes.
		if p.Items != nil {
			return "array[" + odcsCanonicalToken(*p.Items) + "]"
		}
		return "array"
	}
	// An unmodelled logical type: hand it through verbatim so mapDataType is
	// the single place that decides whether it is known.
	return string(p.LogicalType)
}
