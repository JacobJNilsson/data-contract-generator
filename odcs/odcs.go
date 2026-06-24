// Package odcs models the subset of the Open Data Contract Standard
// (ODCS) v3.1.0 that the data-contract-generator emits and reads. It is
// pure data modelling and serialisation: no I/O, no inference, and no
// dependency on the analysers. The structs marshal to and from both YAML
// (gopkg.in/yaml.v3) and JSON, which are the two interchange forms ODCS
// tools accept.
//
// The model is deliberately a subset. ODCS v3.1.0 defines far more than
// dcg uses; only the fields the platform reads or writes are present, so
// the type stays honest about what dcg actually round-trips. Fields absent
// from a given document marshal away (omitempty) rather than appearing as
// empty placeholders, which keeps emitted documents minimal and stable.
package odcs

// APIVersion is the ODCS schema version this package models.
const APIVersion = "v3.1.0"

// KindDataContract is the only ODCS kind dcg produces.
const KindDataContract = "DataContract"

// dcg custom-property keys. ODCS carries structure and types, but it has no
// first-class home for the source-parse facts the fingerprint treats as
// identity-bearing (the source format kind, and a CSV's delimiter,
// encoding, and header presence). The emitter writes these into a schema
// object's customProperties under the keys below, and the fingerprint reads
// them back, so a fingerprint derived from an ODCS document is byte-
// identical to one derived from the native contract. They are namespaced
// with a "dcg" prefix so they never collide with an authored custom
// property.
const (
	// CustomKeySourceFormat carries the fingerprint source-format kind
	// (csv, json, ndjson, xlsx, api).
	CustomKeySourceFormat = "dcgSourceFormat"
	// CustomKeyDelimiter carries a CSV field delimiter.
	CustomKeyDelimiter = "dcgDelimiter"
	// CustomKeyEncoding carries a source's character encoding.
	CustomKeyEncoding = "dcgEncoding"
	// CustomKeyHasHeader carries whether a CSV had a header row.
	CustomKeyHasHeader = "dcgHasHeader"
)

// CustomProp returns the first custom property with the given key, and
// whether one was present. The lookup is linear because the list is short
// (a handful of dcg keys) and ODCS stores custom properties as an ordered
// array, not a map.
func CustomProp(props []CustomProperty, key string) (any, bool) {
	for _, p := range props {
		if p.Property == key {
			return p.Value, true
		}
	}
	return nil, false
}

// Contract is a complete ODCS data contract document: the top-level
// object an ODCS YAML or JSON file deserialises into. Schema holds one
// SchemaObject per table/section the source exposes.
type Contract struct {
	APIVersion       string           `json:"apiVersion" yaml:"apiVersion"`
	Kind             string           `json:"kind" yaml:"kind"`
	ID               string           `json:"id" yaml:"id"`
	Version          string           `json:"version,omitempty" yaml:"version,omitempty"`
	Schema           []SchemaObject   `json:"schema,omitempty" yaml:"schema,omitempty"`
	CustomProperties []CustomProperty `json:"customProperties,omitempty" yaml:"customProperties,omitempty"`
}

// SchemaObject is one schema in a contract: a table, a view, or a file.
// Objects describe structure only and carry no logicalType of their own;
// the logical typing lives on each Property. PhysicalType is the kind of
// object (table/view/file), not a column type.
type SchemaObject struct {
	Name             string           `json:"name" yaml:"name"`
	PhysicalName     string           `json:"physicalName,omitempty" yaml:"physicalName,omitempty"`
	PhysicalType     string           `json:"physicalType,omitempty" yaml:"physicalType,omitempty"`
	Properties       []Property       `json:"properties,omitempty" yaml:"properties,omitempty"`
	CustomProperties []CustomProperty `json:"customProperties,omitempty" yaml:"customProperties,omitempty"`
}

// CustomProperty is one ODCS custom key/value pair. ODCS v3.1.0 carries
// these as an array of objects (not a map) on both the contract root and
// each schema object. dcg uses them to carry source-parse facts (the
// source format kind, and a CSV delimiter / encoding / header presence)
// that the ODCS column model has no first-class home for but the
// fingerprint needs to stay byte-identical to the native path. Property is
// the key name; Value is a free-form scalar.
type CustomProperty struct {
	Property    string `json:"property" yaml:"property"`
	Value       any    `json:"value" yaml:"value"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	ID          string `json:"id,omitempty" yaml:"id,omitempty"`
}

// LogicalType is one of the nine ODCS v3.1.0 logical types. ODCS has no
// first-class binary or enum type: binary payloads are carried as a string
// logicalType with a physicalType that names the native binary type, and
// enums as a string logicalType with a quality rule (see the emitter).
type LogicalType string

// The nine ODCS v3.1.0 logical types.
const (
	LogicalString    LogicalType = "string"
	LogicalDate      LogicalType = "date"
	LogicalTimestamp LogicalType = "timestamp"
	LogicalTime      LogicalType = "time"
	LogicalNumber    LogicalType = "number"
	LogicalInteger   LogicalType = "integer"
	LogicalObject    LogicalType = "object"
	LogicalArray     LogicalType = "array"
	LogicalBoolean   LogicalType = "boolean"
)

// Property is one column of a schema object. LogicalType is one of the
// nine; PhysicalType is the free-form native type (e.g. "text", "uuid",
// "account_status"). Required is nullability with ODCS semantics: true
// means NOT NULL. Items carries the element typing of an array property.
type Property struct {
	Name         string      `json:"name" yaml:"name"`
	LogicalType  LogicalType `json:"logicalType,omitempty" yaml:"logicalType,omitempty"`
	PhysicalType string      `json:"physicalType,omitempty" yaml:"physicalType,omitempty"`

	// Required is nullability under ODCS semantics: true = NOT NULL. It is
	// a pointer so an unspecified nullability (nil) is distinct from an
	// explicit false, and absence marshals away rather than asserting the
	// column is nullable when the source never said so.
	Required *bool `json:"required,omitempty" yaml:"required,omitempty"`

	PrimaryKey         *bool `json:"primaryKey,omitempty" yaml:"primaryKey,omitempty"`
	PrimaryKeyPosition *int  `json:"primaryKeyPosition,omitempty" yaml:"primaryKeyPosition,omitempty"`
	Unique             *bool `json:"unique,omitempty" yaml:"unique,omitempty"`

	LogicalTypeOptions *LogicalTypeOptions `json:"logicalTypeOptions,omitempty" yaml:"logicalTypeOptions,omitempty"`

	// Items carries the element typing of an array property. It is only
	// meaningful when LogicalType is "array"; nil otherwise.
	Items *Property `json:"items,omitempty" yaml:"items,omitempty"`

	Quality []Quality `json:"quality,omitempty" yaml:"quality,omitempty"`
}

// LogicalTypeOptions carries the ODCS constraint refinements that apply to
// a logical type: string-shape constraints (length, pattern, format) and
// numeric-range constraints (minimum, maximum, multipleOf). Every field is
// a pointer so an unspecified constraint marshals away rather than
// asserting a zero bound the source never observed.
type LogicalTypeOptions struct {
	MinLength *int   `json:"minLength,omitempty" yaml:"minLength,omitempty"`
	MaxLength *int   `json:"maxLength,omitempty" yaml:"maxLength,omitempty"`
	Pattern   string `json:"pattern,omitempty" yaml:"pattern,omitempty"`
	Format    string `json:"format,omitempty" yaml:"format,omitempty"`

	Minimum    *float64 `json:"minimum,omitempty" yaml:"minimum,omitempty"`
	Maximum    *float64 `json:"maximum,omitempty" yaml:"maximum,omitempty"`
	MultipleOf *float64 `json:"multipleOf,omitempty" yaml:"multipleOf,omitempty"`
}

// QualityType is the ODCS quality-rule kind.
type QualityType string

// The ODCS quality-rule kinds dcg uses.
const (
	QualityText    QualityType = "text"
	QualityLibrary QualityType = "library"
	QualitySQL     QualityType = "sql"
	QualityCustom  QualityType = "custom"
)

// QualityMetric is the ODCS library-metric name a library quality rule
// asserts against.
type QualityMetric string

// The ODCS library metrics dcg uses.
const (
	MetricNullValues      QualityMetric = "nullValues"
	MetricMissingValues   QualityMetric = "missingValues"
	MetricInvalidValues   QualityMetric = "invalidValues"
	MetricDuplicateValues QualityMetric = "duplicateValues"
	MetricRowCount        QualityMetric = "rowCount"
)

// Quality is one quality rule on a property. dcg's primary use is the enum
// rule: type=library, metric=invalidValues, arguments.validValues carrying
// the ordered label set, mustBe=0, unit=rows. Arguments is free-form (the
// ODCS JSON Schema does not validate it), so the emitter and reader own the
// round-trip of any structure carried there.
//
// The MustBe family are typed as any so each carries whatever JSON/YAML
// scalar an ODCS rule compares against; with omitempty, an unset threshold
// (nil) marshals away rather than asserting a zero bound.
type Quality struct {
	ID        string         `json:"id,omitempty" yaml:"id,omitempty"`
	Type      QualityType    `json:"type,omitempty" yaml:"type,omitempty"`
	Metric    QualityMetric  `json:"metric,omitempty" yaml:"metric,omitempty"`
	Arguments map[string]any `json:"arguments,omitempty" yaml:"arguments,omitempty"`

	MustBe            any `json:"mustBe,omitempty" yaml:"mustBe,omitempty"`
	MustNotBe         any `json:"mustNotBe,omitempty" yaml:"mustNotBe,omitempty"`
	MustBeGreaterThan any `json:"mustBeGreaterThan,omitempty" yaml:"mustBeGreaterThan,omitempty"`
	MustBeLessThan    any `json:"mustBeLessThan,omitempty" yaml:"mustBeLessThan,omitempty"`

	Unit string `json:"unit,omitempty" yaml:"unit,omitempty"`
}
