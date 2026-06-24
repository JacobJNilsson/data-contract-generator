package odcsemit

import "github.com/JacobJNilsson/data-contract-generator/odcs"

// propertyFromProfileType encodes a profile-vocabulary type token (the CSV
// and Excel analysers' DataType: text, numeric, date, timestamp, boolean,
// empty) onto an ODCS property. The native token is echoed into
// physicalType so the fingerprint can recover the same canonical type from
// the ODCS document; for the empty token, where there is no logical type to
// assign, the token is still carried in physicalType so the unknown column
// stays distinguishable on the way back.
func propertyFromProfileType(name, dataType string) odcs.Property {
	p := odcs.Property{Name: name}
	switch dataType {
	case "text":
		p.LogicalType = odcs.LogicalString
		p.PhysicalType = "text"
	case "numeric":
		p.LogicalType = odcs.LogicalNumber
		p.PhysicalType = "numeric"
	case "date":
		p.LogicalType = odcs.LogicalDate
		p.PhysicalType = "date"
		p.LogicalTypeOptions = &odcs.LogicalTypeOptions{Format: "date"}
	case "timestamp":
		p.LogicalType = odcs.LogicalTimestamp
		p.PhysicalType = "timestamp"
		p.LogicalTypeOptions = &odcs.LogicalTypeOptions{Format: "date-time"}
	case "boolean":
		p.LogicalType = odcs.LogicalBoolean
		p.PhysicalType = "boolean"
	case "empty":
		// No observable type. ODCS has no unknown logical type, so leave
		// logicalType empty and carry the native token in physicalType. The
		// fingerprint maps "no logicalType, physicalType empty/null" back to
		// its UNKNOWN canonical type.
		p.PhysicalType = "empty"
	default:
		// Unrecognised profile token. Echo it into physicalType rather than
		// guessing a logical type; the fingerprint fails closed on it, which
		// surfaces the gap instead of hiding it.
		p.PhysicalType = dataType
	}
	return p
}

// propertyFromJSONType encodes a jsoncontract type token (text, numeric,
// date, boolean, object, array, null, empty) onto an ODCS property. It
// extends the profile encoding with the object and array shapes JSON
// exposes. The JSON analyser does not expose array element types, so an
// array is emitted element-opaque (no items); a DB source that does know
// its element type slots in through the same array branch by setting items.
func propertyFromJSONType(name, dataType string) odcs.Property {
	switch dataType {
	case "object":
		return odcs.Property{Name: name, LogicalType: odcs.LogicalObject, PhysicalType: "object"}
	case "array":
		// Element-opaque: the JSON analyser does not expose the element
		// type, so items stays nil and the fingerprint reads this as the
		// bare ARRAY token, matching the native path.
		return odcs.Property{Name: name, LogicalType: odcs.LogicalArray, PhysicalType: "array"}
	case "null":
		// JSON's all-null token is the unknown column, same canonical fate
		// as the profile "empty" token; carry it in physicalType.
		return odcs.Property{Name: name, PhysicalType: "null"}
	default:
		// text, numeric, date, boolean, empty share the profile encoding.
		return propertyFromProfileType(name, dataType)
	}
}

// EnumProperty builds the ODCS encoding of a named enum column: a string
// logical type, the enum type name in physicalType, and a library quality
// rule carrying the ordered label set (spec section 4.2). ODCS v3.1.0 has
// no first-class enum type and no validValues field, so this rule is how
// the label set survives the round-trip. The file analysers do not yet
// surface enums; this is the seam a DB source plugs into, and it is proven
// by the enum round-trip test.
func EnumProperty(name, enumTypeName string, labels []string) odcs.Property {
	values := make([]any, len(labels))
	for i, l := range labels {
		values[i] = l
	}
	zero := 0
	return odcs.Property{
		Name:         name,
		LogicalType:  odcs.LogicalString,
		PhysicalType: enumTypeName,
		Quality: []odcs.Quality{
			{
				ID:     name + "_valid_values",
				Type:   odcs.QualityLibrary,
				Metric: odcs.MetricInvalidValues,
				Arguments: map[string]any{
					"validValues": values,
				},
				MustBe: zero,
				Unit:   "rows",
			},
		},
	}
}

// ReadEnumLabels recovers the ordered label set and native enum type name
// from an enum property encoded by EnumProperty. The bool is false when the
// property does not carry an enum quality rule. This is the reader half of
// the round-trip the spec requires us to own (arguments is free-form, so
// only our own encoder/decoder pair guarantees the labels survive).
func ReadEnumLabels(p odcs.Property) (typeName string, labels []string, ok bool) {
	for _, q := range p.Quality {
		if q.Type != odcs.QualityLibrary || q.Metric != odcs.MetricInvalidValues {
			continue
		}
		raw, present := q.Arguments["validValues"]
		if !present {
			continue
		}
		list, isList := raw.([]any)
		if !isList {
			continue
		}
		labels = make([]string, 0, len(list))
		for _, v := range list {
			s, isStr := v.(string)
			if !isStr {
				return "", nil, false
			}
			labels = append(labels, s)
		}
		return p.PhysicalType, labels, true
	}
	return "", nil, false
}
