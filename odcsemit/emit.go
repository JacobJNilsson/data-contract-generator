// Package odcsemit renders dcg's native analyser results into ODCS
// contracts. It is the one-way bridge from the analyser vocabulary
// (csvcontract / jsoncontract / contract) into the odcs model: one entry
// point per source shape, each producing an odcs.Contract.
//
// The emitter owns the type encoding (spec section 4.1) and the enum
// quality-rule encoding (spec section 4.2). It deliberately carries the
// original native type token in physicalType so the fingerprint can derive
// the same canonical type from the ODCS document that it derives from the
// native contract, which is what makes the DG-3 byte-identity proof
// possible. Profiles and statistics are not pushed into ODCS; they stay a
// dcg-native artefact (spec section 4.3).
package odcsemit

import (
	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/csvcontract"
	"github.com/JacobJNilsson/data-contract-generator/jsoncontract"
	"github.com/JacobJNilsson/data-contract-generator/odcs"
)

// FromSourceContract renders a CSV analysis as a single-object ODCS
// contract. The schema object is named for the source path; each field
// becomes a property typed per encodeProfileType.
func FromSourceContract(sc csvcontract.SourceContract) odcs.Contract {
	props := make([]odcs.Property, 0, len(sc.Fields))
	for _, f := range sc.Fields {
		props = append(props, propertyFromProfileType(f.Name, string(f.DataType)))
	}
	// Carry the CSV parse facts the fingerprint treats as identity-bearing
	// (format, delimiter, encoding, header presence). The ODCS column model
	// has no home for these, so they ride in the schema object's custom
	// properties; the fingerprint reads them back to stay byte-identical.
	custom := []odcs.CustomProperty{
		{Property: odcs.CustomKeySourceFormat, Value: "csv"},
		{Property: odcs.CustomKeyDelimiter, Value: sc.Delimiter},
		{Property: odcs.CustomKeyEncoding, Value: sc.Encoding},
		{Property: odcs.CustomKeyHasHeader, Value: sc.HasHeader},
	}
	return singleObjectContract(sc.SourcePath, sc.SourcePath, props, custom)
}

// FromJSONContract renders a JSON or NDJSON analysis as a single-object
// ODCS contract. JSON exposes object and array fields the CSV path never
// produces, so its property encoding is the superset handled by
// propertyFromJSONType.
func FromJSONContract(sc jsoncontract.SourceContract) odcs.Contract {
	props := make([]odcs.Property, 0, len(sc.Fields))
	for _, f := range sc.Fields {
		props = append(props, propertyFromJSONType(f.Name, string(f.DataType)))
	}
	// JSON and NDJSON parse differently, so the source format is identity-
	// bearing; encoding is the only parse fact the JSON fingerprint carries
	// (no delimiter, no header). The format token is echoed verbatim so an
	// unrecognised value reaches the fingerprint, which fails closed on it.
	custom := []odcs.CustomProperty{
		{Property: odcs.CustomKeySourceFormat, Value: sc.SourceFormat},
		{Property: odcs.CustomKeyEncoding, Value: sc.Encoding},
	}
	return singleObjectContract(sc.SourcePath, sc.SourcePath, props, custom)
}

// FromDataContract renders a multi-schema native contract (an Excel
// workbook is one schema per sheet) as an ODCS contract with one schema
// object per schema. Each field's DataType is a profile-vocabulary token,
// so it shares propertyFromProfileType with the CSV path. Nullability is
// carried as ODCS required (NOT NULL): a field the analyser saw nulls in
// is left nullable (required unset), one it never saw a null in is marked
// required.
func FromDataContract(dc contract.DataContract) odcs.Contract {
	// The native fingerprint reads the source-format kind from contract
	// metadata (xlsx -> the Excel format, openapi -> the API format) and
	// stamps it on every unit. Echo whatever the metadata implies onto each
	// schema object so the fingerprint recovers the same format token; an
	// absent or unrecognised marker rides through as an empty token, which
	// the fingerprint then rejects, matching the native path's refusal to
	// guess a format.
	format := dataContractFormat(dc)
	objects := make([]odcs.SchemaObject, 0, len(dc.Schemas))
	for _, schema := range dc.Schemas {
		props := make([]odcs.Property, 0, len(schema.Fields))
		for _, f := range schema.Fields {
			p := propertyFromProfileType(f.Name, f.DataType)
			if !f.Nullable {
				req := true
				p.Required = &req
			}
			props = append(props, p)
		}
		objects = append(objects, odcs.SchemaObject{
			Name:         schema.Name,
			PhysicalName: schema.Name,
			PhysicalType: "table",
			CustomProperties: []odcs.CustomProperty{
				{Property: odcs.CustomKeySourceFormat, Value: format},
			},
			Properties: props,
		})
	}
	return odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         dc.ID,
		Status:     odcs.StatusActive,
		Schema:     objects,
	}
}

// dataContractFormat maps the native contract's metadata markers to the
// fingerprint source-format token. It mirrors the fingerprint's own
// dataContractFormat: xlsx metadata is the Excel format, an openapi source
// is the API format. Anything else yields an empty token, deferring the
// refusal to the fingerprint so the emitter never invents a format.
func dataContractFormat(dc contract.DataContract) string {
	if sf, ok := dc.Metadata["source_format"].(string); ok && sf == "xlsx" {
		return "xlsx"
	}
	if src, ok := dc.Metadata["source"].(string); ok && src == "openapi" {
		return "api"
	}
	return ""
}

// singleObjectContract wraps one schema object's worth of properties into a
// contract, the shape the single-table file analysers (CSV, JSON) produce.
// custom carries the schema object's source-parse facts.
func singleObjectContract(id, name string, props []odcs.Property, custom []odcs.CustomProperty) odcs.Contract {
	return odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         id,
		Status:     odcs.StatusActive,
		Schema: []odcs.SchemaObject{
			{
				Name:             name,
				PhysicalName:     name,
				PhysicalType:     "file",
				CustomProperties: custom,
				Properties:       props,
			},
		},
	}
}
