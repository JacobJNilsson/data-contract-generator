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
	return singleObjectContract(sc.SourcePath, sc.SourcePath, props)
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
	return singleObjectContract(sc.SourcePath, sc.SourcePath, props)
}

// FromDataContract renders a multi-schema native contract (an Excel
// workbook is one schema per sheet) as an ODCS contract with one schema
// object per schema. Each field's DataType is a profile-vocabulary token,
// so it shares propertyFromProfileType with the CSV path. Nullability is
// carried as ODCS required (NOT NULL): a field the analyser saw nulls in
// is left nullable (required unset), one it never saw a null in is marked
// required.
func FromDataContract(dc contract.DataContract) odcs.Contract {
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
			Properties:   props,
		})
	}
	return odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         dc.ID,
		Schema:     objects,
	}
}

// singleObjectContract wraps one schema object's worth of properties into a
// contract, the shape the single-table file analysers (CSV, JSON) produce.
func singleObjectContract(id, name string, props []odcs.Property) odcs.Contract {
	return odcs.Contract{
		APIVersion: odcs.APIVersion,
		Kind:       odcs.KindDataContract,
		ID:         id,
		Schema: []odcs.SchemaObject{
			{
				Name:         name,
				PhysicalName: name,
				PhysicalType: "file",
				Properties:   props,
			},
		},
	}
}
