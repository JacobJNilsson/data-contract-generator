package fingerprint

import (
	"strings"
	"testing"

	"github.com/JacobJNilsson/data-contract-generator/contract"
	"github.com/JacobJNilsson/data-contract-generator/profile"
)

// Refingerprint moves only the algo version: content is untouched, the
// hash follows the new version, and a fresh analysis of the same shape
// under the current rules reproduces the migrated object exactly.
func TestRefingerprint(t *testing.T) {
	o := mustCSV(t, csvContract(csvField("a", profile.TypeText)))
	stored := o
	stored.AlgoVersion = "fp1"

	migrated, hash := Refingerprint(stored)
	if migrated.AlgoVersion != AlgoVersion {
		t.Errorf("algo_version = %q, want %q", migrated.AlgoVersion, AlgoVersion)
	}
	if !strings.HasPrefix(hash, AlgoVersion+":") {
		t.Errorf("hash = %q, want the %s prefix", hash, AlgoVersion)
	}
	if hash != migrated.Hash() {
		t.Errorf("returned hash %q does not match the migrated object's own %q", hash, migrated.Hash())
	}
	if !Match(o, migrated) {
		t.Errorf("a fresh analysis does not Match the migrated object:\nfresh    %+v\nmigrated %+v", o, migrated)
	}
}

// An fp1 Excel object carries no parse profile; migration keeps none, so
// it can only meet a fresh fp2 analysis (which carries header presence)
// as a miss — never as a false hit.
func TestRefingerprintExcelStaysProfileless(t *testing.T) {
	stored := Object{
		AlgoVersion: "fp1",
		Format:      FormatXLSX,
		Fields:      []Field{{Name: "x", Type: TypeString}},
	}
	migrated, _ := Refingerprint(stored)
	if migrated.ParseProfile != nil {
		t.Errorf("migration invented a parse profile: %+v", migrated.ParseProfile)
	}

	hasHeader := true
	fresh := Object{
		AlgoVersion:  AlgoVersion,
		Format:       FormatXLSX,
		ParseProfile: &ParseProfile{HasHeader: &hasHeader},
		Fields:       []Field{{Name: "x", Type: TypeString}},
	}
	if Match(fresh, migrated) {
		t.Error("a profile-carrying fresh analysis must miss a profileless migrated object")
	}
}

// fp2 (XL-5): per-table header presence is identity-bearing for Excel
// units, so a headerless table whose synthesized names equal a literal
// header no longer collides with it.
func TestExcelHeaderPresenceInIdentity(t *testing.T) {
	withHeader := excelContract(true)
	headerless := excelContract(false)

	uw, _, err := FromDataContract(&withHeader)
	if err != nil {
		t.Fatalf("FromDataContract(withHeader): %v", err)
	}
	uh, _, err := FromDataContract(&headerless)
	if err != nil {
		t.Fatalf("FromDataContract(headerless): %v", err)
	}
	if uw[0].Object.Hash() == uh[0].Object.Hash() {
		t.Error("headerless twin shares a hash with the literal-header table")
	}
	if Match(uw[0].Object, uh[0].Object) {
		t.Error("headerless twin Matches the literal-header table")
	}
	if uw[0].Object.ParseProfile == nil || uw[0].Object.ParseProfile.HasHeader == nil || !*uw[0].Object.ParseProfile.HasHeader {
		t.Errorf("parse profile = %+v, want has_header true", uw[0].Object.ParseProfile)
	}
}

// A schema without the detection outcome (an API spec, or a contract
// predating fp2) keeps a nil parse profile.
func TestExcelSchemaWithoutHeaderMetadata(t *testing.T) {
	dc := excelContract(true)
	dc.Schemas[0].Metadata = nil
	units, _, err := FromDataContract(&dc)
	if err != nil {
		t.Fatalf("FromDataContract: %v", err)
	}
	if units[0].Object.ParseProfile != nil {
		t.Errorf("parse profile = %+v, want nil without the metadata", units[0].Object.ParseProfile)
	}
}

// excelContract builds a minimal one-table xlsx contract whose field
// names are the same either way; only the header flag differs.
func excelContract(hasHeader bool) contract.DataContract {
	return contract.DataContract{
		ContractType: "source",
		ID:           "excel",
		Metadata:     map[string]any{"source_format": "xlsx"},
		Schemas: []contract.SchemaContract{{
			Name:     "Sheet1",
			Metadata: map[string]any{"has_header": hasHeader},
			Fields: []contract.FieldDefinition{
				{Name: "column_1", DataType: "text"},
				{Name: "column_2", DataType: "numeric"},
			},
		}},
	}
}
