package fingerprint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
)

// CanonicalBytes serializes the object into its canonical JSON form: keys
// sorted, fields pre-sorted by name, fixed type tokens, UTF-8, no
// insignificant whitespace, absent values as explicit nulls. Same object ⇒
// same bytes, with no dependence on map order, locale, or platform.
func (o Object) CanonicalBytes() []byte {
	var b bytes.Buffer
	b.WriteString(`{"algo_version":`)
	writeString(&b, o.AlgoVersion)
	b.WriteString(`,"fields":[`)
	for i, f := range o.Fields {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"name":`)
		writeString(&b, f.Name)
		b.WriteString(`,"type":`)
		writeString(&b, string(f.Type))
		b.WriteByte('}')
	}
	b.WriteString(`],"format":`)
	writeString(&b, string(o.Format))
	b.WriteString(`,"nesting":null,"parse_profile":`)
	writeParseProfile(&b, o.ParseProfile)
	b.WriteByte('}')
	return b.Bytes()
}

// Hash returns the cache-key hash: the algorithm version prefixing a SHA-256
// of the canonical bytes, e.g. "fp1:9f2c…". The version prefix guarantees an
// old hash is never reinterpreted under new canonicalisation rules.
func (o Object) Hash() string {
	sum := sha256.Sum256(o.CanonicalBytes())
	return o.AlgoVersion + ":" + hex.EncodeToString(sum[:])
}

// Match reports whether two objects are structurally identical. A cache hit
// requires hash equality AND Match: the stored object is the collision
// guard, so even a SHA-256 collision cannot silently mis-route a file.
func Match(a, b Object) bool {
	if a.AlgoVersion != b.AlgoVersion || a.Format != b.Format {
		return false
	}
	if !parseProfileEqual(a.ParseProfile, b.ParseProfile) {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

func parseProfileEqual(a, b *ParseProfile) bool {
	if a == nil || b == nil {
		return a == b
	}
	return strPtrEqual(a.Delimiter, b.Delimiter) &&
		strPtrEqual(a.Encoding, b.Encoding) &&
		boolPtrEqual(a.HasHeader, b.HasHeader)
}

func strPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func writeParseProfile(b *bytes.Buffer, p *ParseProfile) {
	if p == nil {
		b.WriteString("null")
		return
	}
	b.WriteString(`{"delimiter":`)
	writeStringPtr(b, p.Delimiter)
	b.WriteString(`,"encoding":`)
	writeStringPtr(b, p.Encoding)
	b.WriteString(`,"has_header":`)
	if p.HasHeader == nil {
		b.WriteString("null")
	} else {
		b.WriteString(strconv.FormatBool(*p.HasHeader))
	}
	b.WriteByte('}')
}

func writeStringPtr(b *bytes.Buffer, s *string) {
	if s == nil {
		b.WriteString("null")
		return
	}
	writeString(b, *s)
}

// writeString writes a JSON string literal. encoding/json string encoding is
// deterministic, and a plain string value cannot fail to marshal.
func writeString(b *bytes.Buffer, s string) {
	encoded, _ := json.Marshal(s)
	b.Write(encoded)
}
