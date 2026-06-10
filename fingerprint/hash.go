package fingerprint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// CanonicalBytes serializes the object into its canonical JSON form: keys in
// fixed (alphabetical) order via struct declaration order, fields pre-sorted
// by name, fixed type tokens, UTF-8, no insignificant whitespace, absent
// values as explicit nulls, and no HTML escaping so the byte form is plain
// RFC 8259 JSON reproducible outside Go. Same object ⇒ same bytes, with no
// dependence on map order, locale, or platform.
func (o Object) CanonicalBytes() []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	// Encoding a struct of strings, bools, and slices cannot fail.
	_ = enc.Encode(o)
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
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
	if !bytes.Equal(a.Nesting, b.Nesting) {
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
