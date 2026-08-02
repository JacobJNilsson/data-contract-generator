package fingerprint

// Refingerprint re-stamps a stored fingerprint object under the current
// AlgoVersion and returns it with its recomputed hash. It is the re-
// fingerprint migration seam (XL-5): a cache that stores objects beside
// their hashes replays each stored object through this function and
// updates the row, so existing entries keep their pipelines across an
// algorithm bump instead of re-authoring.
//
// The object's content is carried unchanged; only the version (and
// therefore the hash) moves. A fresh analysis of the same file then
// reproduces the migrated object exactly, except for content the old
// version never recorded: an fp1 Excel object carries no parse profile,
// so after migration it can only meet a fresh fp2 analysis (which
// carries header presence) as a cache miss that re-authors. The miss is
// the safe direction — cheap, loud, and never a mis-route.
func Refingerprint(o Object) (Object, string) {
	o.AlgoVersion = AlgoVersion
	return o, o.Hash()
}
