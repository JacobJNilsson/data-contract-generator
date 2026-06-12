package csvcontract

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestAnalyzeFieldShape pins the serialized shape signature end to
// end: the platform stores this JSON at authoring time and compares
// landed files against it, so the exact wire shape is contract.
func TestAnalyzeFieldShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "products.csv")
	content := "sku,name\nP-600,Kedjespannare K2\nP-601,Drevsats K13\nP-602,Styrlager konisk\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	sc, err := AnalyzeFile(context.Background(), path, nil)
	if err != nil {
		t.Fatalf("AnalyzeFile() = %v, want nil", err)
	}
	if len(sc.Fields) != 2 {
		t.Fatalf("fields = %d, want 2", len(sc.Fields))
	}
	got, err := json.Marshal(sc.Fields[0].Profile.Shape)
	if err != nil {
		t.Fatalf("marshal shape: %v", err)
	}
	want := `{"masks":[{"mask":"A-9","count":3}],"dominant_share":1,"length_min":5,"length_max":5}`
	if string(got) != want {
		t.Errorf("sku shape = %s, want %s", got, want)
	}
	name := sc.Fields[1].Profile.Shape
	if name.Masks[0].Mask != "A_A9" || name.DominantShare != 0.67 {
		t.Errorf("name shape = %+v, want dominant A_A9 at 0.67", name)
	}
}
