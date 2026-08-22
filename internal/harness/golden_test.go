package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoldenCreatesThenMatches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "response.json")

	payload := []byte(`{"amount": 19.99, "currency": "GHS", "items": ["a", "b"]}`)

	assertGoldenAt(t, path, payload)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("golden file not created: %v", err)
	}

	assertGoldenAt(t, path, payload)
}

func TestGoldenKeyOrderInsensitive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reordered.json")

	assertGoldenAt(t, path, []byte(`{"amount": 19.99, "currency": "GHS"}`))
	assertGoldenAt(t, path, []byte(`{"currency": "GHS", "amount": 19.99}`))
}

func TestGoldenArrayOrderSensitive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ordered.json")

	assertGoldenAt(t, path, []byte(`{"ids": [1, 2]}`))

	reordered := []byte(`{"ids": [2, 1]}`)
	got := normalizeJSON(t, reordered)
	rawWant, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	want := normalizeJSON(t, rawWant)
	if diffs := diffPaths("$", want, got); len(diffs) == 0 {
		t.Error("array reordering must be detected as mismatch")
	}
}

func TestGoldenScalarMismatchDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scalar.json")

	assertGoldenAt(t, path, []byte(`{"total": 100}`))

	got := normalizeJSON(t, []byte(`{"total": 200}`))
	rawWant, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	want := normalizeJSON(t, rawWant)

	var wantVal, gotVal any
	if err := jsonUnmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if err := jsonUnmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}

	diffs := diffPaths("$", wantVal, gotVal)
	if len(diffs) == 0 {
		t.Fatal("expected mismatch to be detected")
	}
	if !strings.Contains(diffs[0], "$.total") || !strings.Contains(diffs[0], "100") {
		t.Errorf("diff should point at $.total with want value, got: %q", diffs[0])
	}
}

func TestGoldenUpdateModeRewrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "updatable.json")

	assertGoldenAt(t, path, []byte(`{"v": 1}`))

	t.Setenv(updateGoldenEnv, "true")
	assertGoldenAt(t, path, []byte(`{"v": 2}`))
	t.Setenv(updateGoldenEnv, "")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read updated golden: %v", err)
	}
	if !strings.Contains(string(content), `"v":2`) && !strings.Contains(string(content), `"v": 2`) {
		t.Errorf("golden not rewritten, content = %s", content)
	}
}

func jsonUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}
