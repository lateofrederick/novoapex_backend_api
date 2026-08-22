package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

const updateGoldenEnv = "UPDATE_GOLDEN"

func AssertJSONGolden(t *testing.T, name string, actual []byte) {
	t.Helper()
	dir := callerPackageDir(t)
	assertGoldenAt(t, filepath.Join(dir, "testdata", "golden", name+".json"), actual)
}

func assertGoldenAt(t *testing.T, path string, actual []byte) {
	t.Helper()

	got := normalizeJSON(t, actual)

	if os.Getenv(updateGoldenEnv) == "true" {
		writeGolden(path, got)
		t.Logf("golden updated: %s", path)
		return
	}

	rawWant, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		writeGolden(path, got)
		t.Logf("golden created: %s", path)
		return
	}
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}

	want := normalizeJSON(t, rawWant)

	if reflect.DeepEqual(want, got) {
		return
	}

	var wantVal, gotVal any
	_ = json.Unmarshal(want, &wantVal)
	_ = json.Unmarshal(got, &gotVal)
	diffs := diffPaths("$", wantVal, gotVal)
	t.Errorf("golden mismatch in %s:\n  %s",
		path, strings.Join(diffs, "\n  "))
}

func writeGolden(path string, normalized []byte) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(fmt.Sprintf("create golden dir: %v", err))
	}
	if err := os.WriteFile(path, append(normalized, '\n'), 0o644); err != nil {
		panic(fmt.Sprintf("write golden: %v", err))
	}
}

func normalizeJSON(t *testing.T, b []byte) []byte {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("invalid JSON for golden comparison: %v", err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("re-marshal JSON: %v", err)
	}
	return out
}

func diffPaths(path string, want, got any) []string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s: want object, got %s (%v)", path, jsonKind(got), got)}
		}
		var out []string
		for k, wv := range w {
			gv, present := g[k]
			p := path + "." + k
			if !present {
				out = append(out, fmt.Sprintf("%s: missing from actual (want %v)", p, compact(wv)))
				continue
			}
			out = append(out, diffPaths(p, wv, gv)...)
		}
		for k, gv := range g {
			if _, present := w[k]; !present {
				out = append(out, fmt.Sprintf("%s: unexpected in actual (got %v)", path+"."+k, compact(gv)))
			}
		}
		return out
	case []any:
		g, ok := got.([]any)
		if !ok {
			return []string{fmt.Sprintf("%s: want array, got %s (%v)", path, jsonKind(got), got)}
		}
		if len(w) != len(g) {
			return []string{fmt.Sprintf("%s: array length want %d, got %d", path, len(w), len(g))}
		}
		var out []string
		for i := range w {
			out = append(out, diffPaths(fmt.Sprintf("%s[%d]", path, i), w[i], g[i])...)
		}
		return out
	default:
		if !reflect.DeepEqual(want, got) {
			return []string{fmt.Sprintf("%s: want %v, got %v", path, compact(want), compact(got))}
		}
		return nil
	}
}

func compact(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	s := string(b)
	if len(s) > 120 {
		s = s[:117] + "..."
	}
	return s
}

func jsonKind(v any) string {
	if v == nil {
		return "null"
	}
	return reflect.TypeOf(v).Kind().String()
}

func callerPackageDir(t *testing.T) string {
	t.Helper()
	last := ""
	for skip := 1; skip < 20; skip++ {
		_, file, _, ok := runtime.Caller(skip)
		if !ok {
			break
		}
		last = file
		if strings.Contains(file, "/internal/harness/") || strings.HasSuffix(file, "harness\\fixtures.go") ||
			strings.Contains(file, "\\internal\\harness\\") {
			continue
		}
		return filepath.Dir(file)
	}
	if last == "" {
		return "."
	}
	return filepath.Dir(last)
}
