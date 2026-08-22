package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"
)

// parReplayTimeout is the floor applied when the caller supplies no client or
// a client without a timeout (T2.8 requires >= 240000ms).
const parReplayTimeout = 240 * time.Second

// parMissingSentinel marks an absent side of a comparison. It can never
// collide with real JSON content because parCompact always quotes strings.
const parMissingSentinel = "<missing>"

// DiffJSON structurally compares two JSON documents and returns
// path-addressed diffs ("$.data[2].total: want 100, got 101"), or nil when
// equal.
//
// Semantics:
//   - object key order is irrelevant; array order is significant (index-paired)
//   - numbers compare numerically only when BOTH sides are JSON numbers, so
//     51 == 51.0 == 5.1e1, but the string "51" never equals either
//   - null vs missing key are distinct: {"a":null} vs {} is a diff at $.a,
//     while {"a":null} vs {"a":null} matches
//   - a type mismatch collapses to one diff line at that path (no recursion)
//   - undecodable input yields "$: want/got is not valid JSON (...)" lines
func DiffJSON(want, got []byte) []string {
	wv, werr := parDecode(want)
	gv, gerr := parDecode(got)
	if werr != nil || gerr != nil {
		var out []string
		if werr != nil {
			out = append(out, fmt.Sprintf("$: want is not valid JSON (%v)", werr))
		}
		if gerr != nil {
			out = append(out, fmt.Sprintf("$: got is not valid JSON (%v)", gerr))
		}
		return out
	}
	return parDiff("$", wv, gv)
}

// EqualJSON reports whether DiffJSON finds no differences.
func EqualJSON(want, got []byte) bool {
	return len(DiffJSON(want, got)) == 0
}

// ReplayResult captures one pairwise node-vs-go replay outcome.
type ReplayResult struct {
	Path   string
	Status int // status returned by the Go stack
	Diffs  []string
	Err    error // transport/infrastructure failure for this exchange, if any
}

// ReplayOptions tunes ReplayCorpus behaviour.
type ReplayOptions struct {
	// PathPrefix restricts replay to exchanges whose path has this prefix;
	// empty replays everything.
	PathPrefix string
	// StripFields names fields (e.g. timestamps/ids known volatile) removed
	// recursively before diffing. A field is pruned only where it exists on
	// BOTH sides; a one-sided field still surfaces as missing/unexpected so
	// genuine omissions are not masked.
	StripFields []string
	// FailFast stops after the first exchange that errors or diverges.
	FailFast bool
}

// ReplayCorpus loads a recorded corpus and replays every exchange against
// both stacks (node = baseline "want", go = candidate "got"), diffing the two
// responses pairwise. Each request is reconstructed fresh per stack so body
// readers are never shared.
func ReplayCorpus(corpusPath, nodeBaseURL, goBaseURL string, client *http.Client, opts ReplayOptions) ([]ReplayResult, error) {
	exchanges, err := LoadCorpus(corpusPath)
	if err != nil {
		return nil, err
	}
	if opts.PathPrefix != "" {
		exchanges = FilterByPathPrefix(exchanges, opts.PathPrefix)
	}

	httpClient := parEffectiveClient(client)
	results := make([]ReplayResult, 0, len(exchanges))
	for _, ex := range exchanges {
		res := parReplayExchange(httpClient, ex, nodeBaseURL, goBaseURL, opts)
		results = append(results, res)
		if opts.FailFast && (res.Err != nil || len(res.Diffs) > 0) {
			break
		}
	}
	return results, nil
}

// Summarize renders results as a compact CI-friendly report.
func Summarize(results []ReplayResult) string {
	var b strings.Builder
	matched := 0
	for _, r := range results {
		if r.Err == nil && len(r.Diffs) == 0 {
			matched++
		}
	}
	fmt.Fprintf(&b, "parity: %d/%d exchanges matched", matched, len(results))
	for i, r := range results {
		switch {
		case r.Err != nil:
			fmt.Fprintf(&b, "\nFAIL [%d] %s: %v", i, r.Path, r.Err)
		case len(r.Diffs) > 0:
			fmt.Fprintf(&b, "\nFAIL [%d] %s (status %d): %d diff(s)", i, r.Path, r.Status, len(r.Diffs))
			for _, d := range r.Diffs {
				fmt.Fprintf(&b, "\n    %s", d)
			}
		}
	}
	return b.String()
}

func parReplayExchange(client *http.Client, ex Exchange, nodeBaseURL, goBaseURL string, opts ReplayOptions) ReplayResult {
	result := ReplayResult{Path: ex.Path}

	nodeResp, err := parSend(client, ex, nodeBaseURL)
	if err != nil {
		result.Err = fmt.Errorf("node: %w", err)
		return result
	}
	goResp, err := parSend(client, ex, goBaseURL)
	if err != nil {
		result.Err = fmt.Errorf("go: %w", err)
		return result
	}
	result.Status = goResp.status

	if nodeResp.status != goResp.status {
		result.Diffs = append(result.Diffs,
			fmt.Sprintf("$status: want %d, got %d", nodeResp.status, goResp.status))
	}

	nodeBody, goBody := parStripVolatile(nodeResp.body, goResp.body, opts.StripFields)
	result.Diffs = append(result.Diffs, DiffJSON(nodeBody, goBody)...)
	return result
}

type parResponse struct {
	status int
	body   []byte
}

// parSend reconstructs the exchange's request against baseURL (fresh body
// reader each call) and performs it, draining and closing the response body.
func parSend(client *http.Client, ex Exchange, baseURL string) (*parResponse, error) {
	req, err := ex.ReconstructRequest(baseURL)
	if err != nil {
		return nil, fmt.Errorf("reconstruct %s %s: %w", ex.Method, ex.Path, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send %s %s: %w", req.Method, req.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response %s %s: %w", req.Method, req.URL, err)
	}
	return &parResponse{status: resp.StatusCode, body: body}, nil
}

func parEffectiveClient(client *http.Client) *http.Client {
	switch {
	case client == nil:
		return &http.Client{Timeout: parReplayTimeout}
	case client.Timeout <= 0:
		c := *client
		c.Timeout = parReplayTimeout
		return &c
	default:
		return client
	}
}

// parStripVolatile removes fields named in strip from both bodies before
// diffing. Pruning is pair-aware: a field is dropped only where it exists at
// the same location in BOTH documents, so a one-sided volatile-looking field
// still surfaces as missing/unexpected instead of being masked. Undecodable
// input passes through untouched so DiffJSON reports it path-addressed.
func parStripVolatile(nodeRaw, goRaw []byte, strip []string) ([]byte, []byte) {
	if len(strip) == 0 {
		return nodeRaw, goRaw
	}
	nodeVal, nerr := parDecode(nodeRaw)
	goVal, gerr := parDecode(goRaw)
	if nerr != nil || gerr != nil {
		return nodeRaw, goRaw
	}
	parPrunePair(&nodeVal, &goVal, strip)
	nodeOut, err := json.Marshal(nodeVal)
	if err != nil {
		return nodeRaw, goRaw
	}
	goOut, err := json.Marshal(goVal)
	if err != nil {
		return nodeRaw, goRaw
	}
	return nodeOut, goOut
}

func parPrunePair(want, got *any, strip []string) {
	wMap, wObj := (*want).(map[string]any)
	gMap, gObj := (*got).(map[string]any)
	if wObj && gObj {
		for _, field := range strip {
			_, inWant := wMap[field]
			_, inGot := gMap[field]
			if inWant && inGot {
				delete(wMap, field)
				delete(gMap, field)
			}
		}
		for k := range wMap {
			if gv, present := gMap[k]; present {
				wv := wMap[k]
				parPrunePair(&wv, &gv, strip)
				wMap[k] = wv
				gMap[k] = gv
			}
		}
		return
	}
	wArr, wList := (*want).([]any)
	gArr, gList := (*got).([]any)
	if !wList || !gList {
		return
	}
	n := len(wArr)
	if len(gArr) < n {
		n = len(gArr)
	}
	for i := 0; i < n; i++ {
		parPrunePair(&wArr[i], &gArr[i], strip)
	}
}

// parDecode parses exactly one JSON value, preserving number literals via
// json.Number and rejecting trailing data.
func parDecode(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after JSON value")
	}
	return v, nil
}

func parDiff(path string, want, got any) []string {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return []string{parMismatch(path, want, got)}
		}
		return parDiffObject(path, w, g)
	case []any:
		g, ok := got.([]any)
		if !ok {
			return []string{parMismatch(path, want, got)}
		}
		if len(w) != len(g) {
			return []string{fmt.Sprintf("%s: array length want %d, got %d", path, len(w), len(g))}
		}
		var out []string
		for i := range w {
			out = append(out, parDiff(fmt.Sprintf("%s[%d]", path, i), w[i], g[i])...)
		}
		return out
	default:
		if !parScalarEqual(want, got) {
			return []string{parMismatch(path, want, got)}
		}
		return nil
	}
}

func parDiffObject(path string, want, got map[string]any) []string {
	var out []string
	wantKeys := parSortedKeys(want)
	for _, k := range wantKeys {
		p := path + "." + k
		gv, present := got[k]
		if !present {
			out = append(out, fmt.Sprintf("%s: want %s, got %s", p, parCompact(want[k]), parMissingSentinel))
			continue
		}
		out = append(out, parDiff(p, want[k], gv)...)
	}
	for _, k := range parSortedKeys(got) {
		if _, present := want[k]; !present {
			out = append(out, fmt.Sprintf("%s: want %s, got %s", path+"."+k, parMissingSentinel, parCompact(got[k])))
		}
	}
	return out
}

func parSortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// parScalarEqual compares leaves. Numbers match numerically only when both
// sides are json.Number; strings never coerce to numbers; bool/null compare
// by identity of decoded kind.
func parScalarEqual(want, got any) bool {
	wNum, wIsNum := want.(json.Number)
	gNum, gIsNum := got.(json.Number)
	if wIsNum || gIsNum {
		return wIsNum && gIsNum && parNumbersEqual(wNum, gNum)
	}
	return want == got
}

// parNumbersEqual compares numeric literals exactly: "51" == "51.0" ==
// "5.1e1". Malformed literals fall back to textual equality.
func parNumbersEqual(a, b json.Number) bool {
	ra, okA := new(big.Rat).SetString(a.String())
	rb, okB := new(big.Rat).SetString(b.String())
	if !okA || !okB {
		return a.String() == b.String()
	}
	return ra.Cmp(rb) == 0
}

func parMismatch(path string, want, got any) string {
	return fmt.Sprintf("%s: want %s, got %s", path, parCompact(want), parCompact(got))
}

// parCompact renders a decoded value as bounded JSON text.
func parCompact(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	s := string(b)
	const limit = 120
	if len(s) > limit {
		s = s[:limit-3] + "..."
	}
	return s
}
