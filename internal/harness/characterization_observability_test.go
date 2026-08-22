package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/novoapex/novoapex-backend-api/internal/observability/metrics"
)

const obscolProcessCPUFamily = "process_cpu_seconds_total"

type obscolParityReport struct {
	CustomAppMetrics   []string `json:"custom_app_metrics"`
	GoOnlyFamilies     []string `json:"go_only_families"`
	GoRegistryFamilies []string `json:"go_registry_families"`
	KnownGaps          []string `json:"known_gaps"`
	NodeFamilies       []string `json:"node_families"`
	NodeOnlyFamilies   []string `json:"node_only_families"`
	SharedFamilies     []string `json:"shared_families"`
	Source             string   `json:"source"`
}

func TestT030_MetricNamesParityWithNode(t *testing.T) {
	h := startHarness(t)
	repoDir := harnessRepoDir(t)
	applyMigrations(t, h.PostgresDSN, repoDir)
	stack := StartNodeStack(t, h)

	nodeBody := obscolFetchMetrics(t, stack)
	nodeFamilies := obscolParseFamilies(nodeBody)

	goFamilies := obscolRenderGoFamilies(t)

	if len(nodeFamilies) < 20 {
		t.Fatalf("node /metrics exposed %d families, expected >=20\n--- logs ---\n%s", len(nodeFamilies), stack.DumpLogs())
	}
	if len(goFamilies) < 4 {
		t.Fatalf("go registry exposed %d families, expected >=4", len(goFamilies))
	}

	customApp := obscolSetDiff(nodeFamilies, metrics.NodeDefaultMetricFamilies)
	shared := obscolIntersect(nodeFamilies, goFamilies)
	nodeOnly := obscolSetDiff(nodeFamilies, goFamilies)
	goOnly := obscolSetDiff(goFamilies, nodeFamilies)

	missingPorts := obscolSetDiff(customApp, goFamilies)
	for _, name := range missingPorts {
		t.Errorf("custom app metric %q served by node is missing from go registry", name)
	}
	if !obscolContains(shared, obscolProcessCPUFamily) {
		t.Errorf("%q must be served by both stacks (node=%v go=%v)", obscolProcessCPUFamily, nodeFamilies, goFamilies)
	}
	if len(customApp) == 0 {
		t.Logf("no custom app metrics found on node /metrics (defaults only); nothing to port")
	}

	report := obscolParityReport{
		CustomAppMetrics:   obscolOrEmpty(customApp),
		GoOnlyFamilies:     obscolOrEmpty(goOnly),
		GoRegistryFamilies: obscolOrEmpty(goFamilies),
		KnownGaps:          obscolOrEmpty(metrics.KnownGaps),
		NodeFamilies:       obscolOrEmpty(nodeFamilies),
		NodeOnlyFamilies:   obscolOrEmpty(nodeOnly),
		SharedFamilies:     obscolOrEmpty(shared),
		Source:             "GET /metrics",
	}
	payload, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal parity report: %v", err)
	}
	assertGoldenAt(t, obscolGoldenPath(t), payload)
}

func TestT029_GoLogShapeLokiCompatible(t *testing.T) {
	requireDocker(t)
	ctx := t.Context()
	lokiURL := obscolStartLoki(t, ctx)

	now := time.Now()
	entry := map[string]any{
		"level":          30,
		"time":           now.UnixMilli(),
		"pid":            4242,
		"hostname":       "novoapex-api-parity",
		"msg":            "customer asked about delivery window",
		"event":          "conversation.inbound",
		"conversationId": "conv_parity_0001",
		"direction":      "inbound",
		"messageType":    "text",
	}
	line, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal log line: %v", err)
	}

	labels := map[string]string{
		"container":      "novoapex_api",
		"service":        "api",
		"level":          "30",
		"event":          "conversation.inbound",
		"conversationId": "conv_parity_0001",
		"direction":      "inbound",
		"messageType":    "text",
	}
	tsNs := strconv.FormatInt(now.UnixNano(), 10)
	obscolLokiPush(t, lokiURL, labels, tsNs, string(line))

	query := `{container="novoapex_api", service="api", event="conversation.inbound"}`
	streams := obscolQueryRange(t, lokiURL, query, now)
	if len(streams) != 1 {
		t.Fatalf("query %q returned %d streams, want 1", query, len(streams))
	}
	first, ok := streams[0].(map[string]any)
	if !ok {
		t.Fatalf("stream entry has unexpected shape: %T", streams[0])
	}
	gotLabels, ok := first["stream"].(map[string]any)
	if !ok {
		t.Fatalf("stream labels have unexpected shape: %T", first["stream"])
	}
	for k, want := range labels {
		if got := gotLabels[k]; got != want {
			t.Errorf("label %q = %v, want %v", k, got, want)
		}
	}
	rawValues, ok := first["values"].([]any)
	if !ok {
		t.Fatalf("stream values have unexpected shape: %T", first["values"])
	}
	values := rawValues
	if len(values) != 1 {
		t.Fatalf("stream carries %d entries, want 1", len(values))
	}
	pair, ok := values[0].([]any)
	if !ok || len(pair) < 2 {
		t.Fatalf("value entry has unexpected shape: %T", values[0])
	}
	returnedLine, ok := pair[1].(string)
	if !ok {
		t.Fatalf("entry line has unexpected shape: %T", pair[1])
	}
	var roundTripped map[string]any
	if err := json.Unmarshal([]byte(returnedLine), &roundTripped); err != nil {
		t.Fatalf("returned entry is not valid JSON: %v\nline=%s", err, returnedLine)
	}
	if got, want := roundTripped["msg"], "customer asked about delivery window"; got != want {
		t.Errorf("round-tripped msg = %v, want %v", got, want)
	}
	if got, want := roundTripped["event"], "conversation.inbound"; got != want {
		t.Errorf("round-tripped event = %v, want %v", got, want)
	}
	if got, want := roundTripped["level"], float64(30); got != want {
		t.Errorf("round-tripped level = %v, want %v", got, want)
	}
	if got, want := roundTripped["time"], float64(now.UnixMilli()); got != want {
		t.Errorf("round-tripped time = %v, want %v", got, want)
	}
	if got, want := roundTripped["conversationId"], "conv_parity_0001"; got != want {
		t.Errorf("round-tripped conversationId = %v, want %v", got, want)
	}

	negative := obscolQueryRange(t, lokiURL, `{event="conversation.outbound"}`, now)
	if len(negative) != 0 {
		t.Errorf("negative filter returned %d streams, want 0", len(negative))
	}
}

func obscolFetchMetrics(t *testing.T, stack *NodeStack) string {
	t.Helper()
	target := stack.BaseURL + "/metrics"
	var lastErr error
	for i := 0; i < 10; i++ {
		resp, err := http.Get(target)
		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(resp.Body)
			drainAndClose(resp)
			if readErr != nil {
				t.Fatalf("read /metrics body: %v", readErr)
			}
			if resp.StatusCode == http.StatusOK {
				return string(body)
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("scrape %s failed: %v\n--- logs ---\n%s", target, lastErr, stack.DumpLogs())
	return ""
}

func obscolRenderGoFamilies(t *testing.T) []string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("go /metrics handler status = %d, body=%s", rec.Code, rec.Body.String())
	}
	return obscolParseFamilies(rec.Body.String())
}

func obscolParseFamilies(exposition string) []string {
	families := map[string]struct{}{}
	for _, raw := range strings.Split(exposition, "\n") {
		line := strings.TrimRight(raw, "\r")
		switch {
		case strings.HasPrefix(line, "# TYPE "), strings.HasPrefix(line, "# HELP "):
			fields := strings.Fields(strings.TrimPrefix(line, "# "))
			if len(fields) >= 2 {
				families[fields[1]] = struct{}{}
			}
		case line == "" || strings.HasPrefix(line, "#"):
			continue
		default:
			name := line
			if i := strings.IndexAny(name, " \t"); i >= 0 {
				name = name[:i]
			}
			if i := strings.IndexByte(name, '{'); i >= 0 {
				name = name[:i]
			}
			for _, suffix := range []string{"_bucket", "_sum", "_count"} {
				name = strings.TrimSuffix(name, suffix)
			}
			if name != "" {
				families[name] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(families))
	for name := range families {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func obscolSetDiff(a, b []string) []string {
	set := map[string]struct{}{}
	for _, v := range b {
		set[v] = struct{}{}
	}
	out := []string{}
	for _, v := range a {
		if _, ok := set[v]; !ok {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func obscolIntersect(a, b []string) []string {
	set := map[string]struct{}{}
	for _, v := range b {
		set[v] = struct{}{}
	}
	out := []string{}
	for _, v := range a {
		if _, ok := set[v]; ok {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func obscolContains(list []string, needle string) bool {
	for _, v := range list {
		if v == needle {
			return true
		}
	}
	return false
}

func obscolOrEmpty(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func obscolGoldenPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	return filepath.Join(filepath.Dir(file), "testdata", "golden", "metric_parity.json")
}

func obscolStartLoki(t *testing.T, ctx context.Context) string {
	t.Helper()
	repoDir := harnessRepoDir(t)
	cfgPath := filepath.Join(repoDir, "docker", "loki", "loki-config.yml")
	if _, err := os.Stat(cfgPath); err != nil {
		t.Skipf("loki config not found in node repo: %v", err)
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "grafana/loki:3.4.2",
			ExposedPorts: []string{"3100/tcp"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      cfgPath,
				ContainerFilePath: "/etc/loki/loki-config.yml",
				FileMode:          0o644,
			}},
			Cmd:        []string{"-config.file=/etc/loki/loki-config.yml"},
			WaitingFor: wait.ForListeningPort("3100/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start loki container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("loki host: %v", err)
	}
	mapped, err := container.MappedPort(ctx, "3100/tcp")
	if err != nil {
		t.Fatalf("loki mapped port: %v", err)
	}
	baseURL := fmt.Sprintf("http://%s:%s", host, mapped.Port())

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/ready")
		if err == nil {
			drainAndClose(resp)
			if resp.StatusCode == http.StatusOK {
				return baseURL
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	logs, _ := container.Logs(context.Background())
	t.Fatalf("loki never became ready at %s\n--- logs ---\n%s", baseURL, mustReadLogs(logs))
	return ""
}

func mustReadLogs(r io.Reader) string {
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Sprintf("<read logs: %v>", err)
	}
	return string(b)
}

func obscolLokiPush(t *testing.T, baseURL string, labels map[string]string, tsNs, line string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"streams": []any{map[string]any{
			"stream": labels,
			"values": [][]string{{tsNs, line}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal push payload: %v", err)
	}
	resp, err := http.Post(baseURL+"/loki/api/v1/push", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("loki push: %v", err)
	}
	defer drainAndClose(resp)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("loki push status = %d, body=%s", resp.StatusCode, string(body))
	}
}

func obscolQueryRange(t *testing.T, baseURL, query string, around time.Time) []any {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(around.Add(-2*time.Minute).UnixNano(), 10))
	params.Set("end", strconv.FormatInt(around.Add(2*time.Minute).UnixNano(), 10))
	params.Set("limit", "100")
	params.Set("direction", "forward")

	var lastBody string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			lastBody = err.Error()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		drainAndClose(resp)
		if readErr != nil {
			t.Fatalf("read query_range body: %v", readErr)
		}
		if resp.StatusCode != http.StatusOK {
			lastBody = fmt.Sprintf("status %d body=%s", resp.StatusCode, string(body))
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var parsed struct {
			Status string `json:"status"`
			Data   struct {
				ResultType string `json:"resultType"`
				Result     []any  `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("parse query_range response: %v\nbody=%s", err, string(body))
		}
		if parsed.Status != "success" || parsed.Data.ResultType != "streams" {
			t.Fatalf("unexpected query_range response: status=%q resultType=%q", parsed.Status, parsed.Data.ResultType)
		}
		return parsed.Data.Result
	}
	t.Fatalf("query_range %q never succeeded within 30s; last response: %s", query, lastBody)
	return nil
}
