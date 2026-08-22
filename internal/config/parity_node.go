package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Helpers for the T0.26 cross-stack config parity test. All names carry the
// cfgp prefix so they stay unique inside this package; nothing here may touch
// config.go or the existing tests.

// errCfgpNoRunner marks environmental failures (no node/npx
// toolchain); tests translate it into a graceful skip, mirroring how the
// harness tests skip without Docker.
var errCfgpNoRunner = errors.New("node runner unavailable")

// cfgpRepoDir resolves the sibling novoapex monorepo directory. It mirrors
// harness.NovoApexRepoDir locally on purpose: importing internal/harness here
// would drag testcontainers into this package.
func cfgpRepoDir() (string, error) {
	if v := os.Getenv("NOVOAPEX_REPO"); v != "" {
		if _, err := os.Stat(filepath.Join(v, "prisma", "schema.prisma")); err != nil {
			return "", fmt.Errorf("NOVOAPEX_REPO=%s does not contain prisma/schema.prisma", v)
		}
		return filepath.Abs(v)
	}
	for _, c := range []string{"../../../novoapex", "../../novoapex"} {
		if _, err := os.Stat(filepath.Join(c, "prisma", "schema.prisma")); err == nil {
			return filepath.Abs(c)
		}
	}
	return "", fmt.Errorf("%w: novoapex repo not found; set NOVOAPEX_REPO", errCfgpNoRunner)
}

// cfgpNodeResult is one NDJSON line emitted by the TypeScript runner: the
// outcome of validating one single-var scenario through the zod schema.
type cfgpNodeResult struct {
	Key            string  `json:"key"`
	Scenario       string  `json:"scenario"`
	Input          *string `json:"input"`
	OK             bool    `json:"ok"`
	EffectiveValue any     `json:"effectiveValue"`
	ErrorSummary   string  `json:"errorSummary"`
}

// cfgpScenario is one single-var probe sent to both stacks.
type cfgpScenario struct {
	Key      string  `json:"key"`
	Scenario string  `json:"scenario"`
	Input    *string `json:"input"`
}

// cfgpRunNodeMatrix validates every scenario against the zod schema by
// writing a small TS runner into workDir (t.TempDir()) and executing it,
// preferring node --experimental-strip-types and falling back to npx tsx.
// Results come back in input order.
func cfgpRunNodeMatrix(ctx context.Context, workDir, repoDir string, base map[string]string, scenarios []cfgpScenario) ([]cfgpNodeResult, error) {
	schemaPath := filepath.Join(repoDir, "libs", "common", "src", "config", "config.schema.ts")
	if _, err := os.Stat(schemaPath); err != nil {
		return nil, fmt.Errorf("%w: zod schema not found at %s: %v", errCfgpNoRunner, schemaPath, err)
	}

	payload, err := json.Marshal(struct {
		Base  map[string]string `json:"base"`
		Cases []cfgpScenario    `json:"cases"`
	}{Base: base, Cases: scenarios})
	if err != nil {
		return nil, fmt.Errorf("marshal matrix payload: %w", err)
	}

	const runnerTmpl = `import { readFileSync } from 'node:fs';
import { configSchema } from 'SCHEMA_URL';

const payload = JSON.parse(readFileSync(0, 'utf8'));
for (const c of payload.cases) {
  const env = { ...payload.base };
  if (c.input !== null && c.input !== undefined) env[c.key] = c.input;
  else delete env[c.key];
  const r = configSchema.safeParse(env);
  let line;
  if (r.success) {
    line = {
      key: c.key,
      scenario: c.scenario,
      input: c.input ?? null,
      ok: true,
      effectiveValue: r.data[c.key] ?? null,
      errorSummary: '',
    };
  } else {
    const summary = r.error.issues
      .map((i) => i.path.join('.') + ': ' + i.message)
      .join('; ');
    line = {
      key: c.key,
      scenario: c.scenario,
      input: c.input ?? null,
      ok: false,
      effectiveValue: null,
      errorSummary: summary,
    };
  }
  process.stdout.write(JSON.stringify(line) + '\n');
}
`
	runnerPath := filepath.Join(workDir, "cfgp_runner.mts")
	script := strings.ReplaceAll(runnerTmpl, "SCHEMA_URL", "file://"+schemaPath)
	if err := os.WriteFile(runnerPath, []byte(script), 0o600); err != nil {
		return nil, fmt.Errorf("write ts runner: %w", err)
	}

	out, stripErr := cfgpExecRunner(ctx, "", "node", []string{"--experimental-strip-types", runnerPath}, payload)
	if stripErr != nil {
		out, err = cfgpExecRunner(ctx, repoDir, "npx", []string{"--no-install", "tsx", runnerPath}, payload)
		if err != nil {
			if errors.Is(stripErr, errCfgpNoRunner) || errors.Is(err, errCfgpNoRunner) {
				return nil, fmt.Errorf("%w: node --experimental-strip-types: %v; npx tsx: %v", errCfgpNoRunner, stripErr, err)
			}
			return nil, fmt.Errorf("node --experimental-strip-types failed:\n%v\nand npx tsx fallback failed too:\n%v", stripErr, err)
		}
	}

	var results []cfgpNodeResult
	dec := json.NewDecoder(bytes.NewReader(out))
	for dec.More() {
		var res cfgpNodeResult
		if err := dec.Decode(&res); err != nil {
			return nil, fmt.Errorf("decode node runner output: %w\nraw: %s", err, truncate(out))
		}
		results = append(results, res)
	}
	if len(results) != len(scenarios) {
		return nil, fmt.Errorf("node runner returned %d results, want %d\nraw: %s", len(results), len(scenarios), truncate(out))
	}
	return results, nil
}

func cfgpExecRunner(ctx context.Context, dir, name string, args []string, stdin []byte) ([]byte, error) {
	if _, err := exec.LookPath(name); err != nil {
		return nil, fmt.Errorf("%w: %s not found: %v", errCfgpNoRunner, name, err)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Stdin = bytes.NewReader(stdin)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %s: %w\nstderr: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.Bytes(), nil
}

func truncate(b []byte) string {
	s := string(b)
	if len(s) > 2000 {
		return s[:2000]
	}
	return s
}
