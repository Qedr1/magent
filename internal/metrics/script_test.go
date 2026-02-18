package metrics

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestScriptCollector_ScrapeSuccess verifies JSON stdout parsing for script metrics.
// Params: testing.T for assertions.
// Returns: none.
func TestScriptCollector_ScrapeSuccess(t *testing.T) {
	path := writeExecutableScript(t, `
printf '%s\n' '{"key":"total","data":{"connections":12,"util":{"value":77.7,"kind":"percent"},"errors":{"last":0}}}'
`)

	collector := NewScriptCollector("db", path, 2*time.Second, map[string]string{
		"CUSTOM_FLAG": "1",
	}, HTTPClientCollectorOptions{})

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected points count: %d", len(points))
	}

	point := points[0]
	if point.Key != "total" {
		t.Fatalf("unexpected key: %q", point.Key)
	}

	if got := point.Values["connections"]; got.Kind != KindNumber || got.Raw != 12 {
		t.Fatalf("unexpected connections value: %#v", got)
	}
	if got := point.Values["util"]; got.Kind != KindPercent || got.Raw != 77.7 {
		t.Fatalf("unexpected util value: %#v", got)
	}
	if got := point.Values["errors"]; got.Kind != KindNumber || got.Raw != 0 {
		t.Fatalf("unexpected errors value: %#v", got)
	}
}

// TestScriptCollector_ScrapeArray verifies root-array output parsing.
// Params: testing.T for assertions.
// Returns: none.
func TestScriptCollector_ScrapeArray(t *testing.T) {
	path := writeExecutableScript(t, `
printf '%s\n' '[{"key":"db1","data":{"conn":10}},{"key":"db2","data":{"util":50}}]'
`)

	collector := NewScriptCollector("db", path, 2*time.Second, nil, HTTPClientCollectorOptions{})

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("unexpected points count: %d", len(points))
	}
	if points[1].Values["util"].Kind != KindPercent {
		t.Fatalf("expected util to infer percent kind")
	}
}

// TestScriptCollector_ScrapePrometheus verifies Prometheus stdout parsing for scripts.
// Params: testing.T for assertions.
// Returns: none.
func TestScriptCollector_ScrapePrometheus(t *testing.T) {
	path := writeExecutableScript(t, `
cat <<'EOF'
# TYPE app_jobs gauge
app_jobs{node="a"} 10
app_jobs{node="b"} 20
EOF
`)

	collector := NewScriptCollector("db", path, 2*time.Second, nil, HTTPClientCollectorOptions{
		Format:  "prometheus",
		VarMode: PrometheusVarModeFull,
	})

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if len(points) != 1 || points[0].Key != "total" {
		t.Fatalf("unexpected points: %#v", points)
	}
	if got := points[0].Values["app_jobs"].Raw; got != 30.0 {
		t.Fatalf("unexpected app_jobs: %v", got)
	}
}

// TestParseScriptPoints_PrometheusPayloadLimit verifies shared payload-size guard for script prometheus mode.
// Params: testing.T for assertions.
// Returns: none.
func TestParseScriptPoints_PrometheusPayloadLimit(t *testing.T) {
	tooLarge := bytes.Repeat([]byte("x"), MaxPointsJSONBytes+1)
	_, err := parseScriptPoints(tooLarge, "prometheus", PrometheusParseConfig{VarMode: PrometheusVarModeFull})
	if err == nil {
		t.Fatalf("expected payload-size error")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestScriptCollector_ExitCode verifies failure on non-zero script exit code.
// Params: testing.T for assertions.
// Returns: none.
func TestScriptCollector_ExitCode(t *testing.T) {
	path := writeExecutableScript(t, `
echo 'broken' >&2
exit 3
`)

	collector := NewScriptCollector("db", path, 2*time.Second, nil, HTTPClientCollectorOptions{})

	_, err := collector.Scrape(context.Background())
	if err == nil {
		t.Fatalf("expected non-zero exit error")
	}
	if !strings.Contains(err.Error(), "stderr: broken") {
		t.Fatalf("expected stderr text in error, got: %v", err)
	}
}

// TestScriptCollector_Timeout verifies timeout handling.
// Params: testing.T for assertions.
// Returns: none.
func TestScriptCollector_Timeout(t *testing.T) {
	path := writeExecutableScript(t, `
sleep 1
printf '%s\n' '{"key":"total","data":{"x":1}}'
`)

	collector := NewScriptCollector("db", path, 100*time.Millisecond, nil, HTTPClientCollectorOptions{})

	_, err := collector.Scrape(context.Background())
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout text, got: %v", err)
	}
}

// TestScriptCollector_InvalidJSONContract verifies strict key/data contract.
// Params: testing.T for assertions.
// Returns: none.
func TestScriptCollector_InvalidJSONContract(t *testing.T) {
	path := writeExecutableScript(t, `
printf '%s\n' '{"data":{"x":1}}'
`)

	collector := NewScriptCollector("db", path, 2*time.Second, nil, HTTPClientCollectorOptions{})

	_, err := collector.Scrape(context.Background())
	if err == nil {
		t.Fatalf("expected contract validation error")
	}
	if !strings.Contains(err.Error(), "missing key field") {
		t.Fatalf("unexpected error text: %v", err)
	}
}

// writeExecutableScript writes shell script and marks it executable.
// Params: t test handle; body script body.
// Returns: absolute script path.
func writeExecutableScript(t *testing.T, body string) string {
	t.Helper()

	script := "#!/usr/bin/env bash\nset -euo pipefail\n" + strings.TrimSpace(body) + "\n"
	path := filepath.Join(t.TempDir(), "script.sh")

	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}
	return path
}
