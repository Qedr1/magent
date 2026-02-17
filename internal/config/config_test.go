package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"magent/internal/config"
)

// TestLoad_ExpandsEnvAndAppliesDefaults verifies env expansion and defaulting.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ExpandsEnvAndAppliesDefaults(t *testing.T) {
	t.Setenv("TEST_DC", "dc-main")
	t.Setenv("TEST_PROJECT", "infra")
	t.Setenv("TEST_ROLE", "db")

	path := writeConfig(t, `
[global]
dc = "${TEST_DC}"
project = "${TEST_PROJECT}"
role = "${TEST_ROLE}"
host = ""

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Global.DC != "dc-main" {
		t.Fatalf("unexpected dc: %q", cfg.Global.DC)
	}
	if cfg.Global.Host == "" {
		t.Fatalf("expected host default")
	}
	if !cfg.Log.Console.Enabled {
		t.Fatalf("expected console logging to be enabled by default")
	}
	if got := cfg.Collector[0].Timeout.Duration; got != 5*time.Second {
		t.Fatalf("unexpected default timeout: %v", got)
	}
	if got := cfg.DB.ClickHouse.Host; got != "127.0.0.1" {
		t.Fatalf("unexpected db.clickhouse.host default: %q", got)
	}
	if got := cfg.DB.ClickHouse.Port; got != 8123 {
		t.Fatalf("unexpected db.clickhouse.port default: %d", got)
	}
	if got := cfg.DB.ClickHouse.Database; got != "metrics" {
		t.Fatalf("unexpected db.clickhouse.database default: %q", got)
	}
	if got := cfg.DB.ClickHouse.User; got != "default" {
		t.Fatalf("unexpected db.clickhouse.user default: %q", got)
	}
	if got := cfg.DB.ClickHouse.DialTimeout.Duration; got != 5*time.Second {
		t.Fatalf("unexpected db.clickhouse.dial_timeout default: %v", got)
	}
}

// TestLoad_RejectsMissingRequiredTags verifies fail-fast on required tags.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsMissingRequiredTags(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = ""
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for missing global.dc")
	}
}

// TestLoad_RejectsEmptyCollectorAddr verifies collector address validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsEmptyCollectorAddr(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = [""]
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for empty collector address")
	}
}

// TestLoad_RejectsInvalidProcessThreshold verifies process threshold validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsInvalidProcessThreshold(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.process]]
cpu_util = 120
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for invalid process cpu_util threshold")
	}
}

// TestLoad_ParsesScriptSections verifies [[metrics.script.<name>]] decoding and defaults.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesScriptSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[metrics]
scrape = "5s"
send = "30s"

[[metrics.script.db]]
path = "./scripts/db.sh"
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	workers := cfg.Metrics.Script["db"]
	if len(workers) != 1 {
		t.Fatalf("unexpected script workers count: %d", len(workers))
	}
	if got := workers[0].Path; got != "./scripts/db.sh" {
		t.Fatalf("unexpected script path: %q", got)
	}
	if got := workers[0].Timeout.Duration; got != 5*time.Second {
		t.Fatalf("unexpected script default timeout: %v", got)
	}
	if workers[0].Env == nil {
		t.Fatalf("expected script env map to be initialized")
	}
}

// TestLoad_RejectsScriptWithoutPath verifies script path validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsScriptWithoutPath(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.script.db]]
timeout = "5s"
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for missing script path")
	}
}

// TestLoad_RejectsNegativeScriptTimeout verifies script timeout validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsNegativeScriptTimeout(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.script.db]]
path = "./scripts/db.sh"
timeout = "-1s"
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for negative script timeout")
	}
}

// TestLoad_ParsesScriptPrometheusSections verifies script prometheus settings decoding.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesScriptPrometheusSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.script.demo]]
path = "./scripts/db.sh"
format = "prometheus"
include = ["app_jobs"]
var_mode = "short"
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	worker := cfg.Metrics.Script["demo"][0]
	if worker.Format != "prometheus" {
		t.Fatalf("unexpected script format: %q", worker.Format)
	}
	if worker.VarMode != "short" {
		t.Fatalf("unexpected script var_mode: %q", worker.VarMode)
	}
}

// TestLoad_ParsesHTTPClientSections verifies [[metrics.http_client.<name>]] decoding and defaults.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesHTTPClientSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[metrics]
scrape = "5s"
send = "30s"

[[metrics.http_client.demo]]
url = "http://127.0.0.1:18080/metrics"
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	workers := cfg.Metrics.HTTPClient["demo"]
	if len(workers) != 1 {
		t.Fatalf("unexpected http-client workers count: %d", len(workers))
	}
	if got := workers[0].URL; got != "http://127.0.0.1:18080/metrics" {
		t.Fatalf("unexpected http-client url: %q", got)
	}
	if got := workers[0].Timeout.Duration; got != 5*time.Second {
		t.Fatalf("unexpected http-client default timeout: %v", got)
	}
	if got := workers[0].Format; got != "json" {
		t.Fatalf("unexpected http-client default format: %q", got)
	}
	if got := workers[0].VarMode; got != "full" {
		t.Fatalf("unexpected http-client default var_mode: %q", got)
	}
}

// TestLoad_ParsesHTTPClientPrometheusSections verifies prometheus mode decoding.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesHTTPClientPrometheusSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.http_client.vector]]
url = "http://127.0.0.1:19598/metrics"
format = "prometheus"
include = ["vector_build_info","vector_component_received_bytes_total"]
key_from_labels = ["host","component_id"]
var_mode = "short"
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	workers := cfg.Metrics.HTTPClient["vector"]
	if len(workers) != 1 {
		t.Fatalf("unexpected http-client workers count: %d", len(workers))
	}
	if got := workers[0].Format; got != "prometheus" {
		t.Fatalf("unexpected http-client format: %q", got)
	}
	if got := workers[0].VarMode; got != "short" {
		t.Fatalf("unexpected http-client var_mode: %q", got)
	}
	if got := workers[0].Include; len(got) != 2 || got[0] != "vector_build_info" || got[1] != "vector_component_received_bytes_total" {
		t.Fatalf("unexpected include: %#v", got)
	}
	if got := workers[0].KeyFromLabels; len(got) != 2 || got[0] != "host" || got[1] != "component_id" {
		t.Fatalf("unexpected key_from_labels: %#v", got)
	}
}

// TestLoad_RejectsHTTPClientPrometheusWithoutInclude verifies include validation for prometheus mode.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsHTTPClientPrometheusWithoutInclude(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.http_client.vector]]
url = "http://127.0.0.1:19598/metrics"
format = "prometheus"
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for missing http_client.include in prometheus mode")
	}
}

// TestLoad_RejectsHTTPClientInvalidFormat verifies format validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsHTTPClientInvalidFormat(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.http_client.demo]]
url = "http://127.0.0.1:18080/metrics"
format = "invalid"
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for invalid http_client.format")
	}
}

// TestLoad_RejectsHTTPClientInvalidVarMode verifies var_mode validation for prometheus mode.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsHTTPClientInvalidVarMode(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.http_client.vector]]
url = "http://127.0.0.1:19598/metrics"
format = "prometheus"
include = ["vector_build_info"]
var_mode = "bad"
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for invalid http_client.var_mode")
	}
}

// TestLoad_ParsesHTTPServerSections verifies [[metrics.http_server.<name>]] decoding and defaults.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesHTTPServerSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[metrics]
send = "30s"

[[metrics.http_server.demo]]
listen = "127.0.0.1:18081"
path = "/metrics"
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	workers := cfg.Metrics.HTTPServer["demo"]
	if len(workers) != 1 {
		t.Fatalf("unexpected http-server workers count: %d", len(workers))
	}
	if got := workers[0].Listen; got != "127.0.0.1:18081" {
		t.Fatalf("unexpected http-server listen: %q", got)
	}
	if got := workers[0].Path; got != "/metrics" {
		t.Fatalf("unexpected http-server path: %q", got)
	}
	if got := workers[0].MaxPending; got == 0 {
		t.Fatalf("expected http-server max_pending default")
	}
}

// TestLoad_ParsesHTTPServerPrometheusSections verifies http_server prometheus settings decoding.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesHTTPServerPrometheusSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.http_server.demo]]
listen = "127.0.0.1:18081"
path = "/metrics"
format = "prometheus"
include = ["app_jobs"]
var_mode = "short"
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	worker := cfg.Metrics.HTTPServer["demo"][0]
	if worker.Format != "prometheus" {
		t.Fatalf("unexpected http-server format: %q", worker.Format)
	}
	if worker.VarMode != "short" {
		t.Fatalf("unexpected http-server var_mode: %q", worker.VarMode)
	}
}

// TestLoad_ParsesNetflowSections verifies [[metrics.netflow]] decoding and defaults.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesNetflowSections(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[metrics]
scrape = "5s"
send = "30s"

[[metrics.netflow]]
ifaces = ["eth*", "enp*"]
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if len(cfg.Metrics.Netflow) != 1 {
		t.Fatalf("unexpected netflow workers count: %d", len(cfg.Metrics.Netflow))
	}
	if got := cfg.Metrics.Netflow[0].TopN; got != 20 {
		t.Fatalf("unexpected netflow default top_n: %d", got)
	}
	if got := cfg.Metrics.Netflow[0].Ifaces; len(got) != 2 || got[0] != "eth*" || got[1] != "enp*" {
		t.Fatalf("unexpected netflow ifaces: %#v", got)
	}
}

// TestLoad_RejectsNetflowWithoutIfaces verifies netflow iface mask validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsNetflowWithoutIfaces(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100

[[metrics.netflow]]
top_n = 10
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for missing netflow.ifaces")
	}
}

// TestLoad_ParsesClickHouseConfig verifies db.clickhouse overrides and env expansion.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesClickHouseConfig(t *testing.T) {
	t.Setenv("CH_PASSWORD", "secret-pass")

	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[db.clickhouse]
host = "10.10.10.10"
port = 9440
database = "metrics_e2e"
user = "writer"
password = "${CH_PASSWORD}"
secure = true
dial_timeout = "8s"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got := cfg.DB.ClickHouse.Host; got != "10.10.10.10" {
		t.Fatalf("unexpected db.clickhouse.host: %q", got)
	}
	if got := cfg.DB.ClickHouse.Port; got != 9440 {
		t.Fatalf("unexpected db.clickhouse.port: %d", got)
	}
	if got := cfg.DB.ClickHouse.Database; got != "metrics_e2e" {
		t.Fatalf("unexpected db.clickhouse.database: %q", got)
	}
	if got := cfg.DB.ClickHouse.User; got != "writer" {
		t.Fatalf("unexpected db.clickhouse.user: %q", got)
	}
	if got := cfg.DB.ClickHouse.Password; got != "secret-pass" {
		t.Fatalf("unexpected db.clickhouse.password: %q", got)
	}
	if !cfg.DB.ClickHouse.Secure {
		t.Fatalf("expected db.clickhouse.secure=true")
	}
	if got := cfg.DB.ClickHouse.DialTimeout.Duration; got != 8*time.Second {
		t.Fatalf("unexpected db.clickhouse.dial_timeout: %v", got)
	}
}

// TestLoad_ParsesPprofConfig verifies pprof enable/listen parsing and default listen.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_ParsesPprofConfig(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[pprof]
enabled = true

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if !cfg.Pprof.Enabled {
		t.Fatalf("expected pprof to be enabled")
	}
	if got := cfg.Pprof.Listen; got != "127.0.0.1:6060" {
		t.Fatalf("unexpected pprof.listen default: %q", got)
	}
}

// TestLoad_RejectsInvalidPprofListen verifies pprof listen validation.
// Params: testing.T for assertions.
// Returns: none.
func TestLoad_RejectsInvalidPprofListen(t *testing.T) {
	path := writeConfig(t, `
[global]
dc = "dc1"
project = "infra"
role = "db"

[pprof]
enabled = true
listen = "invalid"

[[collector]]
addr = ["127.0.0.1:6000"]

[collector.batch]
max_events = 100
`)

	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected validation error for invalid pprof.listen")
	}
}

// writeConfig creates a temp TOML config for tests.
// Params: t test handle; body TOML content.
// Returns: absolute path to temp config.
func writeConfig(t *testing.T, body string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return path
}
