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
