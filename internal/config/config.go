package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const (
	defaultLogLevel        = "info"
	defaultLogFormat       = "line"
	defaultCollectorTO     = 5 * time.Second
	defaultCollectorRetry  = 3 * time.Second
	defaultCollectorBatchN = 200
	defaultCollectorBatchA = 5 * time.Second
	defaultScriptTimeout   = 5 * time.Second
	defaultHTTPTimeout     = 5 * time.Second
	defaultHTTPMaxPending  = 4096
	defaultNetflowTopN     = 20
	defaultDBHost          = "127.0.0.1"
	defaultDBPort          = 8123
	defaultDBName          = "metrics"
	defaultDBUser          = "default"
	defaultDBDialTimeout   = 5 * time.Second
	defaultPprofListen     = "127.0.0.1:6060"
)

// Duration wraps time.Duration for TOML parsing.
// Params: text duration string (e.g. "5s", "1m").
// Returns: parse error on invalid duration.
type Duration struct {
	time.Duration
}

// UnmarshalText parses TOML duration values.
// Params: text is raw duration bytes from TOML.
// Returns: error when value is not a valid Go duration.
func (d *Duration) UnmarshalText(text []byte) error {
	value := strings.TrimSpace(string(text))
	if value == "" {
		d.Duration = 0
		return nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value, err)
	}

	d.Duration = parsed
	return nil
}

// Config represents the root agent configuration.
// Params: TOML document sections.
// Returns: validated runtime configuration.
type Config struct {
	Global    GlobalConfig      `toml:"global"`
	Log       LogConfig         `toml:"log"`
	Pprof     PprofConfig       `toml:"pprof"`
	Metrics   MetricsConfig     `toml:"metrics"`
	Collector []CollectorConfig `toml:"collector"`
	DB        DBConfig          `toml:"db"`
}

// PprofConfig defines optional runtime pprof HTTP endpoint.
// Params: enabled flag and listen address in host:port format.
// Returns: pprof runtime settings.
type PprofConfig struct {
	Enabled bool   `toml:"enabled"`
	Listen  string `toml:"listen"`
}

// DBConfig contains database connectivity settings used by docs/tests tooling.
// Params: nested database driver sections.
// Returns: db connection options.
type DBConfig struct {
	ClickHouse ClickHouseConfig `toml:"clickhouse"`
}

// ClickHouseConfig contains ClickHouse connection options.
// Params: host/port/db/user/password and transport settings.
// Returns: clickhouse endpoint credentials.
type ClickHouseConfig struct {
	Host        string   `toml:"host"`
	Port        uint16   `toml:"port"`
	Database    string   `toml:"database"`
	User        string   `toml:"user"`
	Password    string   `toml:"password"`
	Secure      bool     `toml:"secure"`
	DialTimeout Duration `toml:"dial_timeout"`
}

// GlobalConfig contains required shared tags.
// Params: configured global tags.
// Returns: global tag settings for all events.
type GlobalConfig struct {
	DC      string `toml:"dc"`
	Project string `toml:"project"`
	Role    string `toml:"role"`
	Host    string `toml:"host"`
}

// LogConfig contains console/file logging configuration.
// Params: console and file sink options.
// Returns: logger sink settings.
type LogConfig struct {
	Console LogSinkConfig `toml:"console"`
	File    LogSinkConfig `toml:"file"`
}

// LogSinkConfig defines one logging sink.
// Params: sink options from TOML.
// Returns: sink setup.
type LogSinkConfig struct {
	Enabled bool   `toml:"enabled"`
	Level   string `toml:"level"`
	Format  string `toml:"format"`
	Path    string `toml:"path"`
}

// MetricsConfig holds default scrape/send and percentile settings.
// Params: defaults for metric workers.
// Returns: metric defaults.
type MetricsConfig struct {
	Scrape      Duration                            `toml:"scrape"`
	Send        Duration                            `toml:"send"`
	Percentiles []int                               `toml:"percentiles"`
	CPU         []MetricWorkerConfig                `toml:"cpu"`
	RAM         []MetricWorkerConfig                `toml:"ram"`
	SWAP        []MetricWorkerConfig                `toml:"swap"`
	NET         []MetricWorkerConfig                `toml:"net"`
	DISK        []MetricWorkerConfig                `toml:"disk"`
	FS          []MetricWorkerConfig                `toml:"fs"`
	Netflow     []NetflowWorkerConfig               `toml:"netflow"`
	Process     []ProcessWorkerConfig               `toml:"process"`
	Script      map[string][]ScriptWorkerConfig     `toml:"script"`
	HTTPServer  map[string][]HTTPServerWorkerConfig `toml:"http_server"`
	HTTPClient  map[string][]HTTPClientWorkerConfig `toml:"http_client"`
}

// MetricWorkerConfig defines one metric worker instance override.
// Params: optional name, scrape/send overrides, and percentile overrides.
// Returns: one metric worker runtime config.
type MetricWorkerConfig struct {
	Name        string   `toml:"name"`
	Scrape      Duration `toml:"scrape"`
	Send        Duration `toml:"send"`
	Percentiles []int    `toml:"percentiles"`
	DropVar     []string `toml:"drop_var"`
	FilterVar   []string `toml:"filter_var"`
	DropEvent   []string `toml:"drop_event"`
}

// ProcessWorkerConfig defines one process metric worker with thresholds.
// Params: worker schedule overrides and process threshold options.
// Returns: process worker runtime config.
type ProcessWorkerConfig struct {
	Name        string   `toml:"name"`
	Scrape      Duration `toml:"scrape"`
	Send        Duration `toml:"send"`
	Percentiles []int    `toml:"percentiles"`
	CPUUtil     *float64 `toml:"cpu_util"`
	RAMUtil     *float64 `toml:"ram_util"`
	IOPS        *float64 `toml:"iops"`
	DropVar     []string `toml:"drop_var"`
	FilterVar   []string `toml:"filter_var"`
	DropEvent   []string `toml:"drop_event"`
}

// ScriptWorkerConfig defines one script metric worker with command settings.
// Params: worker schedule/filter options and script execution fields.
// Returns: script worker runtime config.
type ScriptWorkerConfig struct {
	Name        string            `toml:"name"`
	Scrape      Duration          `toml:"scrape"`
	Send        Duration          `toml:"send"`
	Percentiles []int             `toml:"percentiles"`
	DropVar     []string          `toml:"drop_var"`
	FilterVar   []string          `toml:"filter_var"`
	DropEvent   []string          `toml:"drop_event"`
	Path        string            `toml:"path"`
	Timeout     Duration          `toml:"timeout"`
	Env         map[string]string `toml:"env"`
	Format      string            `toml:"format"`
	Include     []string          `toml:"include"`
	VarMode     string            `toml:"var_mode"`
}

// HTTPServerWorkerConfig defines one HTTP server metric endpoint.
// Params: aggregation schedule/filter options and HTTP listen settings.
// Returns: http-server worker runtime config.
type HTTPServerWorkerConfig struct {
	Name        string   `toml:"name"`
	Send        Duration `toml:"send"`
	Percentiles []int    `toml:"percentiles"`
	DropVar     []string `toml:"drop_var"`
	FilterVar   []string `toml:"filter_var"`
	DropEvent   []string `toml:"drop_event"`
	Listen      string   `toml:"listen"`
	Path        string   `toml:"path"`
	MaxPending  uint64   `toml:"max_pending"`
	Format      string   `toml:"format"`
	Include     []string `toml:"include"`
	VarMode     string   `toml:"var_mode"`
}

// HTTPClientWorkerConfig defines one HTTP client metric worker.
// Params: scrape/send schedule/filter options and HTTP GET settings.
// Returns: http-client worker runtime config.
type HTTPClientWorkerConfig struct {
	Name          string   `toml:"name"`
	Scrape        Duration `toml:"scrape"`
	Send          Duration `toml:"send"`
	Percentiles   []int    `toml:"percentiles"`
	DropVar       []string `toml:"drop_var"`
	FilterVar     []string `toml:"filter_var"`
	DropEvent     []string `toml:"drop_event"`
	URL           string   `toml:"url"`
	Timeout       Duration `toml:"timeout"`
	Format        string   `toml:"format"`
	Include       []string `toml:"include"`
	KeyFromLabels []string `toml:"key_from_labels"`
	VarMode       string   `toml:"var_mode"`
}

// NetflowWorkerConfig defines one built-in netflow worker using raw packet capture.
// Params: schedule/filter options plus interface selection and top-N controls.
// Returns: netflow worker runtime config.
type NetflowWorkerConfig struct {
	Name        string   `toml:"name"`
	Scrape      Duration `toml:"scrape"`
	Send        Duration `toml:"send"`
	Percentiles []int    `toml:"percentiles"`
	DropVar     []string `toml:"drop_var"`
	FilterVar   []string `toml:"filter_var"`
	DropEvent   []string `toml:"drop_event"`
	Ifaces      []string `toml:"ifaces"`
	TopN        uint32   `toml:"top_n"`
}

// CollectorConfig defines collector target and delivery behavior.
// Params: collector endpoints, retry/batch/queue settings.
// Returns: one collector runtime config.
type CollectorConfig struct {
	Name          string               `toml:"name"`
	Addr          []string             `toml:"addr"`
	Timeout       Duration             `toml:"timeout"`
	RetryInterval Duration             `toml:"retry_interval"`
	Queue         CollectorQueueConfig `toml:"queue"`
	Batch         CollectorBatchConfig `toml:"batch"`
}

// CollectorQueueConfig defines disk queue limits.
// Params: queue controls from TOML.
// Returns: per-collector queue settings.
type CollectorQueueConfig struct {
	Enabled   bool     `toml:"enabled"`
	Dir       string   `toml:"dir"`
	MaxEvents uint64   `toml:"max_events"`
	MaxAge    Duration `toml:"max_age"`
}

// CollectorBatchConfig defines in-memory batch limits.
// Params: batch controls from TOML.
// Returns: per-collector batch settings.
type CollectorBatchConfig struct {
	MaxEvents uint64   `toml:"max_events"`
	MaxAge    Duration `toml:"max_age"`
}

// Load reads, expands, validates, and returns config from path.
// Params: path to TOML config file.
// Returns: validated config pointer or error.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	expanded := os.ExpandEnv(string(raw))

	var cfg Config
	if err := toml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("decode TOML %q: %w", path, err)
	}

	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// applyDefaults fills defaults for optional configuration fields.
// Params: receiver config pointer.
// Returns: error if defaulting needs host lookup and it fails.
func (c *Config) applyDefaults() error {
	c.Log.Console.Level = lowerOrDefault(c.Log.Console.Level, defaultLogLevel)
	c.Log.Console.Format = lowerOrDefault(c.Log.Console.Format, defaultLogFormat)
	c.Log.File.Level = lowerOrDefault(c.Log.File.Level, defaultLogLevel)
	c.Log.File.Format = lowerOrDefault(c.Log.File.Format, "json")

	if !c.Log.Console.Enabled && !c.Log.File.Enabled {
		c.Log.Console.Enabled = true
	}

	if strings.TrimSpace(c.Global.Host) == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("resolve hostname: %w", err)
		}
		c.Global.Host = host
	}

	for i := range c.Collector {
		if c.Collector[i].Timeout.Duration <= 0 {
			c.Collector[i].Timeout.Duration = defaultCollectorTO
		}
		if c.Collector[i].RetryInterval.Duration <= 0 {
			c.Collector[i].RetryInterval.Duration = defaultCollectorRetry
		}
		if c.Collector[i].Batch.MaxEvents == 0 {
			c.Collector[i].Batch.MaxEvents = defaultCollectorBatchN
		}
		if c.Collector[i].Batch.MaxAge.Duration <= 0 {
			c.Collector[i].Batch.MaxAge.Duration = defaultCollectorBatchA
		}
	}

	if strings.TrimSpace(c.DB.ClickHouse.Host) == "" {
		c.DB.ClickHouse.Host = defaultDBHost
	}
	if c.DB.ClickHouse.Port == 0 {
		c.DB.ClickHouse.Port = defaultDBPort
	}
	if strings.TrimSpace(c.DB.ClickHouse.Database) == "" {
		c.DB.ClickHouse.Database = defaultDBName
	}
	if strings.TrimSpace(c.DB.ClickHouse.User) == "" {
		c.DB.ClickHouse.User = defaultDBUser
	}
	if c.DB.ClickHouse.DialTimeout.Duration <= 0 {
		c.DB.ClickHouse.DialTimeout.Duration = defaultDBDialTimeout
	}
	if c.Pprof.Enabled && strings.TrimSpace(c.Pprof.Listen) == "" {
		c.Pprof.Listen = defaultPprofListen
	}

	for scriptName := range c.Metrics.Script {
		workers := c.Metrics.Script[scriptName]
		for idx := range workers {
			if workers[idx].Timeout.Duration == 0 {
				workers[idx].Timeout.Duration = defaultScriptTimeout
			}
			if workers[idx].Env == nil {
				workers[idx].Env = map[string]string{}
			}
			workers[idx].Format = lowerOrDefault(workers[idx].Format, "json")
			if workers[idx].VarMode == "" {
				workers[idx].VarMode = "full"
			} else {
				workers[idx].VarMode = strings.ToLower(strings.TrimSpace(workers[idx].VarMode))
			}
		}
		c.Metrics.Script[scriptName] = workers
	}

	for metricName := range c.Metrics.HTTPServer {
		workers := c.Metrics.HTTPServer[metricName]
		for idx := range workers {
			if workers[idx].MaxPending == 0 {
				workers[idx].MaxPending = defaultHTTPMaxPending
			}
			workers[idx].Format = lowerOrDefault(workers[idx].Format, "json")
			if workers[idx].VarMode == "" {
				workers[idx].VarMode = "full"
			} else {
				workers[idx].VarMode = strings.ToLower(strings.TrimSpace(workers[idx].VarMode))
			}
		}
		c.Metrics.HTTPServer[metricName] = workers
	}

	for metricName := range c.Metrics.HTTPClient {
		workers := c.Metrics.HTTPClient[metricName]
		for idx := range workers {
			if workers[idx].Timeout.Duration == 0 {
				workers[idx].Timeout.Duration = defaultHTTPTimeout
			}
			workers[idx].Format = lowerOrDefault(workers[idx].Format, "json")
			if workers[idx].VarMode == "" {
				workers[idx].VarMode = "full"
			} else {
				workers[idx].VarMode = strings.ToLower(strings.TrimSpace(workers[idx].VarMode))
			}
		}
		c.Metrics.HTTPClient[metricName] = workers
	}

	for idx := range c.Metrics.Netflow {
		if c.Metrics.Netflow[idx].TopN == 0 {
			c.Metrics.Netflow[idx].TopN = defaultNetflowTopN
		}
	}

	return nil
}

// validate checks config consistency and required fields.
// Params: receiver config pointer.
// Returns: validation error for invalid or incomplete config.
func (c *Config) validate() error {
	if strings.TrimSpace(c.Global.DC) == "" {
		return fmt.Errorf("global.dc is required")
	}
	if strings.TrimSpace(c.Global.Project) == "" {
		return fmt.Errorf("global.project is required")
	}
	if strings.TrimSpace(c.Global.Role) == "" {
		return fmt.Errorf("global.role is required")
	}
	if strings.TrimSpace(c.Global.Host) == "" {
		return fmt.Errorf("global.host resolved to empty value")
	}

	if err := validateSink("log.console", c.Log.Console, false); err != nil {
		return err
	}
	if err := validateSink("log.file", c.Log.File, true); err != nil {
		return err
	}
	if err := validateClickHouseConfig("db.clickhouse", c.DB.ClickHouse); err != nil {
		return err
	}
	if err := validatePprofConfig("pprof", c.Pprof); err != nil {
		return err
	}

	if len(c.Collector) == 0 {
		return fmt.Errorf("at least one [[collector]] section is required")
	}

	for idx, collector := range c.Collector {
		path := fmt.Sprintf("collector[%d]", idx)
		if len(collector.Addr) == 0 {
			return fmt.Errorf("%s.addr must contain at least one host:port", path)
		}

		for addrIdx, addr := range collector.Addr {
			if strings.TrimSpace(addr) == "" {
				return fmt.Errorf("%s.addr[%d] cannot be empty", path, addrIdx)
			}
		}

		if collector.Timeout.Duration <= 0 {
			return fmt.Errorf("%s.timeout must be > 0", path)
		}
		if collector.RetryInterval.Duration <= 0 {
			return fmt.Errorf("%s.retry_interval must be > 0", path)
		}

		if collector.Batch.MaxEvents == 0 && collector.Batch.MaxAge.Duration <= 0 {
			return fmt.Errorf("%s.batch requires max_events > 0 or max_age > 0", path)
		}

		if collector.Queue.Enabled {
			if strings.TrimSpace(collector.Queue.Dir) == "" {
				return fmt.Errorf("%s.queue.dir is required when queue is enabled", path)
			}
			if collector.Queue.MaxEvents == 0 && collector.Queue.MaxAge.Duration <= 0 {
				return fmt.Errorf("%s.queue requires max_events > 0 or max_age > 0", path)
			}
		}
	}

	for _, p := range c.Metrics.Percentiles {
		if p <= 0 || p > 100 {
			return fmt.Errorf("metrics.percentiles contains invalid value %d (must be 1..100)", p)
		}
	}

	if c.Metrics.Scrape.Duration < 0 {
		return fmt.Errorf("metrics.scrape cannot be negative")
	}
	if c.Metrics.Send.Duration < 0 {
		return fmt.Errorf("metrics.send cannot be negative")
	}

	if err := validateMetricWorkers("metrics.cpu", c.Metrics.CPU); err != nil {
		return err
	}
	if err := validateMetricWorkers("metrics.ram", c.Metrics.RAM); err != nil {
		return err
	}
	if err := validateMetricWorkers("metrics.swap", c.Metrics.SWAP); err != nil {
		return err
	}
	if err := validateMetricWorkers("metrics.net", c.Metrics.NET); err != nil {
		return err
	}
	if err := validateMetricWorkers("metrics.disk", c.Metrics.DISK); err != nil {
		return err
	}
	if err := validateMetricWorkers("metrics.fs", c.Metrics.FS); err != nil {
		return err
	}
	if err := validateNetflowWorkers("metrics.netflow", c.Metrics.Netflow); err != nil {
		return err
	}
	if err := validateProcessWorkers("metrics.process", c.Metrics.Process); err != nil {
		return err
	}
	if err := validateScriptWorkers("metrics.script", c.Metrics.Script); err != nil {
		return err
	}
	if err := validateHTTPServerWorkers("metrics.http_server", c.Metrics.HTTPServer); err != nil {
		return err
	}
	if err := validateHTTPClientWorkers("metrics.http_client", c.Metrics.HTTPClient); err != nil {
		return err
	}

	return nil
}

// validateSink validates one logging sink configuration.
// Params: name is sink path for errors; sink is sink config; requirePath means path required when enabled.
// Returns: validation error or nil.
func validateSink(name string, sink LogSinkConfig, requirePath bool) error {
	if sink.Enabled && requirePath && strings.TrimSpace(sink.Path) == "" {
		return fmt.Errorf("%s.path is required when sink is enabled", name)
	}

	if err := validateLogLevel(sink.Level); err != nil {
		return fmt.Errorf("%s.level: %w", name, err)
	}
	if err := validateLogFormat(sink.Format); err != nil {
		return fmt.Errorf("%s.format: %w", name, err)
	}

	return nil
}

// validateLogLevel validates known log levels.
// Params: level is lower-case level name.
// Returns: error when level is unsupported.
func validateLogLevel(level string) error {
	switch strings.TrimSpace(strings.ToLower(level)) {
	case "info", "warn", "error", "panic", "debug":
		return nil
	default:
		return fmt.Errorf("unsupported value %q", level)
	}
}

// validateLogFormat validates supported sink formats.
// Params: format is lower-case format name.
// Returns: error when format is unsupported.
func validateLogFormat(format string) error {
	switch strings.TrimSpace(strings.ToLower(format)) {
	case "line", "json":
		return nil
	default:
		return fmt.Errorf("unsupported value %q", format)
	}
}

// lowerOrDefault returns a trimmed lower-case value or default fallback.
// Params: value to normalize; fallback value when empty.
// Returns: normalized value.
func lowerOrDefault(value, fallback string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return fallback
	}
	return normalized
}

// validateMetricWorkers validates worker override sections for one metric type.
// Params: path is config path; workers are per-instance metric overrides.
// Returns: validation error for invalid values.
func validateMetricWorkers(path string, workers []MetricWorkerConfig) error {
	for idx, worker := range workers {
		workerPath := fmt.Sprintf("%s[%d]", path, idx)

		if worker.Scrape.Duration < 0 {
			return fmt.Errorf("%s.scrape cannot be negative", workerPath)
		}
		if worker.Send.Duration < 0 {
			return fmt.Errorf("%s.send cannot be negative", workerPath)
		}

		for _, p := range worker.Percentiles {
			if p <= 0 || p > 100 {
				return fmt.Errorf("%s.percentiles contains invalid value %d (must be 1..100)", workerPath, p)
			}
		}
	}

	return nil
}

// validateNetflowWorkers validates built-in netflow worker config.
// Params: path is config path; workers are netflow worker definitions.
// Returns: validation error for invalid values.
func validateNetflowWorkers(path string, workers []NetflowWorkerConfig) error {
	for idx, worker := range workers {
		workerPath := fmt.Sprintf("%s[%d]", path, idx)

		if worker.Scrape.Duration < 0 {
			return fmt.Errorf("%s.scrape cannot be negative", workerPath)
		}
		if worker.Send.Duration < 0 {
			return fmt.Errorf("%s.send cannot be negative", workerPath)
		}

		for _, p := range worker.Percentiles {
			if p <= 0 || p > 100 {
				return fmt.Errorf("%s.percentiles contains invalid value %d (must be 1..100)", workerPath, p)
			}
		}

		if len(worker.Ifaces) == 0 {
			return fmt.Errorf("%s.ifaces must contain at least one mask", workerPath)
		}
		for ifaceIdx, pattern := range worker.Ifaces {
			if strings.TrimSpace(pattern) == "" {
				return fmt.Errorf("%s.ifaces[%d] cannot be empty", workerPath, ifaceIdx)
			}
		}
		if worker.TopN == 0 {
			return fmt.Errorf("%s.top_n must be > 0", workerPath)
		}
	}

	return nil
}

// validateProcessWorkers validates process worker sections and thresholds.
// Params: path is config path; workers are process worker definitions.
// Returns: validation error for invalid values.
func validateProcessWorkers(path string, workers []ProcessWorkerConfig) error {
	for idx, worker := range workers {
		workerPath := fmt.Sprintf("%s[%d]", path, idx)

		if worker.Scrape.Duration < 0 {
			return fmt.Errorf("%s.scrape cannot be negative", workerPath)
		}
		if worker.Send.Duration < 0 {
			return fmt.Errorf("%s.send cannot be negative", workerPath)
		}

		for _, p := range worker.Percentiles {
			if p <= 0 || p > 100 {
				return fmt.Errorf("%s.percentiles contains invalid value %d (must be 1..100)", workerPath, p)
			}
		}

		if worker.CPUUtil != nil {
			if *worker.CPUUtil < 0 || *worker.CPUUtil > 100 {
				return fmt.Errorf("%s.cpu_util must be within 0..100", workerPath)
			}
		}
		if worker.RAMUtil != nil {
			if *worker.RAMUtil < 0 || *worker.RAMUtil > 100 {
				return fmt.Errorf("%s.ram_util must be within 0..100", workerPath)
			}
		}
		if worker.IOPS != nil {
			if *worker.IOPS < 0 {
				return fmt.Errorf("%s.iops cannot be negative", workerPath)
			}
		}
	}

	return nil
}

// validateScriptWorkers validates script worker sections and command fields.
// Params: path is config path; workers are script workers grouped by metric name.
// Returns: validation error for invalid values.
func validateScriptWorkers(path string, workers map[string][]ScriptWorkerConfig) error {
	for scriptName, definitions := range workers {
		scriptMetric := strings.TrimSpace(scriptName)
		if scriptMetric == "" {
			return fmt.Errorf("%s contains empty script metric name", path)
		}

		for idx, worker := range definitions {
			workerPath := fmt.Sprintf("%s.%s[%d]", path, scriptMetric, idx)

			if worker.Scrape.Duration < 0 {
				return fmt.Errorf("%s.scrape cannot be negative", workerPath)
			}
			if worker.Send.Duration < 0 {
				return fmt.Errorf("%s.send cannot be negative", workerPath)
			}
			if worker.Timeout.Duration <= 0 {
				return fmt.Errorf("%s.timeout must be > 0", workerPath)
			}
			if strings.TrimSpace(worker.Path) == "" {
				return fmt.Errorf("%s.path is required", workerPath)
			}
			if err := validateExternalMetricFormat(workerPath, worker.Format, worker.Include, worker.VarMode); err != nil {
				return err
			}

			for _, p := range worker.Percentiles {
				if p <= 0 || p > 100 {
					return fmt.Errorf("%s.percentiles contains invalid value %d (must be 1..100)", workerPath, p)
				}
			}

			for envKey := range worker.Env {
				if strings.TrimSpace(envKey) == "" {
					return fmt.Errorf("%s.env contains empty key", workerPath)
				}
			}
		}
	}

	return nil
}

// validateHTTPServerWorkers validates HTTP server worker sections and endpoint fields.
// Params: path is config path; workers are definitions grouped by metric name.
// Returns: validation error for invalid values.
func validateHTTPServerWorkers(path string, workers map[string][]HTTPServerWorkerConfig) error {
	for metricName, definitions := range workers {
		metric := strings.TrimSpace(metricName)
		if metric == "" {
			return fmt.Errorf("%s contains empty metric name", path)
		}

		for idx, worker := range definitions {
			workerPath := fmt.Sprintf("%s.%s[%d]", path, metric, idx)

			if worker.Send.Duration < 0 {
				return fmt.Errorf("%s.send cannot be negative", workerPath)
			}

			for _, p := range worker.Percentiles {
				if p <= 0 || p > 100 {
					return fmt.Errorf("%s.percentiles contains invalid value %d (must be 1..100)", workerPath, p)
				}
			}

			if strings.TrimSpace(worker.Listen) == "" {
				return fmt.Errorf("%s.listen is required", workerPath)
			}
			if _, _, err := net.SplitHostPort(worker.Listen); err != nil {
				return fmt.Errorf("%s.listen must be host:port: %w", workerPath, err)
			}

			if strings.TrimSpace(worker.Path) == "" {
				return fmt.Errorf("%s.path is required", workerPath)
			}
			if !strings.HasPrefix(worker.Path, "/") {
				return fmt.Errorf("%s.path must start with /", workerPath)
			}

			if worker.MaxPending == 0 {
				return fmt.Errorf("%s.max_pending must be > 0", workerPath)
			}
			if err := validateExternalMetricFormat(workerPath, worker.Format, worker.Include, worker.VarMode); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateHTTPClientWorkers validates HTTP client worker sections and request fields.
// Params: path is config path; workers are definitions grouped by metric name.
// Returns: validation error for invalid values.
func validateHTTPClientWorkers(path string, workers map[string][]HTTPClientWorkerConfig) error {
	for metricName, definitions := range workers {
		metric := strings.TrimSpace(metricName)
		if metric == "" {
			return fmt.Errorf("%s contains empty metric name", path)
		}

		for idx, worker := range definitions {
			workerPath := fmt.Sprintf("%s.%s[%d]", path, metric, idx)

			if worker.Scrape.Duration < 0 {
				return fmt.Errorf("%s.scrape cannot be negative", workerPath)
			}
			if worker.Send.Duration < 0 {
				return fmt.Errorf("%s.send cannot be negative", workerPath)
			}
			if worker.Timeout.Duration <= 0 {
				return fmt.Errorf("%s.timeout must be > 0", workerPath)
			}
			if strings.TrimSpace(worker.URL) == "" {
				return fmt.Errorf("%s.url is required", workerPath)
			}
			if err := validateExternalMetricFormat(workerPath, worker.Format, worker.Include, worker.VarMode); err != nil {
				return err
			}
			for labelIdx, labelName := range worker.KeyFromLabels {
				if strings.TrimSpace(labelName) == "" {
					return fmt.Errorf("%s.key_from_labels[%d] cannot be empty", workerPath, labelIdx)
				}
			}

			for _, p := range worker.Percentiles {
				if p <= 0 || p > 100 {
					return fmt.Errorf("%s.percentiles contains invalid value %d (must be 1..100)", workerPath, p)
				}
			}
		}
	}

	return nil
}

// validateExternalMetricFormat validates shared json/prometheus format options.
// Params: path is worker path for errors; format/include/varMode are source options.
// Returns: validation error when format options are invalid.
func validateExternalMetricFormat(path string, format string, include []string, varMode string) error {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		return nil
	case "prometheus":
		if len(include) == 0 {
			return fmt.Errorf("%s.include must contain at least one metric for format=prometheus", path)
		}
		for includeIdx, includeName := range include {
			if strings.TrimSpace(includeName) == "" {
				return fmt.Errorf("%s.include[%d] cannot be empty", path, includeIdx)
			}
		}
		switch strings.ToLower(strings.TrimSpace(varMode)) {
		case "full", "short":
			return nil
		default:
			return fmt.Errorf("%s.var_mode must be one of: full, short", path)
		}
	default:
		return fmt.Errorf("%s.format must be one of: json, prometheus", path)
	}
}

// validateClickHouseConfig validates clickhouse connection settings.
// Params: path is config path prefix; cfg clickhouse section.
// Returns: validation error for invalid db settings.
func validateClickHouseConfig(path string, cfg ClickHouseConfig) error {
	if strings.TrimSpace(cfg.Host) == "" {
		return fmt.Errorf("%s.host cannot be empty", path)
	}
	if cfg.Port == 0 {
		return fmt.Errorf("%s.port must be > 0", path)
	}
	if strings.TrimSpace(cfg.Database) == "" {
		return fmt.Errorf("%s.database cannot be empty", path)
	}
	if strings.TrimSpace(cfg.User) == "" {
		return fmt.Errorf("%s.user cannot be empty", path)
	}
	if cfg.DialTimeout.Duration <= 0 {
		return fmt.Errorf("%s.dial_timeout must be > 0", path)
	}
	return nil
}

// validatePprofConfig validates optional pprof endpoint settings.
// Params: path is config path prefix; cfg pprof section.
// Returns: validation error for invalid listen endpoint.
func validatePprofConfig(path string, cfg PprofConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.Listen) == "" {
		return fmt.Errorf("%s.listen cannot be empty when enabled", path)
	}
	if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		return fmt.Errorf("%s.listen must be host:port: %w", path, err)
	}
	return nil
}
