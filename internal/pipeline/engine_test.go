package pipeline

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"magent/internal/config"
	"magent/internal/metrics"
)

// TestProcessEmitFilter_ORLogic verifies OR threshold behavior for process metrics.
// Params: testing.T for assertions.
// Returns: none.
func TestProcessEmitFilter_ORLogic(t *testing.T) {
	cpu := 50.0
	ram := 80.0
	iops := 100.0

	filter := processEmitFilter(config.ProcessWorkerConfig{
		CPUUtil: &cpu,
		RAMUtil: &ram,
		IOPS:    &iops,
	})

	input := map[string]series{
		"cpu_util": {kind: metrics.KindPercent, values: []float64{10, 20, 55}},
		"ram_util": {kind: metrics.KindPercent, values: []float64{30, 40}},
		"iops":     {kind: metrics.KindNumber, values: []float64{10}},
	}

	if !filter("postgres", input) {
		t.Fatalf("expected emit due to cpu_util >= threshold")
	}
}

// TestProcessEmitFilter_NoMatch verifies non-emission when all thresholds fail.
// Params: testing.T for assertions.
// Returns: none.
func TestProcessEmitFilter_NoMatch(t *testing.T) {
	cpu := 50.0
	filter := processEmitFilter(config.ProcessWorkerConfig{
		CPUUtil: &cpu,
	})

	input := map[string]series{
		"cpu_util": {kind: metrics.KindPercent, values: []float64{10, 20, 30}},
	}

	if filter("nginx", input) {
		t.Fatalf("did not expect emit when threshold is not reached")
	}
}

type noopSink struct{}

// Consume discards events in tests.
// Params: ctx and event are unused.
// Returns: nil.
func (s noopSink) Consume(_ context.Context, _ Event) error {
	return nil
}

// TestBuildScriptWorkers verifies script worker construction from config map.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildScriptWorkers(t *testing.T) {
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape:      config.Duration{Duration: 5 * time.Second},
			Send:        config.Duration{Duration: 30 * time.Second},
			Percentiles: []int{50, 90},
			Script: map[string][]config.ScriptWorkerConfig{
				"db": []config.ScriptWorkerConfig{
					{
						Path:    "./scripts/db.sh",
						Timeout: config.Duration{Duration: 5 * time.Second},
					},
				},
			},
		},
	}

	workers, err := buildScriptWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildScriptWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if workers[0].cfg.Metric != "db" {
		t.Fatalf("unexpected script metric name: %q", workers[0].cfg.Metric)
	}
	if workers[0].cfg.Instance != "db-0" {
		t.Fatalf("unexpected script instance: %q", workers[0].cfg.Instance)
	}
	if workers[0].cfg.KeepKnown {
		t.Fatalf("expected keep_known=false for script workers")
	}
}

// TestBuildScriptWorkers_LastOnlyAlignsScrapeToSend verifies scrape=send alignment when percentiles are disabled.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildScriptWorkers_LastOnlyAlignsScrapeToSend(t *testing.T) {
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
			Script: map[string][]config.ScriptWorkerConfig{
				"db": {
					{
						Path:    "./scripts/db.sh",
						Timeout: config.Duration{Duration: 5 * time.Second},
					},
				},
			},
		},
	}

	workers, err := buildScriptWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildScriptWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 30*time.Second {
		t.Fatalf("unexpected aligned scrape interval: %v", got)
	}
}

// TestBuildProcessWorkers_DefaultScrapeEvery20s verifies process-specific default scrape interval.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildProcessWorkers_DefaultScrapeEvery20s(t *testing.T) {
	cpu := 1.0
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape:      config.Duration{Duration: 5 * time.Second},
			Send:        config.Duration{Duration: 30 * time.Second},
			Percentiles: []int{50, 90},
			Process: []config.ProcessWorkerConfig{
				{
					Name:    "process-default-scrape",
					CPUUtil: &cpu,
				},
			},
		},
	}

	workers, err := buildProcessWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildProcessWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 20*time.Second {
		t.Fatalf("unexpected process default scrape interval: %v", got)
	}
}

// TestBuildProcessWorkers_UsesExplicitScrapeOverride verifies per-worker scrape override.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildProcessWorkers_UsesExplicitScrapeOverride(t *testing.T) {
	cpu := 1.0
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape:      config.Duration{Duration: 5 * time.Second},
			Send:        config.Duration{Duration: 30 * time.Second},
			Percentiles: []int{50, 90},
			Process: []config.ProcessWorkerConfig{
				{
					Name:    "process-explicit-scrape",
					Scrape:  config.Duration{Duration: 45 * time.Second},
					CPUUtil: &cpu,
				},
			},
		},
	}

	workers, err := buildProcessWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildProcessWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 45*time.Second {
		t.Fatalf("unexpected process scrape override: %v", got)
	}
}

// TestBuildProcessWorkers_LastOnlyAlignsScrapeToSend verifies process scrape alignment when percentiles are disabled.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildProcessWorkers_LastOnlyAlignsScrapeToSend(t *testing.T) {
	cpu := 1.0
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
			Process: []config.ProcessWorkerConfig{
				{
					Name:    "process-last-only",
					CPUUtil: &cpu,
				},
			},
		},
	}

	workers, err := buildProcessWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildProcessWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 30*time.Second {
		t.Fatalf("unexpected aligned process scrape interval: %v", got)
	}
}

// TestBuildNetflowWorkers verifies netflow worker defaults and last-only percentile mode.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildNetflowWorkers(t *testing.T) {
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
			Netflow: []config.NetflowWorkerConfig{
				{
					Ifaces: []string{"lo"},
					TopN:   20,
				},
			},
		},
	}

	workers, err := buildNetflowWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildNetflowWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if workers[0].cfg.Metric != "netflow" {
		t.Fatalf("unexpected metric: %q", workers[0].cfg.Metric)
	}
	if workers[0].cfg.Instance != "netflow-0" {
		t.Fatalf("unexpected instance: %q", workers[0].cfg.Instance)
	}
	if workers[0].cfg.KeepKnown {
		t.Fatalf("expected keep_known=false for netflow")
	}
	if workers[0].cfg.Percentiles != nil {
		t.Fatalf("expected netflow default last-only mode")
	}
	if got := workers[0].cfg.ScrapeEvery; got != 30*time.Second {
		t.Fatalf("unexpected aligned netflow scrape interval: %v", got)
	}
	if _, ok := workers[0].cfg.Collector.(*metrics.NETFLOWCollector); !ok {
		t.Fatalf("expected netflow worker to use NETFLOWCollector, got %T", workers[0].cfg.Collector)
	}
}

// TestBuildNetflowWorkers_LastOnlyForceScrapeSend verifies worker-level scrape override is aligned to send in last-only mode.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildNetflowWorkers_LastOnlyForceScrapeSend(t *testing.T) {
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
			Netflow: []config.NetflowWorkerConfig{
				{
					Ifaces: []string{"lo"},
					TopN:   20,
					Scrape: config.Duration{Duration: 1 * time.Second},
					Send:   config.Duration{Duration: 4 * time.Second},
				},
			},
		},
	}

	workers, err := buildNetflowWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
	)
	if err != nil {
		t.Fatalf("buildNetflowWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 4*time.Second {
		t.Fatalf("expected aligned scrape=send=4s, got %v", got)
	}
}

// TestBuildWorkersForMetric_LastOnlyAlignsScrapeToSend verifies generic pull worker scrape alignment in last-only mode.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildWorkersForMetric_LastOnlyAlignsScrapeToSend(t *testing.T) {
	workers, err := buildWorkersForMetric(
		"cpu",
		[]config.MetricWorkerConfig{
			{Name: "cpu-last-only", Scrape: config.Duration{Duration: 7 * time.Second}},
		},
		config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
		},
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewCPUCollector("cpu")
		},
	)
	if err != nil {
		t.Fatalf("buildWorkersForMetric: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 30*time.Second {
		t.Fatalf("unexpected aligned scrape interval: %v", got)
	}
}

// TestBuildWorkersForMetric_KernelCollector verifies generic builder can wire kernel collector type.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildWorkersForMetric_KernelCollector(t *testing.T) {
	workers, err := buildWorkersForMetric(
		"kernel",
		[]config.MetricWorkerConfig{
			{Name: "kernel-main"},
		},
		config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
		},
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		noopSink{},
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewKERNELCollector("kernel")
		},
	)
	if err != nil {
		t.Fatalf("buildWorkersForMetric: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if workers[0].cfg.Metric != "kernel" {
		t.Fatalf("unexpected metric: %q", workers[0].cfg.Metric)
	}
	if _, ok := workers[0].cfg.Collector.(*metrics.KERNELCollector); !ok {
		t.Fatalf("expected KERNELCollector, got %T", workers[0].cfg.Collector)
	}
}

// TestNormalizePercentiles verifies inheritance and explicit-empty override behavior.
// Params: testing.T for assertions.
// Returns: none.
func TestNormalizePercentiles(t *testing.T) {
	testCases := []struct {
		name      string
		defaults  []int
		overrides []int
		expected  []int
	}{
		{
			name:      "inherits defaults when override is absent",
			defaults:  []int{90, 50, 90},
			overrides: nil,
			expected:  []int{50, 90},
		},
		{
			name:      "uses metric override when provided",
			defaults:  []int{50, 90},
			overrides: []int{99, 50, 99},
			expected:  []int{50, 99},
		},
		{
			name:      "explicit empty override disables percentiles",
			defaults:  []int{50, 90},
			overrides: []int{},
			expected:  nil,
		},
		{
			name:      "no defaults and no override keeps percentiles disabled",
			defaults:  nil,
			overrides: nil,
			expected:  nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := normalizePercentiles(testCase.defaults, testCase.overrides)
			if !reflect.DeepEqual(got, testCase.expected) {
				t.Fatalf("unexpected result: got=%v want=%v", got, testCase.expected)
			}
		})
	}
}

// TestResolveNetTCPCCTopN verifies pointer-based net tcp_cc_top_n extraction.
// Params: testing.T for assertions.
// Returns: none.
func TestResolveNetTCPCCTopN(t *testing.T) {
	if got := resolveNetTCPCCTopN(config.MetricWorkerConfig{}); got != 0 {
		t.Fatalf("unexpected nil-pointer fallback: %d", got)
	}

	custom := uint32(55)
	if got := resolveNetTCPCCTopN(config.MetricWorkerConfig{TCPCCTopN: &custom}); got != 55 {
		t.Fatalf("unexpected extracted value: %d", got)
	}
}
