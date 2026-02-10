package pipeline

import (
	"context"
	"io"
	"log/slog"
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

	if !filter("1|postgres|/usr/bin/postgres", input) {
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

	if filter("1|nginx|/usr/sbin/nginx", input) {
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
}
