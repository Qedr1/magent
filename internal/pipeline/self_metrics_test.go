package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"magent/internal/config"
	"magent/internal/metrics"
)

type failingCollector struct {
	name string
}

// Name returns metric name.
// Params: none.
// Returns: metric name string.
func (c *failingCollector) Name() string {
	return c.name
}

// Scrape always fails to exercise scrape error accounting.
// Params: ctx ignored.
// Returns: scrape error.
func (c *failingCollector) Scrape(_ context.Context) ([]metrics.Point, error) {
	return nil, errors.New("scrape failed")
}

type recordingEventSink struct {
	events []Event
}

// Consume stores emitted events for assertions.
// Params: ctx ignored; event stored payload.
// Returns: nil.
func (s *recordingEventSink) Consume(_ context.Context, event Event) error {
	s.events = append(s.events, event)
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestInternalStatsCollector_Scrape verifies per-collector and per-worker counter points.
// Params: testing.T for assertions.
// Returns: none.
func TestInternalStatsCollector_Scrape(t *testing.T) {
	deliveryWorker := &collectorWorker{name: "primary"}
	deliveryWorker.batchesSent.Store(7)
	deliveryWorker.batchesFailed.Store(2)
	deliveryWorker.overflowDropped.Store(3)
	deliveryWorker.addrSwitches.Store(1)

	sink := &CollectorSink{workers: []*collectorWorker{deliveryWorker}}

	metricW := &metricWorker{}
	metricW.cfg.Instance = "cpu-0"
	metricW.scrapeErrors.Store(4)

	collector := newInternalStatsCollector(sink, []*metricWorker{metricW})

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("unexpected points count: %d", len(points))
	}

	byKey := make(map[string]map[string]metrics.Value, len(points))
	for _, point := range points {
		byKey[point.Key] = point.Values
	}

	collectorPoint, ok := byKey["primary"]
	if !ok {
		t.Fatalf("expected point for collector key %q", "primary")
	}
	if got := collectorPoint["batches_sent"].Raw; got != 7 {
		t.Fatalf("unexpected batches_sent: %v", got)
	}
	if got := collectorPoint["batches_failed"].Raw; got != 2 {
		t.Fatalf("unexpected batches_failed: %v", got)
	}
	if got := collectorPoint["overflow_dropped"].Raw; got != 3 {
		t.Fatalf("unexpected overflow_dropped: %v", got)
	}
	if got := collectorPoint["addr_switches"].Raw; got != 1 {
		t.Fatalf("unexpected addr_switches: %v", got)
	}
	if got := collectorPoint["queue_pending"].Raw; got != 0 {
		t.Fatalf("unexpected queue_pending without queue: %v", got)
	}

	workerPoint, ok := byKey["cpu-0"]
	if !ok {
		t.Fatalf("expected point for worker key %q", "cpu-0")
	}
	if got := workerPoint["scrape_errors"].Raw; got != 4 {
		t.Fatalf("unexpected scrape_errors: %v", got)
	}
}

// TestMetricWorker_ScrapeErrorCounter verifies scrape failures increment worker counter.
// Params: testing.T for assertions.
// Returns: none.
func TestMetricWorker_ScrapeErrorCounter(t *testing.T) {
	worker, err := newMetricWorker(
		WorkerConfig{
			Metric:      "cpu",
			Instance:    "cpu-0",
			ScrapeEvery: time.Second,
			SendEvery:   time.Second,
			Collector:   &failingCollector{name: "cpu"},
			Tags:        EventTags{DC: "dc1", Host: "h1", Project: "p1", Role: "r1"},
		},
		&recordingEventSink{},
		testLogger(),
	)
	if err != nil {
		t.Fatalf("newMetricWorker: %v", err)
	}

	worker.scrapeOnce(context.Background())
	worker.scrapeOnce(context.Background())

	if got := worker.scrapeErrors.Load(); got != 2 {
		t.Fatalf("expected two scrape errors, got %d", got)
	}
}

// TestSelfMetricsWorker_EmitsCollectorSeries verifies magent_internal event emission with per-collector data.
// Params: testing.T for assertions.
// Returns: none.
func TestSelfMetricsWorker_EmitsCollectorSeries(t *testing.T) {
	deliveryWorker := &collectorWorker{name: "primary"}
	deliveryWorker.batchesSent.Store(5)

	sink := &CollectorSink{workers: []*collectorWorker{deliveryWorker}}
	eventSink := &recordingEventSink{}

	worker, err := buildSelfMetricsWorker(
		config.MetricsConfig{},
		EventTags{DC: "dc1", Host: "h1", Project: "p1", Role: "r1"},
		testLogger(),
		eventSink,
		sink,
		nil,
	)
	if err != nil {
		t.Fatalf("buildSelfMetricsWorker: %v", err)
	}

	worker.scrapeOnce(context.Background())
	worker.window.emitWindow(context.Background(), time.Second)

	if len(eventSink.events) == 0 {
		t.Fatalf("expected emitted %s event", selfMetricsName)
	}

	event := eventSink.events[0]
	if event.Metric != selfMetricsName {
		t.Fatalf("unexpected metric name: %q", event.Metric)
	}
	data, ok := event.Data["batches_sent"]
	if !ok {
		t.Fatalf("expected batches_sent var in event data: %#v", event.Data)
	}
	if got := data["last"]; got != uint64(5) {
		t.Fatalf("unexpected batches_sent last: %#v", got)
	}
	if event.Key != "primary" {
		t.Fatalf("unexpected event key: %q", event.Key)
	}
}

// TestEngine_IncludesSelfMetricsWorker verifies engine always registers the self-metrics worker.
// Params: testing.T for assertions.
// Returns: none.
func TestEngine_IncludesSelfMetricsWorker(t *testing.T) {
	cfg := &config.Config{
		Global: config.GlobalConfig{
			DC:      "dc1",
			Project: "p1",
			Role:    "r1",
			Host:    "h1",
		},
		Collector: []config.CollectorConfig{
			{
				Name:    "primary",
				Addr:    []string{"127.0.0.1:6000"},
				Timeout: config.Duration{Duration: time.Second},
				Batch: config.CollectorBatchConfig{
					MaxEvents: 1,
				},
			},
		},
	}
	cfg.Metrics.CPU = []config.MetricWorkerConfig{{Name: "cpu-main"}}

	collectorSink, err := NewCollectorSink(cfg.Collector, testLogger(), &fakeSender{})
	if err != nil {
		t.Fatalf("NewCollectorSink: %v", err)
	}
	t.Cleanup(func() {
		_ = collectorSink.Close()
	})

	engine, err := NewFromConfig(context.Background(), cfg, testLogger(), collectorSink)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}

	found := false
	for _, r := range engine.runners {
		worker, ok := r.(*metricWorker)
		if !ok {
			continue
		}
		if worker.cfg.Metric == selfMetricsName {
			found = true
			if worker.cfg.ScrapeEvery != worker.cfg.SendEvery {
				t.Fatalf("self metrics worker must have scrape=send")
			}
		}
	}
	if !found {
		t.Fatalf("engine runners do not include %s worker", selfMetricsName)
	}
}
