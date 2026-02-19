package pipeline

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"magent/internal/metrics"
)

type staticCollector struct{}

// Name returns a static collector name for tests.
// Params: none.
// Returns: metric name string.
func (c *staticCollector) Name() string {
	return "cpu"
}

// Scrape returns one stable test point.
// Params: ctx unused in test collector.
// Returns: fixed point slice.
func (c *staticCollector) Scrape(_ context.Context) ([]metrics.Point, error) {
	return []metrics.Point{
		{
			Key: "total",
			Values: map[string]metrics.Value{
				"util": {Raw: 10, Kind: metrics.KindPercent},
			},
		},
	}, nil
}

type captureSink struct {
	events []Event
}

// Consume appends event for test assertions.
// Params: ctx unused; event emitted payload.
// Returns: nil.
func (s *captureSink) Consume(_ context.Context, event Event) error {
	s.events = append(s.events, event)
	return nil
}

// TestMetricWorker_EmitWindowDTBeforeDTS verifies strict dt<dts semantics.
// Params: testing.T for assertions.
// Returns: none.
func TestMetricWorker_EmitWindowDTBeforeDTS(t *testing.T) {
	sink := &captureSink{}
	worker, err := newMetricWorker(
		WorkerConfig{
			Metric:      "cpu",
			Instance:    "cpu-test",
			ScrapeEvery: 10 * time.Second,
			SendEvery:   time.Minute,
			Percentiles: []int{50, 90},
			Collector:   &staticCollector{},
			Tags: EventTags{
				DC:      "dc1",
				Host:    "host1",
				Project: "infra",
				Role:    "db",
			},
			KeepKnown: true,
		},
		sink,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("newMetricWorker: %v", err)
	}

	worker.window.appendPoints([]metrics.Point{
		{
			Key: "total",
			Values: map[string]metrics.Value{
				"util": {Raw: 55, Kind: metrics.KindPercent},
			},
		},
	})
	worker.window.observeDT(uint64(time.Now().UnixMilli()))
	worker.window.emitWindow(context.Background(), worker.cfg.ScrapeEvery)

	if len(sink.events) != 1 {
		t.Fatalf("unexpected emitted events count: %d", len(sink.events))
	}
	event := sink.events[0]
	if event.DT/1000 >= event.DTS {
		t.Fatalf("expected dt to be strictly older than dts, got dt=%d dts=%d", event.DT, event.DTS)
	}
}

// TestMetricWorker_ScrapeKeepsWindowStartDT verifies first scrape timestamp stays for whole send window.
// Params: testing.T for assertions.
// Returns: none.
func TestMetricWorker_ScrapeKeepsWindowStartDT(t *testing.T) {
	sink := &captureSink{}
	worker, err := newMetricWorker(
		WorkerConfig{
			Metric:      "cpu",
			Instance:    "cpu-test",
			ScrapeEvery: 10 * time.Second,
			SendEvery:   time.Minute,
			Percentiles: []int{50, 90},
			Collector:   &staticCollector{},
			Tags: EventTags{
				DC:      "dc1",
				Host:    "host1",
				Project: "infra",
				Role:    "db",
			},
			KeepKnown: true,
		},
		sink,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("newMetricWorker: %v", err)
	}

	worker.scrapeOnce(context.Background())
	firstDT := worker.window.windowDT
	if firstDT == 0 {
		t.Fatalf("expected non-zero window dt after first scrape")
	}

	time.Sleep(10 * time.Millisecond)
	worker.scrapeOnce(context.Background())
	if worker.window.windowDT != firstDT {
		t.Fatalf("window dt changed within the same send window: first=%d second=%d", firstDT, worker.window.windowDT)
	}
}

// TestNewMetricWorker_AllowsEmptyPercentiles verifies last-only mode is accepted.
// Params: testing.T for assertions.
// Returns: none.
func TestNewMetricWorker_AllowsEmptyPercentiles(t *testing.T) {
	_, err := newMetricWorker(
		WorkerConfig{
			Metric:      "cpu",
			Instance:    "cpu-test",
			ScrapeEvery: 10 * time.Second,
			SendEvery:   time.Minute,
			Percentiles: nil,
			Collector:   &staticCollector{},
			Tags: EventTags{
				DC:      "dc1",
				Host:    "host1",
				Project: "infra",
				Role:    "db",
			},
			KeepKnown: true,
		},
		&captureSink{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("newMetricWorker must accept empty percentiles: %v", err)
	}
}
