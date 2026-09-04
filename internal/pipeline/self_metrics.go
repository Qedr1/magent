package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"magent/internal/config"
	"magent/internal/metrics"
)

const selfMetricsName = "magent_internal"

// internalStatsCollector exposes agent scrape/delivery counters as a pull metric.
// Params: point sources per collector worker and metric worker.
// Returns: collector implementation for the self-metrics worker.
type internalStatsCollector struct {
	name    string
	sources []func() []metrics.Point
}

// Name returns metric name.
// Params: none.
// Returns: metric name string.
func (c *internalStatsCollector) Name() string {
	return c.name
}

// Scrape snapshots current internal counters into keyed points.
// Params: ctx scrape context (unused; counters are local).
// Returns: point list, never an error.
func (c *internalStatsCollector) Scrape(_ context.Context) ([]metrics.Point, error) {
	points := make([]metrics.Point, 0, len(c.sources))
	for _, source := range c.sources {
		points = append(points, source()...)
	}
	return points, nil
}

// newInternalStatsCollector builds pull collector over agent internal counters.
// Params: collectorSink delivery sink with per-collector counters; workers pull metric workers.
// Returns: collector emitting per-collector and per-worker counter points.
func newInternalStatsCollector(collectorSink *CollectorSink, workers []*metricWorker) metrics.Collector {
	sources := make([]func() []metrics.Point, 0, len(workers)+1)

	if collectorSink != nil {
		for _, worker := range collectorSink.workers {
			tracked := worker
			sources = append(sources, func() []metrics.Point {
				var pending uint64
				if tracked.queue != nil {
					pending = tracked.queue.Pending()
				}
				return []metrics.Point{{
					Key: tracked.name,
					Values: map[string]metrics.Value{
						"queue_pending":    {Raw: float64(pending), Kind: metrics.KindNumber},
						"batches_sent":     {Raw: float64(tracked.batchesSent.Load()), Kind: metrics.KindNumber},
						"batches_failed":   {Raw: float64(tracked.batchesFailed.Load()), Kind: metrics.KindNumber},
						"overflow_dropped": {Raw: float64(tracked.overflowDropped.Load()), Kind: metrics.KindNumber},
						"addr_switches":    {Raw: float64(tracked.addrSwitches.Load()), Kind: metrics.KindNumber},
					},
				}}
			})
		}
	}

	for _, worker := range workers {
		tracked := worker
		sources = append(sources, func() []metrics.Point {
			return []metrics.Point{{
				Key: tracked.cfg.Instance,
				Values: map[string]metrics.Value{
					"scrape_errors": {Raw: float64(tracked.scrapeErrors.Load()), Kind: metrics.KindNumber},
				},
			}}
		})
	}

	return &internalStatsCollector{name: selfMetricsName, sources: sources}
}

// buildSelfMetricsWorker creates the always-on self-metrics worker.
// Params: defaults global metric defaults; tags global event tags; logger/sink runtime deps;
// collectorSink delivery sink; workers pull metric workers to observe.
// Returns: metric worker emitting magent_internal events or error.
func buildSelfMetricsWorker(
	defaults config.MetricsConfig,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
	collectorSink *CollectorSink,
	workers []*metricWorker,
) (*metricWorker, error) {
	sendEvery := defaults.Send.Duration
	if sendEvery <= 0 {
		sendEvery = defaultSendEvery
	}

	worker, err := newMetricWorker(
		WorkerConfig{
			Metric:      selfMetricsName,
			Instance:    selfMetricsName,
			ScrapeEvery: sendEvery,
			SendEvery:   sendEvery,
			Percentiles: nil,
			Collector:   newInternalStatsCollector(collectorSink, workers),
			Tags:        tags,
			KeepKnown:   true,
		},
		sink,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("build %s worker: %w", selfMetricsName, err)
	}
	return worker, nil
}
