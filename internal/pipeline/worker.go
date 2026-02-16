package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"magent/internal/metrics"
)

// WorkerConfig defines one metric worker runtime.
// Params: metric identity, schedule, collector, and aggregation options.
// Returns: worker runtime configuration.
type WorkerConfig struct {
	Metric      string
	Instance    string
	ScrapeEvery time.Duration
	SendEvery   time.Duration
	Percentiles []int
	Collector   metrics.Collector
	Tags        EventTags
	EmitFilter  EmitFilter
	KeepKnown   bool
	DropVar     []string
	FilterVar   []string
	DropEvent   []DropCondition
}

// EmitFilter decides whether a keyed window should be emitted.
// Params: key is event key; seriesMap contains window samples per variable.
// Returns: true if event for key must be emitted.
type EmitFilter func(key string, seriesMap map[string]series) bool

type metricWorker struct {
	cfg    WorkerConfig
	window *window
}

// run executes scrape/send loops until context cancellation.
// Params: ctx controls lifecycle.
// Returns: nil on graceful stop.
func (w *metricWorker) run(ctx context.Context) error {
	scrapeTicker := time.NewTicker(w.cfg.ScrapeEvery)
	sendTicker := time.NewTicker(w.cfg.SendEvery)
	defer scrapeTicker.Stop()
	defer sendTicker.Stop()

	// Warm-up scrape to avoid empty initial windows.
	w.scrapeOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-scrapeTicker.C:
			w.scrapeOnce(ctx)
		case <-sendTicker.C:
			w.window.emitWindow(ctx, w.cfg.ScrapeEvery)
		}
	}
}

// scrapeOnce collects one point batch and appends samples into worker buffer.
// Params: ctx for scrape cancellation.
// Returns: none.
func (w *metricWorker) scrapeOnce(ctx context.Context) {
	scrapeAt := uint64(time.Now().UnixMilli())

	points, err := w.cfg.Collector.Scrape(ctx)
	if err != nil {
		w.window.cfg.Logger.Error(
			"scrape failed",
			slog.String("metric", w.cfg.Metric),
			slog.String("instance", w.cfg.Instance),
			slog.String("error", err.Error()),
		)
		return
	}

	if !w.window.appendPoints(points) {
		return
	}
	w.window.observeDT(scrapeAt)
}

// newMetricWorker builds a worker from runtime config.
// Params: cfg runtime settings; sink event consumer; logger root logger.
// Returns: worker instance or error.
func newMetricWorker(cfg WorkerConfig, sink Sink, logger *slog.Logger) (*metricWorker, error) {
	if sink == nil {
		return nil, fmt.Errorf("sink is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if cfg.Collector == nil {
		return nil, fmt.Errorf("collector is required")
	}
	if cfg.ScrapeEvery <= 0 {
		return nil, fmt.Errorf("scrape interval must be > 0")
	}
	if cfg.SendEvery <= 0 {
		return nil, fmt.Errorf("send interval must be > 0")
	}
	for idx, expression := range cfg.DropEvent {
		if strings.TrimSpace(expression.Raw) == "" {
			return nil, fmt.Errorf("drop_event[%d] cannot be empty", idx)
		}
	}

	instance := strings.TrimSpace(cfg.Instance)
	if instance == "" {
		instance = "default"
	}
	cfg.Instance = instance

	return &metricWorker{
		cfg: cfg,
		window: newWindow(windowConfig{
			Metric:      cfg.Metric,
			Instance:    cfg.Instance,
			Percentiles: cfg.Percentiles,
			Tags:        cfg.Tags,
			Sink:        sink,
			Logger:      logger,
			EmitFilter:  cfg.EmitFilter,
			KeepKnown:   cfg.KeepKnown,
			DropVar:     cfg.DropVar,
			FilterVar:   cfg.FilterVar,
			DropEvent:   cfg.DropEvent,
		}),
	}, nil
}

// isVariableAllowed applies filter_var/drop_var masks to variable name.
// Params: name is variable name; filterVar keeps only matches; dropVar removes matches.
// Returns: true when variable should remain in event data.
func isVariableAllowed(name string, filterVar, dropVar []string) bool {
	if len(filterVar) > 0 {
		match := false
		for _, pattern := range filterVar {
			if wildcardMatch(pattern, name) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}

	for _, pattern := range dropVar {
		if wildcardMatch(pattern, name) {
			return false
		}
	}

	return true
}
