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
	cfg      WorkerConfig
	sink     Sink
	logger   *slog.Logger
	buffer   map[string]map[string]*series
	known    map[string]map[string]metrics.ValueKind
	lastDTMs uint64
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
			w.emitWindow(ctx)
		}
	}
}

// scrapeOnce collects one point batch and appends samples into worker buffer.
// Params: ctx for scrape cancellation.
// Returns: none.
func (w *metricWorker) scrapeOnce(ctx context.Context) {
	points, err := w.cfg.Collector.Scrape(ctx)
	if err != nil {
		w.logger.Error(
			"scrape failed",
			slog.String("metric", w.cfg.Metric),
			slog.String("instance", w.cfg.Instance),
			slog.String("error", err.Error()),
		)
		return
	}

	w.lastDTMs = uint64(time.Now().UnixMilli())
	w.appendPoints(points)
}

// emitWindow aggregates current buffer and sends events through sink.
// Params: ctx for sink operations.
// Returns: none.
func (w *metricWorker) emitWindow(ctx context.Context) {
	if len(w.buffer) == 0 && len(w.known) == 0 {
		return
	}

	dts := uint64(time.Now().Unix())
	dt := w.lastDTMs
	if dt == 0 {
		dt = uint64(time.Now().UnixMilli())
	}

	for key, vars := range w.known {
		seriesMap := w.resolveSeries(key, vars)
		if w.cfg.EmitFilter != nil && !w.cfg.EmitFilter(key, seriesMap) {
			continue
		}

		data := make(map[string]map[string]any, len(seriesMap))
		for varName, sampleSeries := range seriesMap {
			data[varName] = aggregateSeries(sampleSeries, w.cfg.Percentiles)
		}

		if shouldDropEvent(
			w.cfg.DropEvent,
			EventEvalContext{
				Metric: w.cfg.Metric,
				Key:    key,
				Data:   data,
			},
		) {
			continue
		}

		event := Event{
			DT:      dt,
			DTS:     dts,
			Metric:  w.cfg.Metric,
			DC:      w.cfg.Tags.DC,
			Host:    w.cfg.Tags.Host,
			Project: w.cfg.Tags.Project,
			Role:    w.cfg.Tags.Role,
			Key:     key,
			Data:    data,
		}

		if err := w.sink.Consume(ctx, event); err != nil {
			w.logger.Error(
				"emit failed",
				slog.String("metric", w.cfg.Metric),
				slog.String("instance", w.cfg.Instance),
				slog.String("key", key),
				slog.String("error", err.Error()),
			)
		}
	}

	w.buffer = make(map[string]map[string]*series)
	if !w.cfg.KeepKnown {
		w.known = make(map[string]map[string]metrics.ValueKind)
	}
}

// appendPoints appends one scrape result into in-memory window buffers.
// Params: points keyed values from collector.
// Returns: none.
func (w *metricWorker) appendPoints(points []metrics.Point) {
	for _, point := range points {
		key := strings.TrimSpace(point.Key)
		if key == "" {
			key = "total"
		}

		if _, ok := w.buffer[key]; !ok {
			w.buffer[key] = make(map[string]*series)
		}
		if _, ok := w.known[key]; !ok {
			w.known[key] = make(map[string]metrics.ValueKind)
		}

		for varName, value := range point.Values {
			valueName := strings.TrimSpace(varName)
			if valueName == "" {
				continue
			}
			if !isVariableAllowed(valueName, w.cfg.FilterVar, w.cfg.DropVar) {
				continue
			}

			seriesBuffer, ok := w.buffer[key][valueName]
			if !ok {
				seriesBuffer = &series{kind: value.Kind}
				w.buffer[key][valueName] = seriesBuffer
			}
			seriesBuffer.kind = value.Kind
			seriesBuffer.values = append(seriesBuffer.values, value.Raw)
			w.known[key][valueName] = value.Kind
		}
	}
}

// resolveSeries composes effective series map for one key.
// Params: key key value; vars known variable kinds for key.
// Returns: per-variable series with samples for current window.
func (w *metricWorker) resolveSeries(key string, vars map[string]metrics.ValueKind) map[string]series {
	out := make(map[string]series, len(vars))
	for varName, kind := range vars {
		s := series{kind: kind}
		if keySamples, ok := w.buffer[key]; ok {
			if buffered, ok := keySamples[varName]; ok {
				s = *buffered
			}
		}
		out[varName] = s
	}
	return out
}

// newMetricWorker builds a worker from runtime config.
// Params: cfg runtime settings; sink event consumer; logger root logger.
// Returns: worker instance or error.
func newMetricWorker(cfg WorkerConfig, sink Sink, logger *slog.Logger) (*metricWorker, error) {
	if cfg.Collector == nil {
		return nil, fmt.Errorf("collector is required")
	}
	if cfg.ScrapeEvery <= 0 {
		return nil, fmt.Errorf("scrape interval must be > 0")
	}
	if cfg.SendEvery <= 0 {
		return nil, fmt.Errorf("send interval must be > 0")
	}
	if len(cfg.Percentiles) == 0 {
		return nil, fmt.Errorf("percentiles cannot be empty")
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
		cfg:    cfg,
		sink:   sink,
		logger: logger,
		buffer: make(map[string]map[string]*series),
		known:  make(map[string]map[string]metrics.ValueKind),
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
