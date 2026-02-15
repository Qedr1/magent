package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"magent/internal/config"
	"magent/internal/metrics"
)

const (
	defaultScrapeEvery        = 5 * time.Second
	defaultProcessScrapeEvery = 20 * time.Second
	defaultSendEvery          = 30 * time.Second
)

// EventTags contains mandatory global tags added to every event.
// Params: values from config.global.
// Returns: immutable tags used by workers.
type EventTags struct {
	DC      string
	Host    string
	Project string
	Role    string
}

// Engine owns metric workers lifecycle.
// Params: worker list and logger.
// Returns: pipeline runtime engine.
type Engine struct {
	runners []runner
	logger  *slog.Logger
}

type runner interface {
	run(context.Context) error
}

// NewFromConfig builds metric workers for configured metrics.
// Params: cfg validated runtime config; logger initialized logger.
// Returns: engine with active workers or error.
func NewFromConfig(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Engine, error) {
	tags := EventTags{
		DC:      cfg.Global.DC,
		Host:    cfg.Global.Host,
		Project: cfg.Global.Project,
		Role:    cfg.Global.Role,
	}

	collectorSink, err := NewCollectorSink(ctx, cfg.Collector, logger, &VectorGRPCSender{})
	if err != nil {
		return nil, fmt.Errorf("init collector sink: %w", err)
	}
	sink := NewMultiSink(
		collectorSink,
		NewLogSink(logger),
	)
	runners := make([]runner, 0)

	workerSet, err := buildWorkersForMetric(
		"cpu",
		cfg.Metrics.CPU,
		cfg.Metrics,
		tags,
		logger,
		sink,
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewCPUCollector("cpu")
		},
	)
	if err != nil {
		return nil, err
	}
	for _, worker := range workerSet {
		runners = append(runners, worker)
	}

	workerSet, err = buildWorkersForMetric(
		"ram",
		cfg.Metrics.RAM,
		cfg.Metrics,
		tags,
		logger,
		sink,
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewRAMCollector("ram")
		},
	)
	if err != nil {
		return nil, err
	}
	for _, worker := range workerSet {
		runners = append(runners, worker)
	}

	workerSet, err = buildWorkersForMetric(
		"swap",
		cfg.Metrics.SWAP,
		cfg.Metrics,
		tags,
		logger,
		sink,
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewSWAPCollector("swap")
		},
	)
	if err != nil {
		return nil, err
	}
	for _, worker := range workerSet {
		runners = append(runners, worker)
	}

	workerSet, err = buildWorkersForMetric(
		"net",
		cfg.Metrics.NET,
		cfg.Metrics,
		tags,
		logger,
		sink,
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewNETCollector("net")
		},
	)
	if err != nil {
		return nil, err
	}
	for _, worker := range workerSet {
		runners = append(runners, worker)
	}

	workerSet, err = buildWorkersForMetric(
		"disk",
		cfg.Metrics.DISK,
		cfg.Metrics,
		tags,
		logger,
		sink,
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewDISKCollector("disk")
		},
	)
	if err != nil {
		return nil, err
	}
	for _, worker := range workerSet {
		runners = append(runners, worker)
	}

	workerSet, err = buildWorkersForMetric(
		"fs",
		cfg.Metrics.FS,
		cfg.Metrics,
		tags,
		logger,
		sink,
		func(_ config.MetricWorkerConfig) metrics.Collector {
			return metrics.NewFSCollector("fs")
		},
	)
	if err != nil {
		return nil, err
	}
	for _, worker := range workerSet {
		runners = append(runners, worker)
	}

	processWorkers, err := buildProcessWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	for _, worker := range processWorkers {
		runners = append(runners, worker)
	}

	scriptWorkers, err := buildScriptWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	for _, worker := range scriptWorkers {
		runners = append(runners, worker)
	}

	httpClientWorkers, err := buildHTTPClientWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	for _, worker := range httpClientWorkers {
		runners = append(runners, worker)
	}

	httpServerRunners, err := buildHTTPServerRunners(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	runners = append(runners, httpServerRunners...)

	return &Engine{
		runners: runners,
		logger:  logger,
	}, nil
}

// Run starts all workers and waits for context cancellation.
// Params: ctx lifecycle context.
// Returns: nil on graceful stop.
func (e *Engine) Run(ctx context.Context) error {
	if len(e.runners) == 0 {
		e.logger.Warn("no metric workers configured")
		<-ctx.Done()
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(e.runners))

	for _, r := range e.runners {
		go func(activeRunner runner) {
			defer wg.Done()
			if err := activeRunner.run(ctx); err != nil {
				e.logger.Error("runner stopped with error", slog.String("error", err.Error()))
			}
		}(r)
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

// buildWorkersForMetric creates runtime workers for one metric type.
// Params: metricName logical metric; definitions from config; defaults; tags/logger/sink; collector builder.
// Returns: worker list for metric or error.
func buildWorkersForMetric(
	metricName string,
	definitions []config.MetricWorkerConfig,
	defaults config.MetricsConfig,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
	factory func(config.MetricWorkerConfig) metrics.Collector,
) ([]*metricWorker, error) {
	out := make([]*metricWorker, 0, len(definitions))
	for idx, definition := range definitions {
		dropConditions, err := compileDropConditions(definition.DropEvent)
		if err != nil {
			return nil, fmt.Errorf("build %s worker[%d]: %w", strings.ToLower(metricName), idx, err)
		}

		scrapeEvery := defaults.Scrape.Duration
		if scrapeEvery <= 0 {
			scrapeEvery = defaultScrapeEvery
		}
		if definition.Scrape.Duration > 0 {
			scrapeEvery = definition.Scrape.Duration
		}

		sendEvery := defaults.Send.Duration
		if sendEvery <= 0 {
			sendEvery = defaultSendEvery
		}
		if definition.Send.Duration > 0 {
			sendEvery = definition.Send.Duration
		}

		percentiles := normalizePercentiles(defaults.Percentiles, definition.Percentiles)
		instance := strings.TrimSpace(definition.Name)
		if instance == "" {
			instance = strings.ToLower(metricName) + "-" + strconv.Itoa(idx)
		}

		worker, err := newMetricWorker(
			WorkerConfig{
				Metric:      metricName,
				Instance:    instance,
				ScrapeEvery: scrapeEvery,
				SendEvery:   sendEvery,
				Percentiles: percentiles,
				Collector:   factory(definition),
				Tags:        tags,
				KeepKnown:   true,
				DropVar:     definition.DropVar,
				FilterVar:   definition.FilterVar,
				DropEvent:   dropConditions,
			},
			sink,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("build %s worker[%d]: %w", strings.ToLower(metricName), idx, err)
		}

		out = append(out, worker)
	}
	return out, nil
}

// buildProcessWorkers creates process workers with OR-threshold filter.
// Params: cfg runtime config; tags global event tags; logger and sink runtime deps.
// Returns: process worker list or error.
func buildProcessWorkers(
	cfg *config.Config,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
) ([]*metricWorker, error) {
	definitions := cfg.Metrics.Process
	out := make([]*metricWorker, 0, len(definitions))

	for idx, definition := range definitions {
		if !hasProcessThreshold(definition) {
			logger.Warn(
				"skip process worker: no thresholds configured",
				slog.Int("worker_index", idx),
			)
			continue
		}

		dropConditions, err := compileDropConditions(definition.DropEvent)
		if err != nil {
			return nil, fmt.Errorf("build process worker[%d]: %w", idx, err)
		}

		scrapeEvery := defaultProcessScrapeEvery
		if definition.Scrape.Duration > 0 {
			scrapeEvery = definition.Scrape.Duration
		}

		sendEvery := cfg.Metrics.Send.Duration
		if sendEvery <= 0 {
			sendEvery = defaultSendEvery
		}
		if definition.Send.Duration > 0 {
			sendEvery = definition.Send.Duration
		}

		percentiles := normalizePercentiles(cfg.Metrics.Percentiles, definition.Percentiles)
		instance := strings.TrimSpace(definition.Name)
		if instance == "" {
			instance = "process-" + strconv.Itoa(idx)
		}

		worker, err := newMetricWorker(
			WorkerConfig{
				Metric:      "process",
				Instance:    instance,
				ScrapeEvery: scrapeEvery,
				SendEvery:   sendEvery,
				Percentiles: percentiles,
				Collector:   metrics.NewPROCESSCollector("process"),
				Tags:        tags,
				EmitFilter:  processEmitFilter(definition),
				KeepKnown:   false,
				DropVar:     definition.DropVar,
				FilterVar:   definition.FilterVar,
				DropEvent:   dropConditions,
			},
			sink,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("build process worker[%d]: %w", idx, err)
		}

		out = append(out, worker)
	}

	return out, nil
}

// buildScriptWorkers creates script workers from [[metrics.script.<name>]] sections.
// Params: cfg runtime config; tags global event tags; logger and sink runtime deps.
// Returns: script worker list or error.
func buildScriptWorkers(
	cfg *config.Config,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
) ([]*metricWorker, error) {
	if len(cfg.Metrics.Script) == 0 {
		return nil, nil
	}

	scriptNames := make([]string, 0, len(cfg.Metrics.Script))
	for scriptName := range cfg.Metrics.Script {
		scriptNames = append(scriptNames, scriptName)
	}
	sort.Strings(scriptNames)

	out := make([]*metricWorker, 0)
	for _, scriptName := range scriptNames {
		scriptMetric := strings.TrimSpace(scriptName)
		definitions := cfg.Metrics.Script[scriptName]

		for idx, definition := range definitions {
			dropConditions, err := compileDropConditions(definition.DropEvent)
			if err != nil {
				return nil, fmt.Errorf("build script worker %s[%d]: %w", scriptMetric, idx, err)
			}

			scrapeEvery := cfg.Metrics.Scrape.Duration
			if scrapeEvery <= 0 {
				scrapeEvery = defaultScrapeEvery
			}
			if definition.Scrape.Duration > 0 {
				scrapeEvery = definition.Scrape.Duration
			}

			sendEvery := cfg.Metrics.Send.Duration
			if sendEvery <= 0 {
				sendEvery = defaultSendEvery
			}
			if definition.Send.Duration > 0 {
				sendEvery = definition.Send.Duration
			}

			percentiles := normalizePercentiles(cfg.Metrics.Percentiles, definition.Percentiles)

			instance := strings.TrimSpace(definition.Name)
			if instance == "" {
				instance = scriptMetric + "-" + strconv.Itoa(idx)
			}

			worker, err := newMetricWorker(
				WorkerConfig{
					Metric:      scriptMetric,
					Instance:    instance,
					ScrapeEvery: scrapeEvery,
					SendEvery:   sendEvery,
					Percentiles: percentiles,
					Collector: metrics.NewScriptCollector(
						scriptMetric,
						definition.Path,
						definition.Timeout.Duration,
						definition.Env,
					),
					Tags:      tags,
					KeepKnown: true,
					DropVar:   definition.DropVar,
					FilterVar: definition.FilterVar,
					DropEvent: dropConditions,
				},
				sink,
				logger,
			)
			if err != nil {
				return nil, fmt.Errorf("build script worker %s[%d]: %w", scriptMetric, idx, err)
			}

			out = append(out, worker)
		}
	}

	return out, nil
}

// compileDropConditions parses drop_event expressions.
// Params: expressions from worker config.
// Returns: compiled condition list or parse error.
func compileDropConditions(expressions []string) ([]DropCondition, error) {
	compiled := make([]DropCondition, 0, len(expressions))
	for idx, expression := range expressions {
		condition, err := parseDropCondition(expression)
		if err != nil {
			return nil, fmt.Errorf("invalid drop_event[%d]: %w", idx, err)
		}
		compiled = append(compiled, condition)
	}
	return compiled, nil
}

// hasProcessThreshold checks whether worker has at least one threshold.
// Params: definition process worker config.
// Returns: true if OR-threshold filter is configured.
func hasProcessThreshold(definition config.ProcessWorkerConfig) bool {
	return definition.CPUUtil != nil || definition.RAMUtil != nil || definition.IOPS != nil
}

// processEmitFilter creates OR-threshold emit predicate for process metrics.
// Params: definition process worker config with optional thresholds.
// Returns: emit filter function.
func processEmitFilter(definition config.ProcessWorkerConfig) EmitFilter {
	return func(_ string, seriesMap map[string]series) bool {
		if definition.CPUUtil != nil {
			if seriesMax(seriesMap["cpu_util"]) >= *definition.CPUUtil {
				return true
			}
		}
		if definition.RAMUtil != nil {
			if seriesMax(seriesMap["ram_util"]) >= *definition.RAMUtil {
				return true
			}
		}
		if definition.IOPS != nil {
			if seriesMax(seriesMap["iops"]) >= *definition.IOPS {
				return true
			}
		}
		return false
	}
}

// seriesMax returns maximum sample value in series.
// Params: data series.
// Returns: max value or 0 for empty input.
func seriesMax(data series) float64 {
	if len(data.values) == 0 {
		return 0
	}

	maxValue := data.values[0]
	for idx := 1; idx < len(data.values); idx++ {
		if data.values[idx] > maxValue {
			maxValue = data.values[idx]
		}
	}
	return maxValue
}

// normalizePercentiles resolves metric-specific percentile list with defaults.
// Params: defaults from [metrics]; overrides from [[metrics.<name>]].
// Returns: sorted unique percentile list.
func normalizePercentiles(defaults, overrides []int) []int {
	source := defaults
	if len(overrides) > 0 {
		source = overrides
	}
	if len(source) == 0 {
		source = []int{50, 90, 99}
	}

	seen := make(map[int]struct{}, len(source))
	out := make([]int, 0, len(source))
	for _, value := range source {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Ints(out)
	return out
}
