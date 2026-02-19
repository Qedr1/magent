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

type builtInMetricSpec struct {
	metric      string
	definitions []config.MetricWorkerConfig
	factory     func(config.MetricWorkerConfig) metrics.Collector
}

type pullWorkerRuntime struct {
	instance      string
	scrapeEvery   time.Duration
	sendEvery     time.Duration
	percentiles   []int
	dropCondition []DropCondition
}

type pushWorkerRuntime struct {
	instance      string
	sendEvery     time.Duration
	percentiles   []int
	dropCondition []DropCondition
}

type pullWorkerRuntimeSpec struct {
	metric              string
	index               int
	instancePrefix      string
	name                string
	defaultScrape       time.Duration
	overrideScrape      time.Duration
	defaultSend         time.Duration
	overrideSend        time.Duration
	defaultPercentiles  []int
	overridePercentiles []int
	dropEvent           []string
}

// sortedMetricNames returns lexicographically sorted names from map keys.
// Params: definitions map keyed by metric name.
// Returns: sorted metric name list.
func sortedMetricNames[T any](definitions map[string][]T) []string {
	names := make([]string, 0, len(definitions))
	for name := range definitions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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

	specs := []builtInMetricSpec{
		{
			metric:      "cpu",
			definitions: cfg.Metrics.CPU,
			factory: func(_ config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewCPUCollector("cpu")
			},
		},
		{
			metric:      "ram",
			definitions: cfg.Metrics.RAM,
			factory: func(_ config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewRAMCollector("ram")
			},
		},
		{
			metric:      "swap",
			definitions: cfg.Metrics.SWAP,
			factory: func(_ config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewSWAPCollector("swap")
			},
		},
		{
			metric:      "kernel",
			definitions: cfg.Metrics.Kernel,
			factory: func(_ config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewKERNELCollector("kernel")
			},
		},
		{
			metric:      "net",
			definitions: cfg.Metrics.NET,
			factory: func(definition config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewNETCollector("net", resolveNetTCPCCTopN(definition))
			},
		},
		{
			metric:      "disk",
			definitions: cfg.Metrics.DISK,
			factory: func(_ config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewDISKCollector("disk")
			},
		},
		{
			metric:      "fs",
			definitions: cfg.Metrics.FS,
			factory: func(_ config.MetricWorkerConfig) metrics.Collector {
				return metrics.NewFSCollector("fs")
			},
		},
	}
	for _, spec := range specs {
		workerSet, buildErr := buildWorkersForMetric(
			spec.metric,
			spec.definitions,
			cfg.Metrics,
			tags,
			logger,
			sink,
			spec.factory,
		)
		if buildErr != nil {
			return nil, buildErr
		}
		runners = appendMetricWorkers(runners, workerSet)
	}

	netflowWorkers, err := buildNetflowWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	runners = appendMetricWorkers(runners, netflowWorkers)

	processWorkers, err := buildProcessWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	runners = appendMetricWorkers(runners, processWorkers)

	scriptWorkers, err := buildScriptWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	runners = appendMetricWorkers(runners, scriptWorkers)

	httpClientWorkers, err := buildHTTPClientWorkers(cfg, tags, logger, sink)
	if err != nil {
		return nil, err
	}
	runners = appendMetricWorkers(runners, httpClientWorkers)

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

// appendMetricWorkers appends metric workers into generic runner list.
// Params: runners target runner list; workers metric workers to append.
// Returns: extended runner list.
func appendMetricWorkers(runners []runner, workers []*metricWorker) []runner {
	for _, worker := range workers {
		runners = append(runners, worker)
	}
	return runners
}

// buildResolvedPullWorker resolves runtime schedules/drop rules and builds one pull worker.
// Params: runtime worker runtime inputs; cfg base worker config; sink/logger runtime deps.
// Returns: initialized metric worker or error.
func buildResolvedPullWorker(
	runtime pullWorkerRuntimeSpec,
	cfg WorkerConfig,
	sink Sink,
	logger *slog.Logger,
) (*metricWorker, error) {
	resolved, err := resolvePullWorkerRuntime(
		logger,
		runtime.metric,
		runtime.index,
		runtime.instancePrefix,
		runtime.name,
		runtime.defaultScrape,
		runtime.overrideScrape,
		runtime.defaultSend,
		runtime.overrideSend,
		runtime.defaultPercentiles,
		runtime.overridePercentiles,
		runtime.dropEvent,
	)
	if err != nil {
		return nil, err
	}

	cfg.Instance = resolved.instance
	cfg.ScrapeEvery = resolved.scrapeEvery
	cfg.SendEvery = resolved.sendEvery
	cfg.Percentiles = resolved.percentiles
	cfg.DropEvent = resolved.dropCondition

	return newMetricWorker(cfg, sink, logger)
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
		worker, err := buildResolvedPullWorker(
			pullWorkerRuntimeSpec{
				metric:              metricName,
				index:               idx,
				instancePrefix:      strings.ToLower(metricName),
				name:                definition.Name,
				defaultScrape:       defaults.Scrape.Duration,
				overrideScrape:      definition.Scrape.Duration,
				defaultSend:         defaults.Send.Duration,
				overrideSend:        definition.Send.Duration,
				defaultPercentiles:  defaults.Percentiles,
				overridePercentiles: definition.Percentiles,
				dropEvent:           definition.DropEvent,
			},
			WorkerConfig{
				Metric:    metricName,
				Collector: factory(definition),
				Tags:      tags,
				KeepKnown: true,
				DropVar:   definition.DropVar,
				FilterVar: definition.FilterVar,
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

		worker, err := buildResolvedPullWorker(
			pullWorkerRuntimeSpec{
				metric:              "process",
				index:               idx,
				instancePrefix:      "process",
				name:                definition.Name,
				defaultScrape:       defaultProcessScrapeEvery,
				overrideScrape:      definition.Scrape.Duration,
				defaultSend:         cfg.Metrics.Send.Duration,
				overrideSend:        definition.Send.Duration,
				defaultPercentiles:  cfg.Metrics.Percentiles,
				overridePercentiles: definition.Percentiles,
				dropEvent:           definition.DropEvent,
			},
			WorkerConfig{
				Metric:     "process",
				Collector:  metrics.NewPROCESSCollector("process"),
				Tags:       tags,
				EmitFilter: processEmitFilter(definition),
				KeepKnown:  false,
				DropVar:    definition.DropVar,
				FilterVar:  definition.FilterVar,
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

	scriptNames := sortedMetricNames(cfg.Metrics.Script)

	out := make([]*metricWorker, 0)
	for _, scriptName := range scriptNames {
		scriptMetric := strings.TrimSpace(scriptName)
		definitions := cfg.Metrics.Script[scriptName]

		for idx, definition := range definitions {
			worker, err := buildResolvedPullWorker(
				pullWorkerRuntimeSpec{
					metric:              scriptMetric,
					index:               idx,
					instancePrefix:      scriptMetric,
					name:                definition.Name,
					defaultScrape:       cfg.Metrics.Scrape.Duration,
					overrideScrape:      definition.Scrape.Duration,
					defaultSend:         cfg.Metrics.Send.Duration,
					overrideSend:        definition.Send.Duration,
					defaultPercentiles:  cfg.Metrics.Percentiles,
					overridePercentiles: definition.Percentiles,
					dropEvent:           definition.DropEvent,
				},
				WorkerConfig{
					Metric: scriptMetric,
					Collector: metrics.NewScriptCollector(
						scriptMetric,
						definition.Path,
						definition.Timeout.Duration,
						definition.Env,
						metrics.HTTPClientCollectorOptions{
							Format:  definition.Format,
							VarMode: definition.VarMode,
						},
					),
					Tags:      tags,
					KeepKnown: false,
					DropVar:   definition.DropVar,
					FilterVar: definition.FilterVar,
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

// resolvePullWorkerRuntime compiles reusable runtime settings for pull workers.
// Params: logger for optional alignment warning; metric/index identity; defaults/overrides; drop_event expressions.
// Returns: resolved pull worker runtime values.
func resolvePullWorkerRuntime(
	logger *slog.Logger,
	metric string,
	index int,
	instancePrefix string,
	name string,
	defaultScrape time.Duration,
	overrideScrape time.Duration,
	defaultSend time.Duration,
	overrideSend time.Duration,
	defaultPercentiles []int,
	overridePercentiles []int,
	dropEvent []string,
) (pullWorkerRuntime, error) {
	dropConditions, err := compileDropConditions(dropEvent)
	if err != nil {
		return pullWorkerRuntime{}, err
	}

	scrapeEvery := resolveWorkerInterval(defaultScrape, overrideScrape, defaultScrapeEvery)
	sendEvery := resolveWorkerInterval(defaultSend, overrideSend, defaultSendEvery)
	percentiles := normalizePercentiles(defaultPercentiles, overridePercentiles)
	instance := defaultWorkerInstance(name, instancePrefix, index)
	scrapeEvery = alignScrapeForLastOnly(logger, metric, instance, scrapeEvery, sendEvery, percentiles)

	return pullWorkerRuntime{
		instance:      instance,
		scrapeEvery:   scrapeEvery,
		sendEvery:     sendEvery,
		percentiles:   percentiles,
		dropCondition: dropConditions,
	}, nil
}

// resolvePushWorkerRuntime compiles reusable runtime settings for push workers.
// Params: metric/index identity; defaults/overrides; drop_event expressions.
// Returns: resolved push worker runtime values.
func resolvePushWorkerRuntime(
	index int,
	instancePrefix string,
	name string,
	defaultSend time.Duration,
	overrideSend time.Duration,
	defaultPercentiles []int,
	overridePercentiles []int,
	dropEvent []string,
) (pushWorkerRuntime, error) {
	dropConditions, err := compileDropConditions(dropEvent)
	if err != nil {
		return pushWorkerRuntime{}, err
	}

	return pushWorkerRuntime{
		instance:      defaultWorkerInstance(name, instancePrefix, index),
		sendEvery:     resolveWorkerInterval(defaultSend, overrideSend, defaultSendEvery),
		percentiles:   normalizePercentiles(defaultPercentiles, overridePercentiles),
		dropCondition: dropConditions,
	}, nil
}

// resolveWorkerInterval resolves interval with override/default/fallback precedence.
// Params: defaultValue from global/metric default; overrideValue from worker; fallback hardcoded fallback.
// Returns: resolved interval value.
func resolveWorkerInterval(defaultValue time.Duration, overrideValue time.Duration, fallback time.Duration) time.Duration {
	interval := defaultValue
	if interval <= 0 {
		interval = fallback
	}
	if overrideValue > 0 {
		interval = overrideValue
	}
	return interval
}

// defaultWorkerInstance resolves worker instance name using explicit name or "<prefix>-<idx>".
// Params: name optional configured worker name; prefix metric-derived default prefix; index worker index.
// Returns: non-empty worker instance string.
func defaultWorkerInstance(name string, prefix string, index int) string {
	instance := strings.TrimSpace(name)
	if instance != "" {
		return instance
	}

	base := strings.TrimSpace(prefix)
	if base == "" {
		base = "worker"
	}
	return base + "-" + strconv.Itoa(index)
}

// resolveNetTCPCCTopN resolves net tcp cc socket sample limit from worker config.
// Params: definition worker configuration for net metric.
// Returns: configured top-N value (0 disables by-cc sampling).
func resolveNetTCPCCTopN(definition config.MetricWorkerConfig) uint32 {
	if definition.TCPCCTopN == nil {
		return 0
	}
	return *definition.TCPCCTopN
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

// normalizePercentiles resolves percentile settings with explicit override semantics.
// Params: defaults from [metrics]; overrides from [[metrics.<name>]].
// Returns: sorted unique percentile list, or nil when percentiles are disabled.
func normalizePercentiles(defaults, overrides []int) []int {
	source := defaults
	if overrides != nil {
		source = overrides
	}
	if len(source) == 0 {
		return nil
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

// alignScrapeForLastOnly aligns scrape interval to send interval when percentiles are disabled.
// Params: logger for warning output; metric/instance identity; scrape/send intervals; resolved percentile list.
// Returns: scrape interval (forced to send when percentiles are disabled).
func alignScrapeForLastOnly(
	logger *slog.Logger,
	metric string,
	instance string,
	scrapeEvery time.Duration,
	sendEvery time.Duration,
	percentiles []int,
) time.Duration {
	if len(percentiles) > 0 {
		return scrapeEvery
	}

	if logger != nil {
		logger.Warn(
			"percentiles are disabled: force scrape=send",
			slog.String("metric", metric),
			slog.String("instance", instance),
			slog.Duration("scrape_before", scrapeEvery),
			slog.Duration("send", sendEvery),
			slog.Duration("scrape_after", sendEvery),
		)
	}

	return sendEvery
}
