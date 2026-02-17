package pipeline

import (
	"fmt"
	"log/slog"

	"magent/internal/config"
	"magent/internal/metrics"
)

// buildNetflowWorkers creates workers from [[metrics.netflow]] sections.
// Params: cfg runtime config; tags global event tags; logger and sink runtime deps.
// Returns: worker list or error.
func buildNetflowWorkers(
	cfg *config.Config,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
) ([]*metricWorker, error) {
	definitions := cfg.Metrics.Netflow
	if len(definitions) == 0 {
		return nil, nil
	}

	out := make([]*metricWorker, 0, len(definitions))
	for idx, definition := range definitions {
		resolved, err := resolvePullWorkerRuntime(
			logger,
			"netflow",
			idx,
			"netflow",
			definition.Name,
			cfg.Metrics.Scrape.Duration,
			definition.Scrape.Duration,
			cfg.Metrics.Send.Duration,
			definition.Send.Duration,
			nil,
			definition.Percentiles,
			definition.DropEvent,
		)
		if err != nil {
			return nil, fmt.Errorf("build netflow worker[%d]: %w", idx, err)
		}

		worker, err := newMetricWorker(
			WorkerConfig{
				Metric:      "netflow",
				Instance:    resolved.instance,
				ScrapeEvery: resolved.scrapeEvery,
				SendEvery:   resolved.sendEvery,
				Percentiles: resolved.percentiles,
				Collector: metrics.NewNETFLOWCollector(
					"netflow",
					definition.Ifaces,
					definition.TopN,
					definition.FlowIdleTimeout.Duration,
				),
				Tags:      tags,
				KeepKnown: false,
				DropVar:   definition.DropVar,
				FilterVar: definition.FilterVar,
				DropEvent: resolved.dropCondition,
			},
			sink,
			logger,
		)
		if err != nil {
			return nil, fmt.Errorf("build netflow worker[%d]: %w", idx, err)
		}

		out = append(out, worker)
	}

	return out, nil
}
