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
		worker, err := buildResolvedPullWorker(
			pullWorkerRuntimeSpec{
				metric:              "netflow",
				index:               idx,
				instancePrefix:      "netflow",
				name:                definition.Name,
				defaultScrape:       cfg.Metrics.Scrape.Duration,
				overrideScrape:      definition.Scrape.Duration,
				defaultSend:         cfg.Metrics.Send.Duration,
				overrideSend:        definition.Send.Duration,
				defaultPercentiles:  nil,
				overridePercentiles: definition.Percentiles,
				dropEvent:           definition.DropEvent,
			},
			WorkerConfig{
				Metric: "netflow",
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
