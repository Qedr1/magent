package pipeline

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

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
		dropConditions, err := compileDropConditions(definition.DropEvent)
		if err != nil {
			return nil, fmt.Errorf("build netflow worker[%d]: %w", idx, err)
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

		// Netflow defaults to last-only aggregation; worker-level percentiles can opt in explicitly.
		percentiles := normalizePercentiles(nil, definition.Percentiles)

		instance := strings.TrimSpace(definition.Name)
		if instance == "" {
			instance = "netflow-" + strconv.Itoa(idx)
		}

		worker, err := newMetricWorker(
			WorkerConfig{
				Metric:      "netflow",
				Instance:    instance,
				ScrapeEvery: scrapeEvery,
				SendEvery:   sendEvery,
				Percentiles: percentiles,
				Collector: metrics.NewNETFLOWCollector(
					"netflow",
					definition.Ifaces,
					definition.TopN,
				),
				Tags:      tags,
				KeepKnown: false,
				DropVar:   definition.DropVar,
				FilterVar: definition.FilterVar,
				DropEvent: dropConditions,
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
