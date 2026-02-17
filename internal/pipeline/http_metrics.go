package pipeline

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"magent/internal/config"
	"magent/internal/metrics"
)

// buildHTTPClientWorkers creates workers from [[metrics.http_client.<name>]] sections.
// Params: cfg runtime config; tags global event tags; logger and sink runtime deps.
// Returns: worker list or error.
func buildHTTPClientWorkers(
	cfg *config.Config,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
) ([]*metricWorker, error) {
	if len(cfg.Metrics.HTTPClient) == 0 {
		return nil, nil
	}

	metricNames := make([]string, 0, len(cfg.Metrics.HTTPClient))
	for name := range cfg.Metrics.HTTPClient {
		metricNames = append(metricNames, name)
	}
	sort.Strings(metricNames)

	out := make([]*metricWorker, 0)
	for _, metricName := range metricNames {
		metric := strings.TrimSpace(metricName)
		definitions := cfg.Metrics.HTTPClient[metricName]

		for idx, definition := range definitions {
			dropConditions, err := compileDropConditions(definition.DropEvent)
			if err != nil {
				return nil, fmt.Errorf("build http_client worker %s[%d]: %w", metric, idx, err)
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
				instance = metric + "-" + strconv.Itoa(idx)
			}

			resolvedURL := expandURLTemplate(definition.URL, tags, metric, instance)
			worker, err := newMetricWorker(
				WorkerConfig{
					Metric:      metric,
					Instance:    instance,
					ScrapeEvery: scrapeEvery,
					SendEvery:   sendEvery,
					Percentiles: percentiles,
					Collector: metrics.NewHTTPClientCollector(
						metric,
						resolvedURL,
						definition.Timeout.Duration,
						metrics.HTTPClientCollectorOptions{
							Format:        definition.Format,
							Include:       definition.Include,
							KeyFromLabels: definition.KeyFromLabels,
							VarMode:       definition.VarMode,
						},
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
				return nil, fmt.Errorf("build http_client worker %s[%d]: %w", metric, idx, err)
			}

			out = append(out, worker)
		}
	}

	return out, nil
}

// buildHTTPServerRunners creates push workers and HTTP servers from [[metrics.http_server.<name>]] sections.
// Params: cfg runtime config; tags global event tags; logger and sink runtime deps.
// Returns: runner list or error.
func buildHTTPServerRunners(
	cfg *config.Config,
	tags EventTags,
	logger *slog.Logger,
	sink Sink,
) ([]runner, error) {
	if len(cfg.Metrics.HTTPServer) == 0 {
		return nil, nil
	}

	metricNames := make([]string, 0, len(cfg.Metrics.HTTPServer))
	for name := range cfg.Metrics.HTTPServer {
		metricNames = append(metricNames, name)
	}
	sort.Strings(metricNames)

	type serverGroup struct {
		mux   *http.ServeMux
		paths map[string]struct{}
		srv   *httpIngestServer
	}

	groups := make(map[string]*serverGroup)
	out := make([]runner, 0)
	cleanupServers := func() {
		for _, group := range groups {
			if group == nil || group.srv == nil || group.srv.ln == nil {
				continue
			}
			_ = group.srv.ln.Close()
		}
	}

	for _, metricName := range metricNames {
		metric := strings.TrimSpace(metricName)
		definitions := cfg.Metrics.HTTPServer[metricName]

		for idx, definition := range definitions {
			dropConditions, err := compileDropConditions(definition.DropEvent)
			if err != nil {
				cleanupServers()
				return nil, fmt.Errorf("build http_server worker %s[%d]: %w", metric, idx, err)
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
				instance = metric + "-" + strconv.Itoa(idx)
			}

			worker, err := newPushWorker(
				PushWorkerConfig{
					Metric:      metric,
					Instance:    instance,
					SendEvery:   sendEvery,
					Percentiles: percentiles,
					Tags:        tags,
					KeepKnown:   false,
					MaxPending:  definition.MaxPending,
					DropVar:     definition.DropVar,
					FilterVar:   definition.FilterVar,
					DropEvent:   dropConditions,
				},
				sink,
				logger,
			)
			if err != nil {
				cleanupServers()
				return nil, fmt.Errorf("build http_server worker %s[%d]: %w", metric, idx, err)
			}

			out = append(out, worker)

			listen := strings.TrimSpace(definition.Listen)
			path := strings.TrimSpace(definition.Path)

			group := groups[listen]
			if group == nil {
				mux := http.NewServeMux()
				srv, err := newHTTPIngestServer(listen, mux, logger)
				if err != nil {
					cleanupServers()
					return nil, err
				}
				group = &serverGroup{
					mux:   mux,
					paths: make(map[string]struct{}),
					srv:   srv,
				}
				groups[listen] = group
				out = append(out, srv)
			}

			if _, exists := group.paths[path]; exists {
				cleanupServers()
				return nil, fmt.Errorf("duplicate http_server route: listen=%q path=%q", listen, path)
			}
			group.paths[path] = struct{}{}

			group.mux.HandleFunc(path, makeHTTPIngestHandler(
				worker,
				metric,
				instance,
				definition.Format,
				definition.Include,
				definition.VarMode,
				logger,
			))
		}
	}

	return out, nil
}

// makeHTTPIngestHandler builds HTTP handler that parses points and enqueues them into a push worker.
// Params: worker target push worker; metric/instance for logs; logger root logger.
// Returns: HTTP handler function.
func makeHTTPIngestHandler(
	worker *pushWorker,
	metric string,
	instance string,
	format string,
	include []string,
	varMode string,
	logger *slog.Logger,
) http.HandlerFunc {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "json"
	}
	promCfg := metrics.PrometheusParseConfig{
		Include: include,
		VarMode: varMode,
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var (
			points []metrics.Point
			err    error
		)
		switch format {
		case "prometheus":
			points, err = metrics.ParsePointsPrometheusFromReader(r.Body, promCfg)
		default:
			points, err = metrics.ParsePointsJSONFromReader(r.Body)
		}
		if err != nil {
			logger.Warn("http ingest parse failed", slog.String("metric", metric), slog.String("instance", instance), slog.String("error", err.Error()))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(points) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if ok := worker.ingest(time.Now(), points); !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// expandURLTemplate replaces well-known placeholders in URL template.
// Params: template URL string; tags global tags; metric/instance names.
// Returns: resolved URL string.
func expandURLTemplate(template string, tags EventTags, metric string, instance string) string {
	replacer := strings.NewReplacer(
		"{dc}", url.PathEscape(tags.DC),
		"{host}", url.PathEscape(tags.Host),
		"{project}", url.PathEscape(tags.Project),
		"{role}", url.PathEscape(tags.Role),
		"{metric}", url.PathEscape(metric),
		"{instance}", url.PathEscape(instance),
	)
	return replacer.Replace(template)
}
