package pipeline

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
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

	metricNames := sortedMetricNames(cfg.Metrics.HTTPClient)

	out := make([]*metricWorker, 0)
	for _, metricName := range metricNames {
		metric := strings.TrimSpace(metricName)
		definitions := cfg.Metrics.HTTPClient[metricName]

		for idx, definition := range definitions {
			resolvedURL := expandURLTemplate(definition.URL, tags, metric, defaultWorkerInstance(definition.Name, metric, idx))
			worker, err := buildResolvedPullWorker(
				pullWorkerRuntimeSpec{
					metric:              metric,
					index:               idx,
					instancePrefix:      metric,
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
					Metric: metric,
					Collector: metrics.NewHTTPClientCollector(
						metric,
						resolvedURL,
						definition.Timeout.Duration,
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

	metricNames := sortedMetricNames(cfg.Metrics.HTTPServer)

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
			resolved, err := resolvePushWorkerRuntime(
				idx,
				metric,
				definition.Name,
				cfg.Metrics.Send.Duration,
				definition.Send.Duration,
				cfg.Metrics.Percentiles,
				definition.Percentiles,
				definition.DropEvent,
			)
			if err != nil {
				cleanupServers()
				return nil, fmt.Errorf("build http_server worker %s[%d]: %w", metric, idx, err)
			}

			worker, err := newPushWorker(
				PushWorkerConfig{
					Metric:      metric,
					Instance:    resolved.instance,
					SendEvery:   resolved.sendEvery,
					Percentiles: resolved.percentiles,
					Tags:        tags,
					KeepKnown:   false,
					MaxPending:  definition.MaxPending,
					DropVar:     definition.DropVar,
					FilterVar:   definition.FilterVar,
					DropEvent:   resolved.dropCondition,
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
				resolved.instance,
				definition.Format,
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
	varMode string,
	logger *slog.Logger,
) http.HandlerFunc {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "json"
	}
	var promParser *metrics.PrometheusParser
	if format == "prometheus" {
		promParser = metrics.NewPrometheusParser(metrics.PrometheusParseConfig{
			VarMode: varMode,
		})
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
			points, err = promParser.ParseFromReader(r.Body)
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
