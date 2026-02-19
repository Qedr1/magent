package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPClientCollector fetches JSON over HTTP GET and converts it into metric points.
// Params: metricName emitted into event.metric and HTTP request settings.
// Returns: HTTP client collector instance.
type HTTPClientCollector struct {
	metricName string
	url        string
	client     *http.Client
	format     string
	prometheus *PrometheusParser
}

// HTTPClientCollectorOptions describes optional HTTP client parsing behavior.
// Params: format selects parser mode; prometheus fields apply when format=prometheus.
// Returns: collector runtime options.
type HTTPClientCollectorOptions struct {
	Format  string
	VarMode string
}

// NewHTTPClientCollector creates an HTTP client collector.
// Params: metricName emitted into event.metric; url GET endpoint; timeout request timeout.
// Returns: configured HTTP client collector.
func NewHTTPClientCollector(metricName string, url string, timeout time.Duration, options HTTPClientCollectorOptions) *HTTPClientCollector {
	format := strings.ToLower(strings.TrimSpace(options.Format))
	if format == "" {
		format = "json"
	}

	varMode := strings.ToLower(strings.TrimSpace(options.VarMode))
	if varMode == "" {
		varMode = PrometheusVarModeFull
	}

	return &HTTPClientCollector{
		metricName: strings.TrimSpace(metricName),
		url:        strings.TrimSpace(url),
		client: &http.Client{
			Timeout: timeout,
		},
		format: format,
		prometheus: NewPrometheusParser(PrometheusParseConfig{
			VarMode: varMode,
		}),
	}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *HTTPClientCollector) Name() string {
	return c.metricName
}

// Scrape fetches JSON from configured URL and parses it into keyed metric points.
// Params: ctx for cancellation.
// Returns: parsed points or HTTP/parse error.
func (c *HTTPClientCollector) Scrape(ctx context.Context) ([]Point, error) {
	if strings.TrimSpace(c.url) == "" {
		return nil, fmt.Errorf("url is required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", c.url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		bodyText := strings.TrimSpace(string(body))
		if bodyText == "" {
			return nil, fmt.Errorf("GET %s: unexpected status %s", c.url, resp.Status)
		}
		return nil, fmt.Errorf("GET %s: unexpected status %s: %s", c.url, resp.Status, bodyText)
	}

	var points []Point
	switch c.format {
	case "prometheus":
		points, err = c.prometheus.ParseFromReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("parse response Prometheus text: %w", err)
		}
	default:
		points, err = ParsePointsJSONFromReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("parse response JSON: %w", err)
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("empty points list")
	}

	return points, nil
}
