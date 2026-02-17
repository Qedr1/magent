package pipeline

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"magent/internal/config"
)

type httpNoopSink struct{}

// Consume accepts all events.
// Params: ctx/event ignored.
// Returns: nil.
func (httpNoopSink) Consume(_ context.Context, _ Event) error {
	return nil
}

// TestBuildHTTPServerRunners_ReleasesListenerOnError verifies listeners are closed when route build fails.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildHTTPServerRunners_ReleasesListenerOnError(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve test address: %v", err)
	}
	listenAddr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatalf("release reserved test address: %v", err)
	}

	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			HTTPServer: map[string][]config.HTTPServerWorkerConfig{
				"demo": {
					{Listen: listenAddr, Path: "/ingest", MaxPending: 16},
					{Listen: listenAddr, Path: "/ingest", MaxPending: 16},
				},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err = buildHTTPServerRunners(cfg, EventTags{}, logger, httpNoopSink{})
	if err == nil {
		t.Fatalf("expected duplicate route error")
	}

	ln, bindErr := net.Listen("tcp", listenAddr)
	if bindErr != nil {
		t.Fatalf("listener leak detected on %s: %v", listenAddr, bindErr)
	}
	_ = ln.Close()
}

// TestBuildHTTPClientWorkers_LastOnlyAlignsScrapeToSend verifies scrape alignment for HTTP client workers without percentiles.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildHTTPClientWorkers_LastOnlyAlignsScrapeToSend(t *testing.T) {
	cfg := &config.Config{
		Metrics: config.MetricsConfig{
			Scrape: config.Duration{Duration: 5 * time.Second},
			Send:   config.Duration{Duration: 30 * time.Second},
			HTTPClient: map[string][]config.HTTPClientWorkerConfig{
				"demo": {
					{
						URL:     "http://127.0.0.1:18080/metrics",
						Timeout: config.Duration{Duration: 2 * time.Second},
					},
				},
			},
		},
	}

	workers, err := buildHTTPClientWorkers(
		cfg,
		EventTags{DC: "dc1", Host: "host1", Project: "infra", Role: "db"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		httpNoopSink{},
	)
	if err != nil {
		t.Fatalf("buildHTTPClientWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("unexpected worker count: %d", len(workers))
	}
	if got := workers[0].cfg.ScrapeEvery; got != 30*time.Second {
		t.Fatalf("unexpected aligned scrape interval: %v", got)
	}
	if workers[0].cfg.KeepKnown {
		t.Fatalf("expected keep_known=false for http_client workers")
	}
}
