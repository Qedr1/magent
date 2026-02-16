package pipeline

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

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
