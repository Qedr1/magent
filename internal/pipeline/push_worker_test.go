package pipeline

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestNewPushWorker_AllowsEmptyPercentiles verifies last-only mode for push metrics.
// Params: testing.T for assertions.
// Returns: none.
func TestNewPushWorker_AllowsEmptyPercentiles(t *testing.T) {
	_, err := newPushWorker(
		PushWorkerConfig{
			Metric:      "http_server_demo",
			Instance:    "http-server-demo",
			SendEvery:   30 * time.Second,
			Percentiles: nil,
			Tags: EventTags{
				DC:      "dc1",
				Host:    "host1",
				Project: "infra",
				Role:    "api",
			},
			KeepKnown:  true,
			MaxPending: 1,
		},
		noopSink{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("newPushWorker must accept empty percentiles: %v", err)
	}
}
