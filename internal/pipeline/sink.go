package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// Sink consumes emitted metric events.
// Params: context and one event payload.
// Returns: error if sink cannot process event.
type Sink interface {
	Consume(ctx context.Context, event Event) error
}

// LogSink writes event payloads into debug logs.
// Params: logger used for output.
// Returns: debug sink instance.
type LogSink struct {
	logger *slog.Logger
}

// NewLogSink creates a debug sink.
// Params: logger instance.
// Returns: event sink implementation.
func NewLogSink(logger *slog.Logger) *LogSink {
	return &LogSink{logger: logger}
}

// Consume logs one event as compact JSON.
// Params: ctx is unused; event payload to log.
// Returns: marshal error when payload cannot be encoded.
func (s *LogSink) Consume(ctx context.Context, event Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.logger.Enabled(ctx, slog.LevelDebug) {
		return nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	s.logger.Debug(
		"metric event",
		slog.String("metric", event.Metric),
		slog.String("key", event.Key),
		slog.String("payload", string(payload)),
	)

	return nil
}
