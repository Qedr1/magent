package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"magent/internal/metrics"
)

// PushWorkerConfig defines one push-based metric worker runtime.
// Params: metric identity, send schedule, aggregation options, and queue size.
// Returns: push worker runtime configuration.
type PushWorkerConfig struct {
	Metric      string
	Instance    string
	SendEvery   time.Duration
	Percentiles []int
	Tags        EventTags
	EmitFilter  EmitFilter
	KeepKnown   bool
	MaxPending  uint64
	DropVar     []string
	FilterVar   []string
	DropEvent   []DropCondition
}

type pushBatch struct {
	receivedAt uint64
	points     []metrics.Point
}

type pushWorker struct {
	cfg      PushWorkerConfig
	window   *window
	incoming chan pushBatch
}

// newPushWorker builds a push worker from runtime config.
// Params: cfg runtime settings; sink event consumer; logger root logger.
// Returns: worker instance or error.
func newPushWorker(cfg PushWorkerConfig, sink Sink, logger *slog.Logger) (*pushWorker, error) {
	if cfg.SendEvery <= 0 {
		return nil, fmt.Errorf("send interval must be > 0")
	}
	if cfg.MaxPending == 0 {
		return nil, fmt.Errorf("max_pending must be > 0")
	}

	window, instance, err := buildWorkerWindow(
		workerWindowBaseConfig{
			Metric:      cfg.Metric,
			Instance:    cfg.Instance,
			Percentiles: cfg.Percentiles,
			Tags:        cfg.Tags,
			EmitFilter:  cfg.EmitFilter,
			KeepKnown:   cfg.KeepKnown,
			DropVar:     cfg.DropVar,
			FilterVar:   cfg.FilterVar,
			DropEvent:   cfg.DropEvent,
		},
		sink,
		logger,
	)
	if err != nil {
		return nil, err
	}
	cfg.Instance = instance

	return &pushWorker{
		cfg:      cfg,
		window:   window,
		incoming: make(chan pushBatch, cfg.MaxPending),
	}, nil
}

// ingest attempts to enqueue one batch into the worker.
// Params: receivedAt is event receipt time; points parsed key/value samples.
// Returns: true if batch is accepted; false when queue is full (drop-new policy).
func (w *pushWorker) ingest(receivedAt time.Time, points []metrics.Point) bool {
	batch := pushBatch{
		receivedAt: uint64(receivedAt.UnixMilli()),
		points:     points,
	}

	select {
	case w.incoming <- batch:
		return true
	default:
		return false
	}
}

// run executes ingest/send loops until context cancellation.
// Params: ctx controls lifecycle.
// Returns: nil on graceful stop.
func (w *pushWorker) run(ctx context.Context) error {
	sendTicker := time.NewTicker(w.cfg.SendEvery)
	defer sendTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case batch := <-w.incoming:
			if !w.window.appendPoints(batch.points) {
				continue
			}
			w.window.observeDT(batch.receivedAt)
		case <-sendTicker.C:
			w.window.emitWindow(ctx, w.cfg.SendEvery)
		}
	}
}
