package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"magent/internal/config"
)

const (
	defaultCollectorInputBuffer = 4096
)

// CollectorSink fans out events to per-collector workers.
// Params: collector worker list.
// Returns: sink implementation with lifecycle goroutines.
type CollectorSink struct {
	workers []*collectorWorker
	logger  *slog.Logger
}

type collectorWorker struct {
	name   string
	cfg    config.CollectorConfig
	logger *slog.Logger
	sender CollectorSender
	queue  *DiskQueue

	input chan Event

	batch      []Event
	batchStart time.Time
}

// NewCollectorSink creates sink workers and starts their loops.
// Params: ctx lifecycle context; collectors config list; logger root logger; sender transport implementation.
// Returns: collector sink or error.
func NewCollectorSink(
	ctx context.Context,
	collectors []config.CollectorConfig,
	logger *slog.Logger,
	sender CollectorSender,
) (*CollectorSink, error) {
	if len(collectors) == 0 {
		return nil, fmt.Errorf("collector list is empty")
	}
	if sender == nil {
		return nil, fmt.Errorf("collector sender is nil")
	}

	out := &CollectorSink{
		workers: make([]*collectorWorker, 0, len(collectors)),
		logger:  logger,
	}

	for idx, cfg := range collectors {
		name := strings.TrimSpace(cfg.Name)
		if name == "" {
			name = fmt.Sprintf("collector-%d", idx)
		}

		workerLogger := logger.With(slog.String("collector", name))

		var queue *DiskQueue
		var err error
		if cfg.Queue.Enabled {
			queue, err = OpenDiskQueue(
				cfg.Queue.Dir,
				cfg.Queue.MaxEvents,
				cfg.Queue.MaxAge.Duration,
			)
			if err != nil {
				return nil, fmt.Errorf("init queue for %s: %w", name, err)
			}
		}

		worker := &collectorWorker{
			name:   name,
			cfg:    cfg,
			logger: workerLogger,
			sender: sender,
			queue:  queue,
			input:  make(chan Event, defaultCollectorInputBuffer),
			batch:  make([]Event, 0, cfg.Batch.MaxEvents),
		}
		out.workers = append(out.workers, worker)
	}

	for _, worker := range out.workers {
		go worker.run(ctx)
	}

	return out, nil
}

// Consume enqueues event for all collectors (fan-out).
// Params: ctx consume context; event payload.
// Returns: context error when consume is canceled while waiting for backpressure release.
func (s *CollectorSink) Consume(ctx context.Context, event Event) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, worker := range s.workers {
		select {
		case worker.input <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// run executes collector worker loop: batching, sending, and queue draining.
// Params: ctx worker lifecycle context.
// Returns: none.
func (w *collectorWorker) run(ctx context.Context) {
	defer func() {
		if w.queue == nil {
			return
		}
		if err := w.queue.Close(); err != nil {
			w.logger.Error("close queue failed", slog.String("error", err.Error()))
		}
	}()

	flushTicker := time.NewTicker(time.Second)
	retryTicker := time.NewTicker(w.cfg.RetryInterval.Duration)
	defer flushTicker.Stop()
	defer retryTicker.Stop()

	_ = w.drainQueue(ctx)

	for {
		select {
		case <-ctx.Done():
			w.flushBatch(ctx)
			_ = w.drainQueue(ctx)
			return
		case event := <-w.input:
			w.appendBatch(event)
			if uint64(len(w.batch)) >= w.cfg.Batch.MaxEvents {
				w.flushBatch(ctx)
			}
		case <-flushTicker.C:
			w.flushByAge(ctx)
		case <-retryTicker.C:
			_ = w.drainQueue(ctx)
		}
	}
}

// appendBatch appends one event into current in-memory batch.
// Params: event payload.
// Returns: none.
func (w *collectorWorker) appendBatch(event Event) {
	if len(w.batch) == 0 {
		w.batchStart = time.Now()
	}
	w.batch = append(w.batch, event)
}

// flushByAge flushes batch when max_age threshold is reached.
// Params: ctx lifecycle context.
// Returns: none.
func (w *collectorWorker) flushByAge(ctx context.Context) {
	if len(w.batch) == 0 {
		return
	}
	if w.cfg.Batch.MaxAge.Duration <= 0 {
		return
	}
	if time.Since(w.batchStart) < w.cfg.Batch.MaxAge.Duration {
		return
	}
	w.flushBatch(ctx)
}

// flushBatch tries to send current batch and persists to queue on failure.
// Params: ctx lifecycle context.
// Returns: none.
func (w *collectorWorker) flushBatch(ctx context.Context) {
	if len(w.batch) == 0 {
		return
	}

	payload, err := w.sender.Encode(w.batch)
	if err != nil {
		w.logger.Error("encode collector batch failed", slog.String("error", err.Error()))
		w.batch = w.batch[:0]
		return
	}

	if err := w.sendWithFailover(ctx, payload); err != nil {
		if w.queue != nil {
			if queueErr := w.queue.Enqueue(payload); queueErr != nil {
				w.logger.Error("enqueue failed", slog.String("error", queueErr.Error()))
			} else {
				w.logger.Warn(
					"collector unavailable, batch queued",
					slog.Int("events", len(w.batch)),
					slog.Int("bytes", len(payload)),
				)
			}
		} else {
			w.logger.Error(
				"collector unavailable, dropping batch (queue disabled)",
				slog.Int("events", len(w.batch)),
				slog.String("error", err.Error()),
			)
		}
	} else {
		_ = w.drainQueue(ctx)
	}

	w.batch = w.batch[:0]
}

// sendWithFailover attempts payload delivery to collector addresses in order.
// Params: ctx lifecycle context; payload encoded batch.
// Returns: nil on first successful send, error when all addresses fail.
func (w *collectorWorker) sendWithFailover(ctx context.Context, payload []byte) error {
	var (
		lastErr error
	)

	for _, address := range w.cfg.Addr {
		addressValue := strings.TrimSpace(address)
		if addressValue == "" {
			continue
		}

		sendCtx, cancel := context.WithTimeout(ctx, w.cfg.Timeout.Duration)
		err := w.sender.Send(sendCtx, addressValue, payload, w.cfg.Timeout.Duration)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		w.logger.Warn("send attempt failed", slog.String("address", addressValue), slog.String("error", err.Error()))
	}

	if lastErr == nil {
		return fmt.Errorf("no collector addresses configured")
	}
	return lastErr
}

// drainQueue sends queued payloads while collector is reachable.
// Params: ctx lifecycle context.
// Returns: nil when queue is empty or drained, error on repeated send failure.
func (w *collectorWorker) drainQueue(ctx context.Context) error {
	if w.queue == nil {
		return nil
	}

	for {
		record, err := w.queue.Peek()
		if err != nil {
			if errors.Is(err, errQueueEmpty) {
				return nil
			}
			w.logger.Error("peek queue failed", slog.String("error", err.Error()))
			return err
		}

		if err := w.sendWithFailover(ctx, record.payload); err != nil {
			return err
		}
		if err := w.queue.Ack(record); err != nil {
			w.logger.Error("ack queue record failed", slog.String("error", err.Error()))
			return err
		}
	}
}

// MultiSink dispatches one event to multiple sink implementations.
// Params: sink list.
// Returns: composite sink.
type MultiSink struct {
	sinks []Sink
}

// NewMultiSink builds composite sink from sink list.
// Params: sinks target list.
// Returns: multi sink implementation.
func NewMultiSink(sinks ...Sink) *MultiSink {
	out := make([]Sink, 0, len(sinks))
	for _, sink := range sinks {
		if sink == nil {
			continue
		}
		out = append(out, sink)
	}
	return &MultiSink{sinks: out}
}

// Consume forwards event to each child sink.
// Params: ctx consume context; event payload.
// Returns: first error from downstream sinks, if any.
func (s *MultiSink) Consume(ctx context.Context, event Event) error {
	var firstErr error
	for _, sink := range s.sinks {
		if err := sink.Consume(ctx, event); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
