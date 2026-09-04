package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"magent/internal/config"
)

const (
	defaultCollectorInputBuffer = 4096
)

var errNoActiveAddress = errors.New("no active collector address")

// CollectorSink fans out events to per-collector workers.
// Params: collector worker list.
// Returns: sink implementation with own lifecycle goroutines.
type CollectorSink struct {
	workers []*collectorWorker
	logger  *slog.Logger
	sender  CollectorSender

	cancel context.CancelFunc

	workersWG sync.WaitGroup
	closedCh  chan struct{}
	closeOnce sync.Once
}

type collectorWorker struct {
	name   string
	cfg    config.CollectorConfig
	logger *slog.Logger
	sender CollectorSender
	queue  *DiskQueue

	input   chan Event
	inputMu sync.Mutex

	batch      []Event
	batchStart time.Time

	healthMu sync.RWMutex
	active   string

	overflowDropped atomic.Uint64
	batchesSent     atomic.Uint64
	batchesFailed   atomic.Uint64
	addrSwitches    atomic.Uint64
}

type senderCloser interface {
	Close() error
}

// NewCollectorSink creates sink workers and starts their loops on an internal lifecycle.
// Params: collectors config list; logger root logger; sender transport implementation.
// Returns: collector sink or error.
func NewCollectorSink(
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

	ctx, cancel := context.WithCancel(context.Background())

	out := &CollectorSink{
		workers: make([]*collectorWorker, 0, len(collectors)),
		logger:  logger,
		sender:  sender,
		cancel:  cancel,
		closedCh: make(chan struct{}),
	}
	cleanupQueues := func() {
		for _, worker := range out.workers {
			if worker.queue == nil {
				continue
			}
			_ = worker.queue.Close()
		}
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
				cleanupQueues()
				cancel()
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

	out.workersWG.Add(len(out.workers))
	for _, worker := range out.workers {
		go func(active *collectorWorker) {
			defer out.workersWG.Done()
			active.run(ctx)
		}(worker)
	}
	go func() {
		out.workersWG.Wait()
		out.closeSender()
		close(out.closedCh)
	}()

	return out, nil
}

// Close stops all collector workers, flushes pending batches, and closes sender resources.
// Params: none.
// Returns: nil after graceful shutdown completes.
func (s *CollectorSink) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.closedCh
	})
	return nil
}

// Consume enqueues event for all collectors (fan-out).
// Params: ctx consume context; event payload.
// Returns: context error when consume is canceled while waiting for backpressure release.
func (s *CollectorSink) Consume(ctx context.Context, event Event) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for _, worker := range s.workers {
		if err := worker.enqueue(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

// closeSender closes collector sender resources once after worker shutdown.
// Params: none.
// Returns: none.
func (s *CollectorSink) closeSender() {
	if s == nil {
		return
	}
	closer, ok := s.sender.(senderCloser)
	if !ok {
		return
	}
	if err := closer.Close(); err != nil && s.logger != nil {
		s.logger.Error("close collector sender failed", slog.String("error", err.Error()))
	}
}

// enqueue puts one event into worker input applying configured overflow policy.
// Params: ctx consume context; event payload.
// Returns: context error for block policy cancellation; nil otherwise.
func (w *collectorWorker) enqueue(ctx context.Context, event Event) error {
	if !w.cfg.OverflowDropOldest() {
		select {
		case w.input <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	w.inputMu.Lock()
	defer w.inputMu.Unlock()

	select {
	case w.input <- event:
		return nil
	default:
	}

	select {
	case <-w.input:
		w.overflowDropped.Add(1)
	default:
	}

	select {
	case w.input <- event:
	default:
		// Zero-capacity buffer: drop newest.
		w.overflowDropped.Add(1)
	}
	return nil
}

// run executes collector worker loop: batching, sending, health checks, and queue draining.
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

	retryEvery := w.cfg.RetryInterval.Duration
	if retryEvery <= 0 {
		retryEvery = time.Second
	}
	healthEvery := w.cfg.HealthInterval.Duration
	if healthEvery <= 0 {
		healthEvery = 3 * time.Second
	}

	flushTicker := time.NewTicker(time.Second)
	retryTicker := time.NewTicker(retryEvery)
	healthTicker := time.NewTicker(healthEvery)
	defer flushTicker.Stop()
	defer retryTicker.Stop()
	defer healthTicker.Stop()

	w.refreshHealth(ctx)
	_ = w.drainQueue(ctx)

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), w.shutdownDrainTimeout())
			w.flushBatch(shutdownCtx)
			_ = w.drainQueue(shutdownCtx)
			cancel()
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
		case <-healthTicker.C:
			w.refreshHealth(ctx)
		}
	}
}

// shutdownDrainTimeout calculates bounded timeout used for final flush/drain after root context cancellation.
// Params: none.
// Returns: timeout duration for graceful collector shutdown.
func (w *collectorWorker) shutdownDrainTimeout() time.Duration {
	base := w.cfg.Timeout.Duration
	if base <= 0 {
		base = 5 * time.Second
	}

	addresses := len(w.addresses())
	if addresses == 0 {
		addresses = 1
	}

	timeout := time.Duration(addresses)*base + 2*time.Second
	if timeout < 3*time.Second {
		timeout = 3 * time.Second
	}
	if timeout > time.Minute {
		timeout = time.Minute
	}
	return timeout
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

// flushBatch delivers current batch preserving FIFO against queued records.
// Params: ctx lifecycle context.
// Returns: none.
func (w *collectorWorker) flushBatch(ctx context.Context) {
	if len(w.batch) == 0 {
		return
	}

	if w.queue != nil && w.queue.Pending() > 0 {
		// FIFO: park fresh batch behind queued records, then drain in write order.
		if err := w.enqueueBatch(ctx, w.batch); err != nil {
			w.logger.Error("enqueue failed", slog.String("error", err.Error()))
		} else {
			_ = w.drainQueue(ctx)
		}
		w.batch = w.batch[:0]
		return
	}

	payload, encodeErr := w.encodeBatch(ctx, w.batch)
	if encodeErr != nil {
		w.logger.Error("encode collector batch failed", slog.String("error", encodeErr.Error()))
		w.batch = w.batch[:0]
		return
	}

	if err := w.sendToActive(ctx, payload); err != nil {
		w.batchesFailed.Add(1)
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
		w.batchesSent.Add(1)
		_ = w.drainQueue(ctx)
	}

	w.batch = w.batch[:0]
}

// encodeBatch serializes events once, injecting source IP of the active address when known.
// Falls back to the first configured address: route-based resolution works during outage.
// Params: ctx lifecycle context; events batch payload.
// Returns: encoded protobuf payload or encode error.
func (w *collectorWorker) encodeBatch(ctx context.Context, events []Event) ([]byte, error) {
	hostAddr := w.activeAddress()
	if hostAddr == "" {
		if addrs := w.addresses(); len(addrs) > 0 {
			hostAddr = addrs[0]
		}
	}

	hostIP := ""
	if hostAddr != "" {
		if ip, err := w.sender.LocalIP(ctx, hostAddr, w.cfg.Timeout.Duration); err == nil {
			hostIP = ip
		}
	}
	return w.sender.Encode(events, hostIP)
}

// enqueueBatch encodes events and appends payload to the disk queue.
// Params: ctx lifecycle context; events batch payload.
// Returns: encode or queue error.
func (w *collectorWorker) enqueueBatch(ctx context.Context, events []Event) error {
	if w.queue == nil {
		return fmt.Errorf("queue is not configured")
	}
	payload, err := w.encodeBatch(ctx, events)
	if err != nil {
		return err
	}
	return w.queue.Enqueue(payload)
}

// sendToActive delivers payload to the active address with re-election and one retry on failure.
// Params: ctx lifecycle context; payload encoded batch.
// Returns: nil on successful delivery, error when no alive address accepts the payload.
func (w *collectorWorker) sendToActive(ctx context.Context, payload []byte) error {
	addr := w.ensureActive(ctx)
	if addr == "" {
		return errNoActiveAddress
	}

	if err := w.sender.Send(ctx, addr, payload, w.cfg.Timeout.Duration, w.cfg.CompressionGzip()); err != nil {
		w.logger.Warn("send attempt failed", slog.String("address", addr), slog.String("error", err.Error()))
		w.clearActive(addr)

		next := w.probeAll(ctx)
		w.setActive(next)
		if next == "" {
			return err
		}

		if retryErr := w.sender.Send(ctx, next, payload, w.cfg.Timeout.Duration, w.cfg.CompressionGzip()); retryErr != nil {
			w.logger.Warn("send retry failed", slog.String("address", next), slog.String("error", retryErr.Error()))
			w.clearActive(next)
			return retryErr
		}
	}
	return nil
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

		if err := w.sendToActive(ctx, record.payload); err != nil {
			w.batchesFailed.Add(1)
			return err
		}
		w.batchesSent.Add(1)
		if err := w.queue.Ack(record); err != nil {
			w.logger.Error("ack queue record failed", slog.String("error", err.Error()))
			return err
		}
	}
}

// activeAddress returns currently elected collector address.
// Params: none.
// Returns: active address or empty string when none is elected.
func (w *collectorWorker) activeAddress() string {
	w.healthMu.RLock()
	defer w.healthMu.RUnlock()
	return w.active
}

// setActive elects new active address and counts transitions.
// Params: next elected address (may be empty).
// Returns: none.
func (w *collectorWorker) setActive(next string) {
	w.healthMu.Lock()
	if next != w.active {
		if next != "" {
			w.addrSwitches.Add(1)
		}
		w.active = next
	}
	w.healthMu.Unlock()
}

// clearActive drops elected address when it matches the failed one.
// Params: addr failed address.
// Returns: none.
func (w *collectorWorker) clearActive(addr string) {
	w.healthMu.Lock()
	if w.active == addr {
		w.active = ""
	}
	w.healthMu.Unlock()
}

// ensureActive returns elected address, running election when none is active.
// Params: ctx lifecycle context.
// Returns: active address or empty string when all addresses are unreachable.
func (w *collectorWorker) ensureActive(ctx context.Context) string {
	if active := w.activeAddress(); active != "" {
		return active
	}
	next := w.probeAll(ctx)
	w.setActive(next)
	return next
}

// refreshHealth keeps the active address when alive, otherwise re-elects.
// Params: ctx lifecycle context.
// Returns: none.
func (w *collectorWorker) refreshHealth(ctx context.Context) {
	if active := w.activeAddress(); active != "" {
		if err := w.sender.Check(ctx, active, w.cfg.Timeout.Duration); err == nil {
			return
		}
		w.clearActive(active)
	}
	w.setActive(w.probeAll(ctx))
}

// probeAll checks all configured addresses concurrently and returns the first alive one.
// Params: ctx lifecycle context.
// Returns: first responding address or empty string when all probes fail.
func (w *collectorWorker) probeAll(ctx context.Context) string {
	addrs := w.addresses()
	if len(addrs) == 0 {
		return ""
	}

	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan string, len(addrs))
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go func(candidate string) {
			defer wg.Done()
			if err := w.sender.Check(probeCtx, candidate, w.cfg.Timeout.Duration); err == nil {
				select {
				case resCh <- candidate:
				case <-probeCtx.Done():
				}
			}
		}(addr)
	}
	go func() {
		wg.Wait()
		close(resCh)
	}()

	first, ok := <-resCh
	if !ok {
		return ""
	}
	return first
}

// addresses returns cleaned non-empty collector addresses in config order.
// Params: none.
// Returns: address list.
func (w *collectorWorker) addresses() []string {
	out := make([]string, 0, len(w.cfg.Addr))
	for _, address := range w.cfg.Addr {
		if trimmed := strings.TrimSpace(address); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
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
