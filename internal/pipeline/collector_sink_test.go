package pipeline

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"log/slog"

	"magent/internal/config"
)

type fakeSender struct {
	encodedPayload []byte
	sendCalls      []string
	sendTimeouts   []time.Duration
	sendDeadlines  []bool
	failMap        map[string]error
}

// Encode returns fixed payload for deterministic tests.
// Params: events ignored in fake implementation.
// Returns: preconfigured payload.
func (s *fakeSender) Encode(_ []Event) ([]byte, error) {
	return s.encodedPayload, nil
}

// Send records call order and returns configured error by address.
// Params: ctx/timeout ignored; address selects simulated result.
// Returns: configured error or nil.
func (s *fakeSender) Send(ctx context.Context, address string, _ []byte, timeout time.Duration) error {
	s.sendCalls = append(s.sendCalls, address)
	s.sendTimeouts = append(s.sendTimeouts, timeout)
	_, hasDeadline := ctx.Deadline()
	s.sendDeadlines = append(s.sendDeadlines, hasDeadline)
	if err, ok := s.failMap[address]; ok {
		return err
	}
	return nil
}

// TestCollectorWorker_SendWithFailover verifies address failover order.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_SendWithFailover(t *testing.T) {
	sender := &fakeSender{
		encodedPayload: []byte("payload"),
		failMap: map[string]error{
			"127.0.0.1:1": errors.New("down"),
		},
	}

	worker := &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Addr:    []string{"127.0.0.1:1", "127.0.0.1:2"},
			Timeout: config.Duration{Duration: time.Second},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sender: sender,
	}

	if err := worker.sendWithFailover(context.Background(), []byte("x")); err != nil {
		t.Fatalf("sendWithFailover: %v", err)
	}
	if len(sender.sendCalls) != 2 {
		t.Fatalf("unexpected send attempts: %d", len(sender.sendCalls))
	}
	if sender.sendCalls[0] != "127.0.0.1:1" || sender.sendCalls[1] != "127.0.0.1:2" {
		t.Fatalf("unexpected failover order: %#v", sender.sendCalls)
	}
}

// TestCollectorWorker_SendWithFailoverUsesConfiguredTimeout verifies timeout/deadline propagation.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_SendWithFailoverUsesConfiguredTimeout(t *testing.T) {
	configuredTimeout := 250 * time.Millisecond
	sender := &fakeSender{
		encodedPayload: []byte("payload"),
		failMap: map[string]error{
			"127.0.0.1:1": errors.New("down"),
			"127.0.0.1:2": errors.New("down"),
		},
	}

	worker := &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Addr:    []string{"127.0.0.1:1", "127.0.0.1:2"},
			Timeout: config.Duration{Duration: configuredTimeout},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sender: sender,
	}

	if err := worker.sendWithFailover(context.Background(), []byte("x")); err == nil {
		t.Fatalf("expected failover error when all collector addresses fail")
	}

	if len(sender.sendTimeouts) != 2 {
		t.Fatalf("unexpected timeout call count: %d", len(sender.sendTimeouts))
	}
	for idx, got := range sender.sendTimeouts {
		if got != configuredTimeout {
			t.Fatalf("unexpected timeout at send[%d]: %v", idx, got)
		}
	}
	for idx, hasDeadline := range sender.sendDeadlines {
		if !hasDeadline {
			t.Fatalf("expected context deadline at send[%d]", idx)
		}
	}
}

// TestCollectorWorker_QueueOnFailure verifies enqueue path when all addresses fail.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_QueueOnFailure(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 10, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})

	sender := &fakeSender{
		encodedPayload: []byte("payload"),
		failMap: map[string]error{
			"127.0.0.1:1": errors.New("down"),
		},
	}

	worker := &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Addr: []string{"127.0.0.1:1"},
			Timeout: config.Duration{
				Duration: time.Second,
			},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sender: sender,
		queue:  queue,
		batch: []Event{
			{Metric: "cpu", Key: "total"},
		},
	}

	worker.flushBatch(context.Background())

	if got := queue.Pending(); got != 1 {
		t.Fatalf("expected one queued payload, got %d", got)
	}
}

// TestCollectorSink_ConsumeBackpressure verifies consume blocks until channel has space.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorSink_ConsumeBackpressure(t *testing.T) {
	worker := &collectorWorker{
		name:   "c1",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		input:  make(chan Event, 1),
	}
	worker.input <- Event{Metric: "cpu", Key: "prefilled"}

	sink := &CollectorSink{
		workers: []*collectorWorker{worker},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	done := make(chan error, 1)
	go func() {
		done <- sink.Consume(context.Background(), Event{Metric: "cpu", Key: "next"})
	}()

	select {
	case err := <-done:
		t.Fatalf("consume must block on full channel, got err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	<-worker.input

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume after backpressure release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("consume did not finish after channel release")
	}

	got := <-worker.input
	if got.Key != "next" {
		t.Fatalf("unexpected enqueued event key: %q", got.Key)
	}
}

// TestCollectorSink_ConsumeCanceled verifies consume returns context error under backpressure.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorSink_ConsumeCanceled(t *testing.T) {
	worker := &collectorWorker{
		name:   "c1",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		input:  make(chan Event, 1),
	}
	worker.input <- Event{Metric: "cpu", Key: "prefilled"}

	sink := &CollectorSink{
		workers: []*collectorWorker{worker},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := sink.Consume(ctx, Event{Metric: "cpu", Key: "blocked"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

// TestCollectorSink_ConsumeFanout verifies event fan-out to all collector workers.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorSink_ConsumeFanout(t *testing.T) {
	workerA := &collectorWorker{
		name:   "a",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		input:  make(chan Event, 1),
	}
	workerB := &collectorWorker{
		name:   "b",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		input:  make(chan Event, 1),
	}

	sink := &CollectorSink{
		workers: []*collectorWorker{workerA, workerB},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	event := Event{Metric: "cpu", Key: "total"}
	if err := sink.Consume(context.Background(), event); err != nil {
		t.Fatalf("consume fanout: %v", err)
	}

	select {
	case got := <-workerA.input:
		if got.Key != "total" {
			t.Fatalf("unexpected workerA event key: %q", got.Key)
		}
	default:
		t.Fatalf("workerA did not receive event")
	}

	select {
	case got := <-workerB.input:
		if got.Key != "total" {
			t.Fatalf("unexpected workerB event key: %q", got.Key)
		}
	default:
		t.Fatalf("workerB did not receive event")
	}
}

type retrySender struct {
	mu             sync.Mutex
	encodedPayload []byte
	results        []error
	sendCalls      int
}

// Encode returns fixed payload for deterministic retry tests.
// Params: events ignored in fake implementation.
// Returns: preconfigured payload.
func (s *retrySender) Encode(_ []Event) ([]byte, error) {
	return s.encodedPayload, nil
}

// Send returns scripted results in call order.
// Params: ctx/address/payload/timeout are ignored in this fake.
// Returns: scripted error for current call index.
func (s *retrySender) Send(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	callIdx := s.sendCalls
	s.sendCalls++
	if callIdx < len(s.results) {
		return s.results[callIdx]
	}
	return nil
}

// Calls returns current send call count.
// Params: none.
// Returns: number of Send invocations.
func (s *retrySender) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendCalls
}

// TestCollectorWorker_RetryDrainsQueue verifies queued payload is retried and acked by retry ticker.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_RetryDrainsQueue(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 10, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})

	sender := &retrySender{
		encodedPayload: []byte("payload"),
		results: []error{
			errors.New("down-now"),
			errors.New("down-drain-initial"),
			nil,
		},
	}

	worker := &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Addr:          []string{"127.0.0.1:1"},
			Timeout:       config.Duration{Duration: 50 * time.Millisecond},
			RetryInterval: config.Duration{Duration: 40 * time.Millisecond},
			Batch: config.CollectorBatchConfig{
				MaxEvents: 1,
				MaxAge:    config.Duration{Duration: time.Second},
			},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sender: sender,
		queue:  queue,
		input:  make(chan Event, 1),
		batch: []Event{
			{Metric: "cpu", Key: "total"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker.flushBatch(ctx)
	if got := queue.Pending(); got != 1 {
		t.Fatalf("expected one queued payload after initial send failure, got %d", got)
	}

	done := make(chan struct{})
	go func() {
		worker.run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if queue.Pending() == 0 && sender.Calls() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("worker did not stop after cancel")
	}

	if got := queue.Pending(); got != 0 {
		t.Fatalf("expected queued payload to be drained after retry, got pending=%d", got)
	}
	if sender.Calls() < 3 {
		t.Fatalf("expected at least three send attempts, got %d", sender.Calls())
	}
}

type recordingSink struct {
	id     string
	calls  *[]string
	mu     *sync.Mutex
	retErr error
}

// Consume records sink call order for assertions.
// Params: ctx/event are ignored.
// Returns: configured sink error.
func (s *recordingSink) Consume(_ context.Context, _ Event) error {
	s.mu.Lock()
	*s.calls = append(*s.calls, s.id)
	s.mu.Unlock()
	return s.retErr
}

// TestMultiSink_ConsumeSequential verifies all sinks are called and first error is returned.
// Params: testing.T for assertions.
// Returns: none.
func TestMultiSink_ConsumeSequential(t *testing.T) {
	calls := make([]string, 0, 3)
	var mu sync.Mutex

	sink := NewMultiSink(
		&recordingSink{id: "s1", calls: &calls, mu: &mu},
		nil,
		&recordingSink{id: "s2", calls: &calls, mu: &mu, retErr: errors.New("sink s2 failed")},
		&recordingSink{id: "s3", calls: &calls, mu: &mu},
	)

	err := sink.Consume(context.Background(), Event{Metric: "cpu", Key: "total"})
	if err == nil || err.Error() != "sink s2 failed" {
		t.Fatalf("unexpected consume error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("unexpected sink call count: %d", len(calls))
	}
	if calls[0] != "s1" || calls[1] != "s2" || calls[2] != "s3" {
		t.Fatalf("unexpected sink call order: %#v", calls)
	}
}
