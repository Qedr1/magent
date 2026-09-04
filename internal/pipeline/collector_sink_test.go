package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"log/slog"

	"magent/internal/config"
)

type fakeSender struct {
	mu           sync.Mutex
	encodeCalls  int
	sendCalls    []string
	sendPayloads []string
	sendTimeouts []time.Duration
	sendGzip     []bool
	checkCalls   []string
	checkFail    map[string]error
	sendFail     map[string]error
}

// Encode returns deterministic payload per call and records host ip usage.
// Params: events ignored; hostIP recorded for assertions.
// Returns: unique payload per invocation.
func (s *fakeSender) Encode(_ []Event, _ string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.encodeCalls++
	return []byte(fmt.Sprintf("payload-%d", s.encodeCalls)), nil
}

// Send records call order/payload and returns configured error by address.
// Params: ctx/timeout recorded; address selects simulated result.
// Returns: configured error or nil.
func (s *fakeSender) Send(_ context.Context, address string, payload []byte, timeout time.Duration, gzip bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendCalls = append(s.sendCalls, address)
	s.sendPayloads = append(s.sendPayloads, string(payload))
	s.sendTimeouts = append(s.sendTimeouts, timeout)
	s.sendGzip = append(s.sendGzip, gzip)
	if err, ok := s.sendFail[address]; ok {
		return err
	}
	return nil
}

// Check records probes and returns configured error by address.
// Params: address selects simulated result.
// Returns: configured error or nil.
func (s *fakeSender) Check(_ context.Context, address string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkCalls = append(s.checkCalls, address)
	if err, ok := s.checkFail[address]; ok {
		return err
	}
	return nil
}

// LocalIP returns static source ip.
// Params: all params ignored.
// Returns: loopback ip string.
func (s *fakeSender) LocalIP(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "127.0.0.1", nil
}

func (s *fakeSender) sendsSnapshot() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := append([]string(nil), s.sendCalls...)
	payloads := append([]string(nil), s.sendPayloads...)
	return calls, payloads
}

func newTestWorker(sender CollectorSender, addrs ...string) *collectorWorker {
	return &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Addr:    addrs,
			Timeout: config.Duration{Duration: time.Second},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sender: sender,
	}
}

// TestCollectorWorker_SendUsesFirstAliveAddress verifies health-first election and single-address send.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_SendUsesFirstAliveAddress(t *testing.T) {
	sender := &fakeSender{
		checkFail: map[string]error{
			"127.0.0.1:1": errors.New("down"),
		},
	}
	worker := newTestWorker(sender, "127.0.0.1:1", "127.0.0.1:2")

	if err := worker.sendToActive(context.Background(), []byte("x")); err != nil {
		t.Fatalf("sendToActive: %v", err)
	}

	calls, _ := sender.sendsSnapshot()
	if len(calls) != 1 || calls[0] != "127.0.0.1:2" {
		t.Fatalf("unexpected send addresses: %#v", calls)
	}
	if got := worker.activeAddress(); got != "127.0.0.1:2" {
		t.Fatalf("unexpected active address: %q", got)
	}
	if got := worker.addrSwitches.Load(); got != 1 {
		t.Fatalf("unexpected addr switches: %d", got)
	}
}

// TestCollectorWorker_SendFailureReelectsAndRetries verifies re-election and batch retry on send error.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_SendFailureReelectsAndRetries(t *testing.T) {
	sender := &fakeSender{
		checkFail: map[string]error{
			"127.0.0.1:1": errors.New("dead"),
		},
		sendFail: map[string]error{
			"127.0.0.1:1": errors.New("send failed"),
		},
	}
	worker := newTestWorker(sender, "127.0.0.1:1", "127.0.0.1:2")
	worker.active = "127.0.0.1:1"

	if err := worker.sendToActive(context.Background(), []byte("x")); err != nil {
		t.Fatalf("sendToActive: %v", err)
	}

	calls, _ := sender.sendsSnapshot()
	if len(calls) != 2 || calls[0] != "127.0.0.1:1" || calls[1] != "127.0.0.1:2" {
		t.Fatalf("unexpected send sequence: %#v", calls)
	}
	if got := worker.activeAddress(); got != "127.0.0.1:2" {
		t.Fatalf("unexpected re-elected address: %q", got)
	}
}

// TestCollectorWorker_NoActiveAddress verifies error when every probe fails.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_NoActiveAddress(t *testing.T) {
	sender := &fakeSender{
		checkFail: map[string]error{
			"127.0.0.1:1": errors.New("down"),
			"127.0.0.1:2": errors.New("down"),
		},
	}
	worker := newTestWorker(sender, "127.0.0.1:1", "127.0.0.1:2")

	if err := worker.sendToActive(context.Background(), []byte("x")); !errors.Is(err, errNoActiveAddress) {
		t.Fatalf("expected errNoActiveAddress, got %v", err)
	}

	calls, _ := sender.sendsSnapshot()
	if len(calls) != 0 {
		t.Fatalf("expected no send attempts, got %#v", calls)
	}
}

// TestCollectorWorker_RefreshHealthKeepsAliveActive verifies sticky active address and re-election on death.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_RefreshHealthKeepsAliveActive(t *testing.T) {
	sender := &fakeSender{}
	worker := newTestWorker(sender, "127.0.0.1:1", "127.0.0.1:2")
	worker.active = "127.0.0.1:1"

	worker.refreshHealth(context.Background())
	if got := worker.activeAddress(); got != "127.0.0.1:1" {
		t.Fatalf("active address must stay while alive, got %q", got)
	}

	sender.mu.Lock()
	sender.checkFail = map[string]error{"127.0.0.1:1": errors.New("down")}
	sender.mu.Unlock()

	worker.refreshHealth(context.Background())
	if got := worker.activeAddress(); got != "127.0.0.1:2" {
		t.Fatalf("expected re-election to alive address, got %q", got)
	}
}

// TestCollectorWorker_EncodeOncePerBatch verifies single encode for multi-address batch failure path.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_EncodeOncePerBatch(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 10, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})

	sender := &fakeSender{
		sendFail: map[string]error{
			"127.0.0.1:1": errors.New("down"),
			"127.0.0.1:2": errors.New("down"),
		},
	}
	worker := newTestWorker(sender, "127.0.0.1:1", "127.0.0.1:2")
	worker.queue = queue
	worker.active = "127.0.0.1:1"
	worker.batch = []Event{{Metric: "cpu", Key: "total"}}

	worker.flushBatch(context.Background())

	if got := sender.encodeCalls; got != 1 {
		t.Fatalf("expected single encode for one batch, got %d", got)
	}
	if got := queue.Pending(); got != 1 {
		t.Fatalf("expected one queued payload, got %d", got)
	}
}

// TestCollectorWorker_FIFOPendingQueue verifies fresh batch is queued behind pending records and drained in order.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_FIFOPendingQueue(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 10, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})
	if err := queue.Enqueue([]byte("old-payload")); err != nil {
		t.Fatalf("enqueue old payload: %v", err)
	}

	sender := &fakeSender{}
	worker := newTestWorker(sender, "127.0.0.1:1")
	worker.queue = queue
	worker.batch = []Event{{Metric: "cpu", Key: "total"}}

	worker.flushBatch(context.Background())

	_, payloads := sender.sendsSnapshot()
	if len(payloads) != 2 || payloads[0] != "old-payload" || payloads[1] != "payload-1" {
		t.Fatalf("expected strict FIFO send order [old-payload payload-1], got %#v", payloads)
	}
	if got := queue.Pending(); got != 0 {
		t.Fatalf("expected drained queue, got pending=%d", got)
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
		checkFail: map[string]error{
			"127.0.0.1:1": errors.New("down"),
		},
	}
	worker := newTestWorker(sender, "127.0.0.1:1")
	worker.queue = queue
	worker.batch = []Event{{Metric: "cpu", Key: "total"}}

	worker.flushBatch(context.Background())

	if got := queue.Pending(); got != 1 {
		t.Fatalf("expected one queued payload, got %d", got)
	}
	if got := worker.batchesFailed.Load(); got != 1 {
		t.Fatalf("expected one failed batch counter, got %d", got)
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

// TestCollectorSink_ConsumeDropOldest verifies drop_oldest overflow policy never blocks the producer.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorSink_ConsumeDropOldest(t *testing.T) {
	worker := &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Overflow: "drop_oldest",
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		input:  make(chan Event, 1),
	}
	worker.input <- Event{Metric: "cpu", Key: "oldest"}

	sink := &CollectorSink{
		workers: []*collectorWorker{worker},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	done := make(chan error, 1)
	go func() {
		done <- sink.Consume(context.Background(), Event{Metric: "cpu", Key: "newest"})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume with drop_oldest must not block: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("consume with drop_oldest blocked on full buffer")
	}

	got := <-worker.input
	if got.Key != "newest" {
		t.Fatalf("expected oldest event evicted, channel holds key=%q", got.Key)
	}
	if dropped := worker.overflowDropped.Load(); dropped != 1 {
		t.Fatalf("expected one overflow drop, got %d", dropped)
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
	mu      sync.Mutex
	results []error
	calls   int
}

// Encode returns fixed payload for deterministic retry tests.
// Params: events/host ip ignored in fake implementation.
// Returns: preconfigured payload.
func (s *retrySender) Encode(_ []Event, _ string) ([]byte, error) {
	return []byte("payload"), nil
}

// Send returns scripted results in call order.
// Params: all params ignored in this fake.
// Returns: scripted error for current call index.
func (s *retrySender) Send(_ context.Context, _ string, _ []byte, _ time.Duration, _ bool) error {
	return s.nextResult()
}

// Check returns scripted results in call order.
// Params: all params ignored in this fake.
// Returns: scripted error for current call index.
func (s *retrySender) Check(_ context.Context, _ string, _ time.Duration) error {
	return s.nextResult()
}

// LocalIP returns static source ip.
// Params: all params ignored.
// Returns: loopback ip string.
func (s *retrySender) LocalIP(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "127.0.0.1", nil
}

func (s *retrySender) nextResult() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	callIdx := s.calls
	s.calls++
	if callIdx < len(s.results) {
		return s.results[callIdx]
	}
	return nil
}

// Calls returns current call count.
// Params: none.
// Returns: number of Check/Send invocations.
func (s *retrySender) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type cancelAwareSender struct {
	mu         sync.Mutex
	successful int
}

// Encode returns static payload.
// Params: events/host ip ignored.
// Returns: static payload.
func (s *cancelAwareSender) Encode(_ []Event, _ string) ([]byte, error) {
	return []byte("payload"), nil
}

// Send succeeds only with non-canceled contexts.
// Params: ctx/address/payload/timeout/gzip.
// Returns: context error when canceled.
func (s *cancelAwareSender) Send(ctx context.Context, _ string, _ []byte, _ time.Duration, _ bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.successful++
	s.mu.Unlock()
	return nil
}

// Check succeeds only with non-canceled contexts.
// Params: ctx/address/timeout.
// Returns: context error when canceled.
func (s *cancelAwareSender) Check(ctx context.Context, _ string, _ time.Duration) error {
	return ctx.Err()
}

// LocalIP returns static source ip.
// Params: all params ignored.
// Returns: loopback ip string.
func (s *cancelAwareSender) LocalIP(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "127.0.0.1", nil
}

// Successful returns count of successful Send calls.
// Params: none.
// Returns: successful call count.
func (s *cancelAwareSender) Successful() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.successful
}

type closeAwareSender struct {
	mu       sync.Mutex
	closeCnt int
}

// Encode returns static payload.
// Params: events/host ip ignored.
// Returns: static encoded payload.
func (s *closeAwareSender) Encode(_ []Event, _ string) ([]byte, error) {
	return []byte("payload"), nil
}

// Send succeeds in tests.
// Params: all params ignored.
// Returns: nil.
func (s *closeAwareSender) Send(_ context.Context, _ string, _ []byte, _ time.Duration, _ bool) error {
	return nil
}

// Check succeeds in tests.
// Params: all params ignored.
// Returns: nil.
func (s *closeAwareSender) Check(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

// LocalIP returns static source ip.
// Params: all params ignored.
// Returns: loopback ip string.
func (s *closeAwareSender) LocalIP(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "127.0.0.1", nil
}

// Close tracks sender close invocations.
// Params: none.
// Returns: nil.
func (s *closeAwareSender) Close() error {
	s.mu.Lock()
	s.closeCnt++
	s.mu.Unlock()
	return nil
}

// CloseCount returns total Close invocations.
// Params: none.
// Returns: close call count.
func (s *closeAwareSender) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCnt
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

// TestCollectorWorker_RunFlushesBatchOnShutdown verifies final flush uses graceful context after cancellation.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorWorker_RunFlushesBatchOnShutdown(t *testing.T) {
	sender := &cancelAwareSender{}

	worker := &collectorWorker{
		name: "c1",
		cfg: config.CollectorConfig{
			Addr:          []string{"127.0.0.1:6000"},
			Timeout:       config.Duration{Duration: 50 * time.Millisecond},
			RetryInterval: config.Duration{Duration: time.Hour},
			Batch: config.CollectorBatchConfig{
				MaxEvents: 100,
				MaxAge:    config.Duration{Duration: time.Minute},
			},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		sender: sender,
		input:  make(chan Event, 1),
		batch: []Event{
			{Metric: "cpu", Key: "total"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker did not stop after cancel")
	}

	if sender.Successful() == 0 {
		t.Fatalf("expected final shutdown flush to send at least one batch")
	}
}

// TestCollectorSink_ClosesSenderOnceAfterWorkersStop verifies sender lifecycle close on sink shutdown.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectorSink_ClosesSenderOnceAfterWorkersStop(t *testing.T) {
	sender := &closeAwareSender{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	collectors := []config.CollectorConfig{
		{
			Name:          "c1",
			Addr:          []string{"127.0.0.1:6000"},
			Timeout:       config.Duration{Duration: 100 * time.Millisecond},
			RetryInterval: config.Duration{Duration: 100 * time.Millisecond},
			Batch: config.CollectorBatchConfig{
				MaxEvents: 10,
				MaxAge:    config.Duration{Duration: time.Second},
			},
		},
		{
			Name:          "c2",
			Addr:          []string{"127.0.0.1:6001"},
			Timeout:       config.Duration{Duration: 100 * time.Millisecond},
			RetryInterval: config.Duration{Duration: 100 * time.Millisecond},
			Batch: config.CollectorBatchConfig{
				MaxEvents: 10,
				MaxAge:    config.Duration{Duration: time.Second},
			},
		},
	}

	sink, err := NewCollectorSink(collectors, logger, sender)
	if err != nil {
		t.Fatalf("NewCollectorSink: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("sink close: %v", err)
	}

	if got := sender.CloseCount(); got != 1 {
		t.Fatalf("expected sender Close() to be called once, got %d", got)
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
