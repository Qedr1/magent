package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magent/internal/config"
)

type fakeEngine struct {
	stopped chan struct{}
}

type immediateEngine struct {
	err error
}

// Run blocks until context cancellation and marks engine stop.
// Params: ctx lifecycle context.
// Returns: nil on graceful stop.
func (e *fakeEngine) Run(ctx context.Context) error {
	<-ctx.Done()
	close(e.stopped)
	return nil
}

// Run exits immediately with predefined error.
// Params: _ ignored context.
// Returns: predefined run error.
func (e *immediateEngine) Run(_ context.Context) error {
	return e.err
}

type fakeEngineFactory struct {
	mu      sync.Mutex
	engines []*fakeEngine
	cfgs    []*config.Config
	failAt  map[int]error
}

// build creates one fake engine and records config snapshot.
// Params: _ ignored runtime context; cfg runtime config snapshot; _ ignored logger.
// Returns: fake engine or configured build error.
func (f *fakeEngineFactory) build(_ context.Context, cfg *config.Config, _ *slog.Logger) (engineRunner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	index := len(f.engines)
	if err, exists := f.failAt[index]; exists {
		return nil, err
	}

	engine := &fakeEngine{stopped: make(chan struct{})}
	f.engines = append(f.engines, engine)
	f.cfgs = append(f.cfgs, cfg)
	return engine, nil
}

// count returns created engines count.
// Params: none.
// Returns: number of created engine instances.
func (f *fakeEngineFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.engines)
}

// waitCount waits until created engines reaches expected value.
// Params: t test context; expected desired count.
// Returns: none; fails test on timeout.
func (f *fakeEngineFactory) waitCount(t *testing.T, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count() >= expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for engine count=%d (have=%d)", expected, f.count())
}

// waitStopped waits for specific engine stop signal.
// Params: t test context; index engine index.
// Returns: none; fails test on timeout.
func (f *fakeEngineFactory) waitStopped(t *testing.T, index int) {
	t.Helper()

	f.mu.Lock()
	if index >= len(f.engines) {
		f.mu.Unlock()
		t.Fatalf("engine index %d not found", index)
	}
	stopped := f.engines[index].stopped
	f.mu.Unlock()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting engine[%d] stop", index)
	}
}

// isStopped checks whether specific engine has been stopped.
// Params: index engine index.
// Returns: true when engine stop signal is closed.
func (f *fakeEngineFactory) isStopped(index int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= len(f.engines) {
		return false
	}
	select {
	case <-f.engines[index].stopped:
		return true
	default:
		return false
	}
}

// metricCounts returns CPU metric worker count per engine build.
// Params: none.
// Returns: slice of CPU worker counts by build order.
func (f *fakeEngineFactory) metricCounts() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]int, 0, len(f.cfgs))
	for _, cfg := range f.cfgs {
		out = append(out, len(cfg.Metrics.CPU))
	}
	return out
}

// collectorCounts returns collector count per engine build.
// Params: none.
// Returns: slice of collector counts by build order.
func (f *fakeEngineFactory) collectorCounts() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]int, 0, len(f.cfgs))
	for _, cfg := range f.cfgs {
		out = append(out, len(cfg.Collector))
	}
	return out
}

type loaderResponse struct {
	cfg *config.Config
	err error
}

type loaderSequence struct {
	mu        sync.Mutex
	responses []loaderResponse
	calls     int
}

// load returns next preconfigured response for config loading.
// Params: _ ignored path.
// Returns: config or error based on configured sequence.
func (l *loaderSequence) load(_ string) (*config.Config, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	index := l.calls
	l.calls++
	if index >= len(l.responses) {
		return nil, errors.New("unexpected config load call")
	}

	response := l.responses[index]
	if response.err != nil {
		return nil, response.err
	}
	return response.cfg, nil
}

type fakeLoggerFactory struct {
	created atomic.Int32
	closed  atomic.Int32
}

// create builds disposable logger and tracks create/close counts.
// Params: _ ignored log config.
// Returns: logger, close callback, and nil error.
func (f *fakeLoggerFactory) create(_ config.LogConfig) (*slog.Logger, func(), error) {
	f.created.Add(1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return logger, func() {
		f.closed.Add(1)
	}, nil
}

type fakePprofFactory struct {
	started atomic.Int32
	stopped atomic.Int32
}

// start tracks pprof start and returns tracked stop callback.
// Params: _ ignored context; _ ignored config; _ ignored logger.
// Returns: stop callback and nil error.
func (f *fakePprofFactory) start(_ context.Context, _ config.PprofConfig, _ *slog.Logger) (func(), error) {
	f.started.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			f.stopped.Add(1)
		})
	}, nil
}

// buildTestDeps creates run deps from fake components.
// Params: loader, loggers, pprof, and engines fakes.
// Returns: dependency set for runWithDeps tests.
func buildTestDeps(
	loader *loaderSequence,
	loggers *fakeLoggerFactory,
	pprof *fakePprofFactory,
	engines *fakeEngineFactory,
) runDeps {
	return runDeps{
		loadConfig: loader.load,
		newLogger:  loggers.create,
		startPprof: pprof.start,
		newEngine:  engines.build,
	}
}

// testConfig creates minimal config snapshot for runtime reload tests.
// Params: project project tag; cpuWorkers number of CPU workers; collectors number of collectors.
// Returns: config snapshot instance.
func testConfig(project string, cpuWorkers int, collectors int) *config.Config {
	cfg := &config.Config{
		Global: config.GlobalConfig{
			DC:      "dc1",
			Project: project,
			Role:    "role1",
			Host:    "host1",
		},
		Log: config.LogConfig{
			Console: config.LogSinkConfig{
				Enabled: true,
				Level:   "info",
				Format:  "line",
			},
		},
	}

	cfg.Metrics.CPU = make([]config.MetricWorkerConfig, cpuWorkers)
	cfg.Collector = make([]config.CollectorConfig, collectors)
	for idx := 0; idx < collectors; idx++ {
		cfg.Collector[idx] = config.CollectorConfig{
			Name: "collector",
			Addr: []string{"127.0.0.1:6000"},
			Timeout: config.Duration{
				Duration: time.Second,
			},
			RetryInterval: config.Duration{
				Duration: time.Second,
			},
			Batch: config.CollectorBatchConfig{
				MaxEvents: 1,
			},
		}
	}

	return cfg
}

// TestRunWithDeps_ReloadValidConfig verifies successful runtime swap on valid reload.
// Params: t test context.
// Returns: none.
func TestRunWithDeps_ReloadValidConfig(t *testing.T) {
	loader := &loaderSequence{
		responses: []loaderResponse{
			{cfg: testConfig("p1", 1, 1)},
			{cfg: testConfig("p2", 2, 2)},
		},
	}
	loggers := &fakeLoggerFactory{}
	pprof := &fakePprofFactory{}
	engines := &fakeEngineFactory{}
	deps := buildTestDeps(loader, loggers, pprof, engines)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reload := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWithDeps(ctx, Runtime{ConfigPath: "test.toml", Reload: reload}, deps)
	}()

	engines.waitCount(t, 1)
	reload <- struct{}{}
	engines.waitCount(t, 2)
	engines.waitStopped(t, 0)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithDeps: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting runWithDeps stop")
	}

	if got := loggers.created.Load(); got != 2 {
		t.Fatalf("logger created=%d, want=2", got)
	}
	if got := loggers.closed.Load(); got != 2 {
		t.Fatalf("logger closed=%d, want=2", got)
	}
	if got := pprof.started.Load(); got != 2 {
		t.Fatalf("pprof started=%d, want=2", got)
	}
	if got := pprof.stopped.Load(); got != 2 {
		t.Fatalf("pprof stopped=%d, want=2", got)
	}
}

// TestRunWithDeps_ReloadInvalidConfigKeepsRuntime verifies invalid reload keeps current runtime active.
// Params: t test context.
// Returns: none.
func TestRunWithDeps_ReloadInvalidConfigKeepsRuntime(t *testing.T) {
	loader := &loaderSequence{
		responses: []loaderResponse{
			{cfg: testConfig("p1", 1, 1)},
			{err: errors.New("invalid config")},
		},
	}
	loggers := &fakeLoggerFactory{}
	pprof := &fakePprofFactory{}
	engines := &fakeEngineFactory{}
	deps := buildTestDeps(loader, loggers, pprof, engines)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reload := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWithDeps(ctx, Runtime{ConfigPath: "test.toml", Reload: reload}, deps)
	}()

	engines.waitCount(t, 1)
	reload <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	if got := engines.count(); got != 1 {
		t.Fatalf("engine count=%d, want=1", got)
	}
	if engines.isStopped(0) {
		t.Fatal("runtime stopped after invalid reload")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithDeps: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting runWithDeps stop")
	}
}

// TestRunWithDeps_ReloadApplyMetricSetChanges verifies runtime rebuild on metric add/remove.
// Params: t test context.
// Returns: none.
func TestRunWithDeps_ReloadApplyMetricSetChanges(t *testing.T) {
	loader := &loaderSequence{
		responses: []loaderResponse{
			{cfg: testConfig("p1", 1, 1)},
			{cfg: testConfig("p1", 2, 1)},
			{cfg: testConfig("p1", 0, 1)},
		},
	}
	loggers := &fakeLoggerFactory{}
	pprof := &fakePprofFactory{}
	engines := &fakeEngineFactory{}
	deps := buildTestDeps(loader, loggers, pprof, engines)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reload := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWithDeps(ctx, Runtime{ConfigPath: "test.toml", Reload: reload}, deps)
	}()

	engines.waitCount(t, 1)
	reload <- struct{}{}
	engines.waitCount(t, 2)
	reload <- struct{}{}
	engines.waitCount(t, 3)

	counts := engines.metricCounts()
	want := []int{1, 2, 0}
	for idx := range want {
		if counts[idx] != want[idx] {
			t.Fatalf("metric count[%d]=%d, want=%d", idx, counts[idx], want[idx])
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithDeps: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting runWithDeps stop")
	}
}

// TestRunWithDeps_ReloadApplyCollectorSetChanges verifies runtime rebuild on collector add/remove.
// Params: t test context.
// Returns: none.
func TestRunWithDeps_ReloadApplyCollectorSetChanges(t *testing.T) {
	loader := &loaderSequence{
		responses: []loaderResponse{
			{cfg: testConfig("p1", 1, 1)},
			{cfg: testConfig("p1", 1, 2)},
			{cfg: testConfig("p1", 1, 1)},
		},
	}
	loggers := &fakeLoggerFactory{}
	pprof := &fakePprofFactory{}
	engines := &fakeEngineFactory{}
	deps := buildTestDeps(loader, loggers, pprof, engines)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reload := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWithDeps(ctx, Runtime{ConfigPath: "test.toml", Reload: reload}, deps)
	}()

	engines.waitCount(t, 1)
	reload <- struct{}{}
	engines.waitCount(t, 2)
	reload <- struct{}{}
	engines.waitCount(t, 3)

	counts := engines.collectorCounts()
	want := []int{1, 2, 1}
	for idx := range want {
		if counts[idx] != want[idx] {
			t.Fatalf("collector count[%d]=%d, want=%d", idx, counts[idx], want[idx])
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithDeps: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting runWithDeps stop")
	}
}

// TestRunWithDeps_EngineStopsUnexpectedly verifies run loop returns error when engine exits before context cancellation.
// Params: t test context.
// Returns: none.
func TestRunWithDeps_EngineStopsUnexpectedly(t *testing.T) {
	loader := &loaderSequence{
		responses: []loaderResponse{
			{cfg: testConfig("p1", 1, 1)},
		},
	}
	loggers := &fakeLoggerFactory{}
	pprof := &fakePprofFactory{}
	engineErr := errors.New("boom")
	deps := runDeps{
		loadConfig: loader.load,
		newLogger:  loggers.create,
		startPprof: pprof.start,
		newEngine: func(_ context.Context, _ *config.Config, _ *slog.Logger) (engineRunner, error) {
			return &immediateEngine{err: engineErr}, nil
		},
	}

	err := runWithDeps(context.Background(), Runtime{ConfigPath: "test.toml"}, deps)
	if err == nil {
		t.Fatal("expected runWithDeps error")
	}
	if !errors.Is(err, engineErr) {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := loggers.closed.Load(); got != 1 {
		t.Fatalf("logger closed=%d, want=1", got)
	}
	if got := pprof.stopped.Load(); got != 1 {
		t.Fatalf("pprof stopped=%d, want=1", got)
	}
}

// TestRunWithDeps_ReloadInterruptedByShutdown verifies graceful stop when shutdown interrupts reload apply.
// Params: t test context.
// Returns: none.
func TestRunWithDeps_ReloadInterruptedByShutdown(t *testing.T) {
	loader := &loaderSequence{
		responses: []loaderResponse{
			{cfg: testConfig("p1", 1, 1)},
			{cfg: testConfig("p2", 1, 1)},
		},
	}
	loggers := &fakeLoggerFactory{}
	pprof := &fakePprofFactory{}

	secondBuildStarted := make(chan struct{})
	var buildCount atomic.Int32
	deps := runDeps{
		loadConfig: loader.load,
		newLogger:  loggers.create,
		startPprof: pprof.start,
		newEngine: func(ctx context.Context, _ *config.Config, _ *slog.Logger) (engineRunner, error) {
			call := buildCount.Add(1)
			if call == 1 {
				return &fakeEngine{stopped: make(chan struct{})}, nil
			}
			close(secondBuildStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reload := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- runWithDeps(ctx, Runtime{ConfigPath: "test.toml", Reload: reload}, deps)
	}()

	reload <- struct{}{}
	select {
	case <-secondBuildStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting second build start")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithDeps: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting runWithDeps stop")
	}
}
