package metrics

import (
	"context"
	"testing"
	"time"
)

// TestParseKernelLoadAverages verifies /proc/loadavg parsing.
// Params: testing.T for assertions.
// Returns: none.
func TestParseKernelLoadAverages(t *testing.T) {
	got, err := parseKernelLoadAverages([]byte("0.12 1.34 5.67 1/234 5678\n"))
	if err != nil {
		t.Fatalf("parseKernelLoadAverages() error: %v", err)
	}
	if got.load1 != 0.12 || got.load5 != 1.34 || got.load15 != 5.67 {
		t.Fatalf("unexpected load values: %#v", got)
	}
}

// TestParseKernelCounters verifies /proc/stat counter parsing.
// Params: testing.T for assertions.
// Returns: none.
func TestParseKernelCounters(t *testing.T) {
	payload := []byte(
		"cpu  1 2 3 4 5 6 7 8 9 10\n" +
			"intr 120 1 2 3\n" +
			"ctxt 400\n" +
			"btime 1700000000\n" +
			"processes 45\n" +
			"procs_running 7\n" +
			"procs_blocked 2\n" +
			"softirq 88 0 1 2\n",
	)

	got, err := parseKernelCounters(payload)
	if err != nil {
		t.Fatalf("parseKernelCounters() error: %v", err)
	}
	if got.intr != 120 {
		t.Fatalf("unexpected intr: %d", got.intr)
	}
	if got.ctxt != 400 {
		t.Fatalf("unexpected ctxt: %d", got.ctxt)
	}
	if got.softirq != 88 {
		t.Fatalf("unexpected softirq: %d", got.softirq)
	}
	if got.forks != 45 {
		t.Fatalf("unexpected forks: %d", got.forks)
	}
	if got.procsRunning != 7 {
		t.Fatalf("unexpected procs_running: %d", got.procsRunning)
	}
	if got.procsBlocked != 2 {
		t.Fatalf("unexpected procs_blocked: %d", got.procsBlocked)
	}
}

// TestKERNELCollectorScrape verifies point shape and per-second counter deltas.
// Params: testing.T for assertions.
// Returns: none.
func TestKERNELCollectorScrape(t *testing.T) {
	loadPayload := []byte("0.50 0.25 0.10 1/200 3000\n")
	statPayload := []byte(
		"intr 100 1 2 3\n" +
			"ctxt 200\n" +
			"processes 50\n" +
			"procs_running 4\n" +
			"procs_blocked 1\n" +
			"softirq 300 1 2 3\n",
	)

	collector := NewKERNELCollector("kernel")
	collector.readFile = func(path string) ([]byte, error) {
		switch path {
		case "/proc/loadavg":
			return loadPayload, nil
		case "/proc/stat":
			return statPayload, nil
		default:
			return nil, nil
		}
	}

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("first scrape error: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected point count: %d", len(points))
	}
	if points[0].Key != "total" {
		t.Fatalf("unexpected key: %q", points[0].Key)
	}
	if points[0].Values["ctxt_per_sec"].Raw != 0 {
		t.Fatalf("expected ctxt_per_sec=0 on first scrape, got %v", points[0].Values["ctxt_per_sec"].Raw)
	}
	if points[0].Values["forks_per_sec"].Raw != 0 {
		t.Fatalf("expected forks_per_sec=0 on first scrape, got %v", points[0].Values["forks_per_sec"].Raw)
	}

	statPayload = []byte(
		"intr 300 1 2 3\n" +
			"ctxt 500\n" +
			"processes 110\n" +
			"procs_running 8\n" +
			"procs_blocked 0\n" +
			"softirq 600 1 2 3\n",
	)

	collector.mu.Lock()
	collector.prev.at = time.Now().Add(-2 * time.Second)
	collector.mu.Unlock()

	points, err = collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("second scrape error: %v", err)
	}
	if points[0].Values["ctxt_per_sec"].Raw <= 0 {
		t.Fatalf("expected ctxt_per_sec>0, got %v", points[0].Values["ctxt_per_sec"].Raw)
	}
	if points[0].Values["intr_per_sec"].Raw <= 0 {
		t.Fatalf("expected intr_per_sec>0, got %v", points[0].Values["intr_per_sec"].Raw)
	}
	if points[0].Values["softirq_per_sec"].Raw <= 0 {
		t.Fatalf("expected softirq_per_sec>0, got %v", points[0].Values["softirq_per_sec"].Raw)
	}
	if points[0].Values["forks_per_sec"].Raw <= 0 {
		t.Fatalf("expected forks_per_sec>0, got %v", points[0].Values["forks_per_sec"].Raw)
	}
	if points[0].Values["procs_running"].Raw != 8 {
		t.Fatalf("expected procs_running=8, got %v", points[0].Values["procs_running"].Raw)
	}
	if points[0].Values["procs_blocked"].Raw != 0 {
		t.Fatalf("expected procs_blocked=0, got %v", points[0].Values["procs_blocked"].Raw)
	}
}
