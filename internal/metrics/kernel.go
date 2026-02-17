package metrics

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type kernelLoadAverages struct {
	load1  float64
	load5  float64
	load15 float64
}

type kernelCounters struct {
	ctxt         uint64
	intr         uint64
	softirq      uint64
	forks        uint64
	procsRunning uint64
	procsBlocked uint64
}

type kernelSnapshot struct {
	at       time.Time
	counters kernelCounters
}

// KERNELCollector scrapes core Linux kernel counters from procfs.
// Params: metricName emitted into event.metric.
// Returns: KERNEL collector instance.
type KERNELCollector struct {
	metricName string
	readFile   func(string) ([]byte, error)

	mu   sync.Mutex
	prev kernelSnapshot
}

// NewKERNELCollector creates a KERNEL collector.
// Params: metricName emitted into event.metric.
// Returns: configured KERNEL collector.
func NewKERNELCollector(metricName string) *KERNELCollector {
	return &KERNELCollector{
		metricName: metricName,
		readFile:   os.ReadFile,
	}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *KERNELCollector) Name() string {
	return c.metricName
}

// Scrape reads kernel load/process state and per-second counter deltas.
// Params: ctx for cancellation.
// Returns: single keyed point with kernel values or scrape error.
func (c *KERNELCollector) Scrape(ctx context.Context) ([]Point, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	loadPayload, err := c.readFile("/proc/loadavg")
	if err != nil {
		return nil, fmt.Errorf("read /proc/loadavg: %w", err)
	}
	loadValues, err := parseKernelLoadAverages(loadPayload)
	if err != nil {
		return nil, fmt.Errorf("parse /proc/loadavg: %w", err)
	}

	statPayload, err := c.readFile("/proc/stat")
	if err != nil {
		return nil, fmt.Errorf("read /proc/stat: %w", err)
	}
	currentCounters, err := parseKernelCounters(statPayload)
	if err != nil {
		return nil, fmt.Errorf("parse /proc/stat: %w", err)
	}

	now := time.Now()
	ctxtPerSec := uint64(0)
	intrPerSec := uint64(0)
	softirqPerSec := uint64(0)
	forksPerSec := uint64(0)

	c.mu.Lock()
	seconds := now.Sub(c.prev.at).Seconds()
	if !c.prev.at.IsZero() {
		ctxtPerSec = ratePerSecond(positiveDelta(currentCounters.ctxt, c.prev.counters.ctxt), seconds)
		intrPerSec = ratePerSecond(positiveDelta(currentCounters.intr, c.prev.counters.intr), seconds)
		softirqPerSec = ratePerSecond(positiveDelta(currentCounters.softirq, c.prev.counters.softirq), seconds)
		forksPerSec = ratePerSecond(positiveDelta(currentCounters.forks, c.prev.counters.forks), seconds)
	}
	c.prev = kernelSnapshot{
		at:       now,
		counters: currentCounters,
	}
	c.mu.Unlock()

	point := Point{
		Key: "total",
		Values: map[string]Value{
			"load1":         {Raw: loadValues.load1, Kind: KindNumber},
			"load5":         {Raw: loadValues.load5, Kind: KindNumber},
			"load15":        {Raw: loadValues.load15, Kind: KindNumber},
			"procs_running": {Raw: float64(currentCounters.procsRunning), Kind: KindNumber},
			"procs_blocked": {Raw: float64(currentCounters.procsBlocked), Kind: KindNumber},
			"ctxt_per_sec":  {Raw: float64(ctxtPerSec), Kind: KindNumber},
			"intr_per_sec":  {Raw: float64(intrPerSec), Kind: KindNumber},
			"softirq_per_sec": {
				Raw:  float64(softirqPerSec),
				Kind: KindNumber,
			},
			"forks_per_sec": {Raw: float64(forksPerSec), Kind: KindNumber},
		},
	}

	return []Point{point}, nil
}

// parseKernelLoadAverages parses first three load-average values from /proc/loadavg.
// Params: payload is /proc/loadavg file body.
// Returns: parsed load averages or parse error.
func parseKernelLoadAverages(payload []byte) (kernelLoadAverages, error) {
	fields := strings.Fields(string(payload))
	if len(fields) < 3 {
		return kernelLoadAverages{}, fmt.Errorf("expected at least 3 fields")
	}

	load1, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return kernelLoadAverages{}, fmt.Errorf("parse load1: %w", err)
	}
	load5, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return kernelLoadAverages{}, fmt.Errorf("parse load5: %w", err)
	}
	load15, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return kernelLoadAverages{}, fmt.Errorf("parse load15: %w", err)
	}

	return kernelLoadAverages{
		load1:  load1,
		load5:  load5,
		load15: load15,
	}, nil
}

// parseKernelCounters parses required counters from /proc/stat.
// Params: payload is /proc/stat file body.
// Returns: parsed kernel counters or parse error.
func parseKernelCounters(payload []byte) (kernelCounters, error) {
	scanner := bufio.NewScanner(bytes.NewReader(payload))

	counters := kernelCounters{}
	seenCtxt := false
	seenIntr := false
	seenSoftIRQ := false
	seenForks := false
	seenProcsRunning := false
	seenProcsBlocked := false

	for scanner.Scan() {
		fields := strings.Fields(strings.TrimSpace(scanner.Text()))
		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "ctxt":
			value, err := parseKernelUintField(fields, 1, "ctxt")
			if err != nil {
				return kernelCounters{}, err
			}
			counters.ctxt = value
			seenCtxt = true
		case "intr":
			value, err := parseKernelUintField(fields, 1, "intr")
			if err != nil {
				return kernelCounters{}, err
			}
			counters.intr = value
			seenIntr = true
		case "softirq":
			value, err := parseKernelUintField(fields, 1, "softirq")
			if err != nil {
				return kernelCounters{}, err
			}
			counters.softirq = value
			seenSoftIRQ = true
		case "processes":
			value, err := parseKernelUintField(fields, 1, "processes")
			if err != nil {
				return kernelCounters{}, err
			}
			counters.forks = value
			seenForks = true
		case "procs_running":
			value, err := parseKernelUintField(fields, 1, "procs_running")
			if err != nil {
				return kernelCounters{}, err
			}
			counters.procsRunning = value
			seenProcsRunning = true
		case "procs_blocked":
			value, err := parseKernelUintField(fields, 1, "procs_blocked")
			if err != nil {
				return kernelCounters{}, err
			}
			counters.procsBlocked = value
			seenProcsBlocked = true
		}
	}
	if err := scanner.Err(); err != nil {
		return kernelCounters{}, fmt.Errorf("scan /proc/stat: %w", err)
	}

	if !seenCtxt {
		return kernelCounters{}, fmt.Errorf("missing ctxt field")
	}
	if !seenIntr {
		return kernelCounters{}, fmt.Errorf("missing intr field")
	}
	if !seenSoftIRQ {
		return kernelCounters{}, fmt.Errorf("missing softirq field")
	}
	if !seenForks {
		return kernelCounters{}, fmt.Errorf("missing processes field")
	}
	if !seenProcsRunning {
		return kernelCounters{}, fmt.Errorf("missing procs_running field")
	}
	if !seenProcsBlocked {
		return kernelCounters{}, fmt.Errorf("missing procs_blocked field")
	}

	return counters, nil
}

// parseKernelUintField parses one uint64 field from tokenized /proc/stat line.
// Params: fields tokenized line, index numeric token index, and field label for errors.
// Returns: parsed uint64 value or parse error.
func parseKernelUintField(fields []string, index int, fieldName string) (uint64, error) {
	if len(fields) <= index {
		return 0, fmt.Errorf("%s field has no value", fieldName)
	}

	value, err := strconv.ParseUint(fields[index], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", fieldName, err)
	}

	return value, nil
}
