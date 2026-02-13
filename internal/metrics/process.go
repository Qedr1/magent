package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/mem"
	goprocess "github.com/shirou/gopsutil/v4/process"
)

type processSnapshot struct {
	at         time.Time
	readCount  uint64
	writeCount uint64
}

type processMeta struct {
	name string
}

// PROCESSCollector scrapes process CPU/RAM and IO operations rate.
// Params: metricName emitted into event.metric.
// Returns: PROCESS collector instance.
type PROCESSCollector struct {
	metricName string
	mu         sync.Mutex
	prev       map[int32]processSnapshot
	meta       map[int32]processMeta
	procs      map[int32]*goprocess.Process
}

// NewPROCESSCollector creates a PROCESS collector.
// Params: metricName emitted into event.metric.
// Returns: configured PROCESS collector.
func NewPROCESSCollector(metricName string) *PROCESSCollector {
	return &PROCESSCollector{
		metricName: metricName,
		prev:       make(map[int32]processSnapshot),
		meta:       make(map[int32]processMeta),
		procs:      make(map[int32]*goprocess.Process),
	}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *PROCESSCollector) Name() string {
	return c.metricName
}

// Scrape reads process metrics and derives IOPS per process.
// Params: ctx for cancellation.
// Returns: one point per process or error.
func (c *PROCESSCollector) Scrape(ctx context.Context) ([]Point, error) {
	pids, err := goprocess.PidsWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("read process list: %w", err)
	}

	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("read host memory for process metrics: %w", err)
	}

	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	nextPrev := make(map[int32]processSnapshot, len(pids))
	nextMeta := make(map[int32]processMeta, len(pids))
	nextProcs := make(map[int32]*goprocess.Process, len(pids))
	points := make([]Point, 0, len(pids))
	skipped := 0

	for _, pid := range pids {
		proc, exists := c.procs[pid]
		if !exists {
			proc, err = goprocess.NewProcessWithContext(ctx, pid)
			if err != nil {
				skipped++
				continue
			}
		}

		meta, exists := c.meta[pid]
		if !exists {
			name, nameErr := proc.NameWithContext(ctx)
			if nameErr != nil {
				skipped++
				continue
			}
			meta = processMeta{
				name: name,
			}
		}
		if meta.name == "" {
			skipped++
			continue
		}
		nextMeta[pid] = meta
		nextProcs[pid] = proc

		cpuUtil, cpuErr := proc.PercentWithContext(ctx, 0)
		if cpuErr != nil {
			skipped++
			continue
		}

		memInfo, memErr := proc.MemoryInfoWithContext(ctx)
		if memErr != nil {
			skipped++
			continue
		}

		ioStat, ioErr := proc.IOCountersWithContext(ctx)
		if ioErr != nil {
			ioStat = &goprocess.IOCountersStat{}
		}

		ramUtil := 0.0
		if vm.Total > 0 {
			ramUtil = (float64(memInfo.RSS) / float64(vm.Total)) * 100
		}

		iops := uint64(0)
		if prev, ok := c.prev[pid]; ok {
			seconds := now.Sub(prev.at).Seconds()
			if seconds > 0 {
				readDelta := positiveDelta(ioStat.ReadCount, prev.readCount)
				writeDelta := positiveDelta(ioStat.WriteCount, prev.writeCount)
				iops = ratePerSecond(readDelta+writeDelta, seconds)
			}
		}

		nextPrev[pid] = processSnapshot{
			at:         now,
			readCount:  ioStat.ReadCount,
			writeCount: ioStat.WriteCount,
		}

		points = append(points, Point{
			Key: meta.name,
			Values: map[string]Value{
				"cpu_util": {Raw: cpuUtil, Kind: KindPercent},
				"ram_util": {Raw: ramUtil, Kind: KindPercent},
				"iops":     {Raw: float64(iops), Kind: KindNumber},
			},
		})
	}

	c.prev = nextPrev
	c.meta = nextMeta
	c.procs = nextProcs

	if len(points) == 0 && skipped > 0 {
		return nil, fmt.Errorf("all process scrapes failed")
	}

	return points, nil
}
