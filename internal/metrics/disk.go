package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
)

type diskSnapshot struct {
	at       time.Time
	counters map[string]disk.IOCountersStat
}

// DISKCollector scrapes block device IO counters and derives rates/latency.
// Params: metricName emitted into event.metric.
// Returns: DISK collector instance.
type DISKCollector struct {
	metricName string
	mu         sync.Mutex
	prev       diskSnapshot
}

// NewDISKCollector creates a DISK collector.
// Params: metricName emitted into event.metric.
// Returns: configured DISK collector.
func NewDISKCollector(metricName string) *DISKCollector {
	return &DISKCollector{metricName: metricName}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *DISKCollector) Name() string {
	return c.metricName
}

// Scrape reads per-device IO counters and computes per-second values.
// Params: ctx for cancellation.
// Returns: one point per block device or error.
func (c *DISKCollector) Scrape(ctx context.Context) ([]Point, error) {
	stats, err := disk.IOCountersWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("read disk counters: %w", err)
	}

	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	seconds := now.Sub(c.prev.at).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	elapsedMS := seconds * 1000

	points := make([]Point, 0, len(stats))

	for name, stat := range stats {
		key := devicePath(name)
		prev, hasPrev := c.prev.counters[name]

		readCountDelta := uint64(0)
		writeCountDelta := uint64(0)
		readBytesDelta := uint64(0)
		writeBytesDelta := uint64(0)
		readTimeDelta := uint64(0)
		writeTimeDelta := uint64(0)
		ioTimeDelta := uint64(0)
		weightedIODelta := uint64(0)

		if hasPrev {
			readCountDelta = positiveDelta(stat.ReadCount, prev.ReadCount)
			writeCountDelta = positiveDelta(stat.WriteCount, prev.WriteCount)
			readBytesDelta = positiveDelta(stat.ReadBytes, prev.ReadBytes)
			writeBytesDelta = positiveDelta(stat.WriteBytes, prev.WriteBytes)
			readTimeDelta = positiveDelta(stat.ReadTime, prev.ReadTime)
			writeTimeDelta = positiveDelta(stat.WriteTime, prev.WriteTime)
			ioTimeDelta = positiveDelta(stat.IoTime, prev.IoTime)
			weightedIODelta = positiveDelta(stat.WeightedIO, prev.WeightedIO)
		}

		rxAwait := averageOrZero(readTimeDelta, readCountDelta)
		txAwait := averageOrZero(writeTimeDelta, writeCountDelta)
		await := averageOrZero(readTimeDelta+writeTimeDelta, readCountDelta+writeCountDelta)
		qdepth := 0.0
		if elapsedMS > 0 {
			qdepth = float64(weightedIODelta) / elapsedMS
		}
		util := 0.0
		if elapsedMS > 0 {
			util = (float64(ioTimeDelta) / elapsedMS) * 100
		}

		points = append(points, Point{
			Key: key,
			Values: map[string]Value{
				"rx_io":    {Raw: float64(ratePerSecond(readCountDelta, seconds)), Kind: KindNumber},
				"tx_io":    {Raw: float64(ratePerSecond(writeCountDelta, seconds)), Kind: KindNumber},
				"rx_b":     {Raw: float64(ratePerSecond(readBytesDelta, seconds)), Kind: KindNumber},
				"tx_b":     {Raw: float64(ratePerSecond(writeBytesDelta, seconds)), Kind: KindNumber},
				"rx_await": {Raw: rxAwait, Kind: KindNumber},
				"tx_await": {Raw: txAwait, Kind: KindNumber},
				"await":    {Raw: await, Kind: KindNumber},
				"qdepth":   {Raw: qdepth, Kind: KindNumber},
				"util":     {Raw: util, Kind: KindPercent},
				"inflight": {Raw: float64(stat.IopsInProgress), Kind: KindNumber},
			},
		})
	}

	c.prev = diskSnapshot{
		at:       now,
		counters: make(map[string]disk.IOCountersStat, len(stats)),
	}
	for name, stat := range stats {
		c.prev.counters[name] = stat
	}

	return points, nil
}

// devicePath builds canonical block device key path.
// Params: device name from gopsutil disk stats.
// Returns: normalized /dev path.
func devicePath(name string) string {
	value := strings.TrimSpace(name)
	if value == "" {
		return "/dev/unknown"
	}
	if strings.HasPrefix(value, "/") {
		return value
	}
	return "/dev/" + value
}

// averageOrZero calculates numerator/denominator or zero.
// Params: numerator total; denominator count.
// Returns: average value or zero on empty denominator.
func averageOrZero(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}
