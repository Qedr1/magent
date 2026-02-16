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
	readIO     func(context.Context, ...string) (map[string]disk.IOCountersStat, error)
	now        func() time.Time
}

// NewDISKCollector creates a DISK collector.
// Params: metricName emitted into event.metric.
// Returns: configured DISK collector.
func NewDISKCollector(metricName string) *DISKCollector {
	return &DISKCollector{
		metricName: metricName,
		readIO:     disk.IOCountersWithContext,
		now:        time.Now,
	}
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
	readIO := c.readIO
	if readIO == nil {
		readIO = disk.IOCountersWithContext
	}
	stats, err := readIO(ctx)
	if err != nil {
		return nil, fmt.Errorf("read disk counters: %w", err)
	}

	nowFn := c.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()

	c.mu.Lock()
	defer c.mu.Unlock()

	seconds := now.Sub(c.prev.at).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	elapsedMS := seconds * 1000

	points := make([]Point, 0, len(stats))
	current := make(map[string]disk.IOCountersStat, len(stats))

	for name, stat := range stats {
		if !isBaseDiskDevice(name) {
			continue
		}

		current[name] = stat
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
				"rx_io":            {Raw: float64(ratePerSecond(readCountDelta, seconds)), Kind: KindNumber},
				"tx_io":            {Raw: float64(ratePerSecond(writeCountDelta, seconds)), Kind: KindNumber},
				"rx_bytes":         {Raw: float64(readBytesDelta), Kind: KindNumber},
				"tx_bytes":         {Raw: float64(writeBytesDelta), Kind: KindNumber},
				"rx_bytes_per_sec": {Raw: float64(ratePerSecond(readBytesDelta, seconds)), Kind: KindNumber},
				"tx_bytes_per_sec": {Raw: float64(ratePerSecond(writeBytesDelta, seconds)), Kind: KindNumber},
				"rx_await":         {Raw: rxAwait, Kind: KindNumber},
				"tx_await":         {Raw: txAwait, Kind: KindNumber},
				"await":            {Raw: await, Kind: KindNumber},
				"qdepth":           {Raw: qdepth, Kind: KindNumber},
				"util":             {Raw: util, Kind: KindPercent},
				"inflight":         {Raw: float64(stat.IopsInProgress), Kind: KindNumber},
			},
		})
	}

	c.prev = diskSnapshot{
		at:       now,
		counters: current,
	}

	return points, nil
}

// isBaseDiskDevice returns true for top-level block devices and false for partitions.
// Params: device name from gopsutil, with or without `/dev/` prefix.
// Returns: true when device should be reported by DISK metric.
func isBaseDiskDevice(name string) bool {
	device := normalizeDeviceName(name)
	if device == "" {
		return false
	}

	if matched, base := matchLetterDisk(device, "sd"); matched {
		return base
	}
	if matched, base := matchLetterDisk(device, "vd"); matched {
		return base
	}
	if matched, base := matchLetterDisk(device, "xvd"); matched {
		return base
	}
	if matched, base := matchLetterDisk(device, "hd"); matched {
		return base
	}
	if matched, base := matchNVMeDisk(device); matched {
		return base
	}
	if matched, base := matchMMCBLKDisk(device); matched {
		return base
	}

	if strings.HasPrefix(device, "loop") && isDigits(device[len("loop"):]) {
		return false
	}
	if strings.HasPrefix(device, "ram") && isDigits(device[len("ram"):]) {
		return false
	}

	if strings.HasPrefix(device, "dm-") && isDigits(device[len("dm-"):]) {
		return true
	}
	if strings.HasPrefix(device, "md") && isDigits(device[len("md"):]) {
		return true
	}
	if strings.HasPrefix(device, "zd") && isDigits(device[len("zd"):]) {
		return true
	}

	// Keep unknown names to avoid dropping valid devices on non-standard kernels.
	return true
}

// normalizeDeviceName trims spaces and optional /dev/ prefix.
// Params: raw device name.
// Returns: normalized short device name.
func normalizeDeviceName(name string) string {
	device := strings.TrimSpace(name)
	if strings.HasPrefix(device, "/dev/") {
		device = strings.TrimPrefix(device, "/dev/")
	}
	return device
}

// matchLetterDisk matches sd/vd/xvd/hd style devices with optional numeric partition suffix.
// Params: normalized device name, family prefix.
// Returns: matched family flag and base-disk decision.
func matchLetterDisk(device, prefix string) (bool, bool) {
	if !strings.HasPrefix(device, prefix) {
		return false, false
	}

	rest := device[len(prefix):]
	if rest == "" {
		return false, false
	}

	letters := 0
	for letters < len(rest) {
		ch := rest[letters]
		if ch < 'a' || ch > 'z' {
			break
		}
		letters++
	}
	if letters == 0 {
		return false, false
	}
	if letters == len(rest) {
		return true, true
	}
	if isDigits(rest[letters:]) {
		return true, false
	}
	return true, true
}

// matchNVMeDisk matches nvmeNnM and nvmeNnMpP names.
// Params: normalized device name.
// Returns: matched family flag and base-disk decision.
func matchNVMeDisk(device string) (bool, bool) {
	if !strings.HasPrefix(device, "nvme") {
		return false, false
	}

	rest := device[len("nvme"):]
	n := consumeDigits(rest)
	if n == 0 {
		return false, false
	}
	rest = rest[n:]
	if !strings.HasPrefix(rest, "n") {
		return false, false
	}
	rest = rest[1:]
	n = consumeDigits(rest)
	if n == 0 {
		return false, false
	}
	rest = rest[n:]
	if rest == "" {
		return true, true
	}
	if strings.HasPrefix(rest, "p") && isDigits(rest[1:]) {
		return true, false
	}
	return true, true
}

// matchMMCBLKDisk matches mmcblkN and mmcblkNpP names.
// Params: normalized device name.
// Returns: matched family flag and base-disk decision.
func matchMMCBLKDisk(device string) (bool, bool) {
	if !strings.HasPrefix(device, "mmcblk") {
		return false, false
	}

	rest := device[len("mmcblk"):]
	n := consumeDigits(rest)
	if n == 0 {
		return false, false
	}
	rest = rest[n:]
	if rest == "" {
		return true, true
	}
	if strings.HasPrefix(rest, "p") && isDigits(rest[1:]) {
		return true, false
	}
	return true, true
}

// consumeDigits returns the leading decimal digit run length.
// Params: source string.
// Returns: leading digits count.
func consumeDigits(value string) int {
	index := 0
	for index < len(value) {
		ch := value[index]
		if ch < '0' || ch > '9' {
			break
		}
		index++
	}
	return index
}

// isDigits checks that value is a non-empty decimal number.
// Params: string to validate.
// Returns: true if value contains only digits and is not empty.
func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for idx := 0; idx < len(value); idx++ {
		ch := value[idx]
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
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
