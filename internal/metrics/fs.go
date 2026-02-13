package metrics

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/shirou/gopsutil/v4/disk"
)

// FSCollector scrapes filesystem usage and inode usage.
// Params: metricName emitted into event.metric.
// Returns: FS collector instance.
type FSCollector struct {
	metricName string
}

// NewFSCollector creates an FS collector.
// Params: metricName emitted into event.metric.
// Returns: configured FS collector.
func NewFSCollector(metricName string) *FSCollector {
	return &FSCollector{metricName: metricName}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *FSCollector) Name() string {
	return c.metricName
}

// Scrape reads mounted filesystems and usage stats.
// Params: ctx for cancellation.
// Returns: one point per filesystem or error.
func (c *FSCollector) Scrape(ctx context.Context) ([]Point, error) {
	partitions, err := disk.PartitionsWithContext(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	points := make([]Point, 0, len(partitions))
	skipped := 0

	for _, part := range partitions {
		mpoint := strings.TrimSpace(part.Mountpoint)
		if mpoint == "" {
			skipped++
			continue
		}

		usage, usageErr := disk.UsageWithContext(ctx, mpoint)
		if usageErr != nil {
			skipped++
			continue
		}

		inodesUtil := usage.InodesUsedPercent
		if math.IsNaN(inodesUtil) || math.IsInf(inodesUtil, 0) {
			inodesUtil = 0
		}

		points = append(points, Point{
			Key: mpoint,
			Values: map[string]Value{
				"total":        {Raw: float64(usage.Total), Kind: KindNumber},
				"used":         {Raw: float64(usage.Used), Kind: KindNumber},
				"free":         {Raw: float64(usage.Free), Kind: KindNumber},
				"avail":        {Raw: float64(usage.Free), Kind: KindNumber},
				"util":         {Raw: usage.UsedPercent, Kind: KindPercent},
				"inodes_total": {Raw: float64(usage.InodesTotal), Kind: KindNumber},
				"inodes_used":  {Raw: float64(usage.InodesUsed), Kind: KindNumber},
				"inodes_free":  {Raw: float64(usage.InodesFree), Kind: KindNumber},
				"inodes_util":  {Raw: inodesUtil, Kind: KindPercent},
				"readonly":     {Raw: readonlyValue(part.Opts), Kind: KindNumber},
			},
		})
	}

	if len(points) == 0 && skipped > 0 {
		return nil, fmt.Errorf("all filesystem usage reads failed")
	}

	return points, nil
}

// readonlyValue maps partition mount options to readonly flag (0/1).
// Params: opts mount option list from partition info.
// Returns: 1 for read-only, 0 otherwise.
func readonlyValue(opts []string) float64 {
	for _, option := range opts {
		if strings.EqualFold(strings.TrimSpace(option), "ro") {
			return 1
		}
	}
	return 0
}
