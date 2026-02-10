package metrics

import (
	"context"
	"fmt"

	"github.com/shirou/gopsutil/v4/cpu"
)

// CPUCollector scrapes CPU total and per-core utilization.
// Params: metricName emitted into event.metric.
// Returns: CPU collector instance.
type CPUCollector struct {
	metricName string
}

// NewCPUCollector creates a CPU collector.
// Params: metricName emitted into event.metric.
// Returns: configured CPU collector.
func NewCPUCollector(metricName string) *CPUCollector {
	return &CPUCollector{metricName: metricName}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *CPUCollector) Name() string {
	return c.metricName
}

// Scrape reads CPU utilization for total and each core.
// Params: ctx for cancellation.
// Returns: keyed CPU points or error.
func (c *CPUCollector) Scrape(ctx context.Context) ([]Point, error) {
	total, err := cpu.PercentWithContext(ctx, 0, false)
	if err != nil {
		return nil, fmt.Errorf("read total CPU percent: %w", err)
	}

	perCore, err := cpu.PercentWithContext(ctx, 0, true)
	if err != nil {
		return nil, fmt.Errorf("read per-core CPU percent: %w", err)
	}

	points := make([]Point, 0, len(perCore)+1)

	if len(total) > 0 {
		points = append(points, Point{
			Key: "total",
			Values: map[string]Value{
				"util": {Raw: total[0], Kind: KindPercent},
			},
		})
	}

	for idx, util := range perCore {
		points = append(points, Point{
			Key: fmt.Sprintf("core%d", idx),
			Values: map[string]Value{
				"util": {Raw: util, Kind: KindPercent},
			},
		})
	}

	return points, nil
}
