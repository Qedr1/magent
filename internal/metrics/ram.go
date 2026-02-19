package metrics

import (
	"context"
	"fmt"

	"github.com/shirou/gopsutil/v4/mem"
)

// RAMCollector scrapes RAM totals, used/free, and utilization.
// Params: metricName emitted into event.metric.
// Returns: RAM collector instance.
type RAMCollector struct {
	metricName string
}

// NewRAMCollector creates a RAM collector.
// Params: metricName emitted into event.metric.
// Returns: configured RAM collector.
func NewRAMCollector(metricName string) *RAMCollector {
	return &RAMCollector{metricName: metricName}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *RAMCollector) Name() string {
	return c.metricName
}

// Scrape reads RAM state from kernel and emits one `total` key.
// Params: ctx for cancellation.
// Returns: one RAM point or error.
func (c *RAMCollector) Scrape(ctx context.Context) ([]Point, error) {
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("read virtual memory: %w", err)
	}

	util := 0.0
	if vm.Total > 0 {
		util = (float64(vm.Used) / float64(vm.Total)) * 100
	}

	return []Point{
		{
			Key: "total",
			Values: map[string]Value{
				"total": {Raw: float64(vm.Total), Kind: KindNumber},
				"used":  {Raw: float64(vm.Used), Kind: KindNumber},
				"free":  {Raw: float64(vm.Available), Kind: KindNumber},
				"util":  {Raw: util, Kind: KindPercent},
			},
		},
	}, nil
}
