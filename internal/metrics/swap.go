package metrics

import (
	"context"
	"fmt"

	"github.com/shirou/gopsutil/v4/mem"
)

// SWAPCollector scrapes swap totals, used bytes, and utilization.
// Params: metricName emitted into event.metric.
// Returns: SWAP collector instance.
type SWAPCollector struct {
	metricName string
}

// NewSWAPCollector creates a SWAP collector.
// Params: metricName emitted into event.metric.
// Returns: configured SWAP collector.
func NewSWAPCollector(metricName string) *SWAPCollector {
	return &SWAPCollector{metricName: metricName}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *SWAPCollector) Name() string {
	return c.metricName
}

// Scrape reads swap state and emits one `total` key.
// Params: ctx for cancellation.
// Returns: one SWAP point or error.
func (c *SWAPCollector) Scrape(ctx context.Context) ([]Point, error) {
	sm, err := mem.SwapMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("read swap memory: %w", err)
	}

	util := 0.0
	if sm.Total > 0 {
		util = (float64(sm.Used) / float64(sm.Total)) * 100
	}

	return []Point{
		{
			Key: "total",
			Values: map[string]Value{
				"total": {Raw: float64(sm.Total), Kind: KindNumber},
				"used":  {Raw: float64(sm.Used), Kind: KindNumber},
				"util":  {Raw: util, Kind: KindPercent},
			},
		},
	}, nil
}
