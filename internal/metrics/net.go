package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	netio "github.com/shirou/gopsutil/v4/net"
)

type netSnapshot struct {
	at       time.Time
	counters map[string]netio.IOCountersStat
}

// NETCollector scrapes per-interface traffic and error counters.
// Params: metricName emitted into event.metric.
// Returns: NET collector instance.
type NETCollector struct {
	metricName string
	mu         sync.Mutex
	prev       netSnapshot
}

// NewNETCollector creates a NET collector.
// Params: metricName emitted into event.metric.
// Returns: configured NET collector.
func NewNETCollector(metricName string) *NETCollector {
	return &NETCollector{metricName: metricName}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *NETCollector) Name() string {
	return c.metricName
}

// Scrape reads per-interface counters and emits rate/delta values.
// Params: ctx for cancellation.
// Returns: NET points per interface or error.
func (c *NETCollector) Scrape(ctx context.Context) ([]Point, error) {
	stats, err := netio.IOCountersWithContext(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("read net counters: %w", err)
	}

	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	points := make([]Point, 0, len(stats))
	seconds := now.Sub(c.prev.at).Seconds()
	if seconds <= 0 {
		seconds = 0
	}

	for _, stat := range stats {
		prev, hasPrev := c.prev.counters[stat.Name]

		deltaSent := uint64(0)
		deltaRecv := uint64(0)
		deltaSentPkt := uint64(0)
		deltaRecvPkt := uint64(0)
		deltaRecvErr := uint64(0)
		deltaSentErr := uint64(0)
		deltaRecvDrop := uint64(0)
		deltaSentDrop := uint64(0)

		if hasPrev {
			deltaSent = positiveDelta(stat.BytesSent, prev.BytesSent)
			deltaRecv = positiveDelta(stat.BytesRecv, prev.BytesRecv)
			deltaSentPkt = positiveDelta(stat.PacketsSent, prev.PacketsSent)
			deltaRecvPkt = positiveDelta(stat.PacketsRecv, prev.PacketsRecv)
			deltaRecvErr = positiveDelta(stat.Errin, prev.Errin)
			deltaSentErr = positiveDelta(stat.Errout, prev.Errout)
			deltaRecvDrop = positiveDelta(stat.Dropin, prev.Dropin)
			deltaSentDrop = positiveDelta(stat.Dropout, prev.Dropout)
		}

		txRate := ratePerSecond(deltaSent, seconds)
		rxRate := ratePerSecond(deltaRecv, seconds)
		txPktRate := ratePerSecond(deltaSentPkt, seconds)
		rxPktRate := ratePerSecond(deltaRecvPkt, seconds)

		points = append(points, Point{
			Key: stat.Name,
			Values: map[string]Value{
				"tx":      {Raw: float64(txRate), Kind: KindNumber},
				"rx":      {Raw: float64(rxRate), Kind: KindNumber},
				"tx_pkt":  {Raw: float64(txPktRate), Kind: KindNumber},
				"rx_pkt":  {Raw: float64(rxPktRate), Kind: KindNumber},
				"rx_err":  {Raw: float64(deltaRecvErr), Kind: KindNumber},
				"tx_err":  {Raw: float64(deltaSentErr), Kind: KindNumber},
				"rx_drop": {Raw: float64(deltaRecvDrop), Kind: KindNumber},
				"tx_drop": {Raw: float64(deltaSentDrop), Kind: KindNumber},
			},
		})
	}

	c.prev = netSnapshot{
		at:       now,
		counters: make(map[string]netio.IOCountersStat, len(stats)),
	}
	for _, stat := range stats {
		c.prev.counters[stat.Name] = stat
	}

	return points, nil
}

// positiveDelta returns non-negative monotonically increasing delta.
// Params: current counter, previous counter.
// Returns: counter delta or 0 when counter reset.
func positiveDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

// ratePerSecond converts delta over elapsed seconds into per-second rate.
// Params: delta value and elapsed seconds.
// Returns: rounded per-second rate as uint64.
func ratePerSecond(delta uint64, seconds float64) uint64 {
	if seconds <= 0 {
		return 0
	}
	return uint64(float64(delta) / seconds)
}
