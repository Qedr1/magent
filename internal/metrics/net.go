package metrics

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	netio "github.com/shirou/gopsutil/v4/net"
)

const (
	procSNMPPath      = "/proc/net/snmp"
	procNetstatPath   = "/proc/net/netstat"
	procTCPPath       = "/proc/net/tcp"
	procTCP6Path      = "/proc/net/tcp6"
	procTCPCCPath     = "/proc/sys/net/ipv4/tcp_congestion_control"
	tcpListenStateHex = "0A"
)

type netStackCounters struct {
	tcpActiveOpens uint64
	tcpPassiveOpen uint64
	tcpRetransSegs uint64
	tcpTimeouts    uint64
	tcpOutRsts     uint64

	udpInDatagrams uint64
	udpOutDgrams   uint64
	udpInErrors    uint64
	udpNoPorts     uint64
	udpRcvbufErrs  uint64
	udpSndbufErrs  uint64
}

type tcpSocketSample struct {
	txQueueBytes uint64
	rxQueueBytes uint64
	retransmits  uint64
	cwndSegs     uint64
}

type netSnapshot struct {
	at       time.Time
	counters map[string]netio.IOCountersStat
	stack    netStackCounters
}

// NETCollector scrapes per-interface traffic counters and host-level TCP/UDP stack counters.
// Params: metricName emitted into event.metric and tcpCCTopN controls by-CC socket sampling (0 disables).
// Returns: NET collector instance.
type NETCollector struct {
	metricName string
	tcpCCTopN  int

	readFile       func(string) ([]byte, error)
	readIOCounters func(context.Context, bool) ([]netio.IOCountersStat, error)

	mu   sync.Mutex
	prev netSnapshot
}

// NewNETCollector creates a NET collector.
// Params: metricName emitted into event.metric and tcpCCTopN controls by-CC socket sampling (0 disables).
// Returns: configured NET collector.
func NewNETCollector(metricName string, tcpCCTopN uint32) *NETCollector {
	limit := int(tcpCCTopN)

	return &NETCollector{
		metricName:     metricName,
		tcpCCTopN:      limit,
		readFile:       os.ReadFile,
		readIOCounters: netio.IOCountersWithContext,
	}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *NETCollector) Name() string {
	return c.metricName
}

// Scrape reads interface counters, stack counters, and optional by-CC socket aggregates.
// Params: ctx for cancellation.
// Returns: points for interfaces, key=total stack counters, and optional key=cc:<algo>.
func (c *NETCollector) Scrape(ctx context.Context) ([]Point, error) {
	interfaceStats, err := c.readIOCounters(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("read net counters: %w", err)
	}

	stackCounters, err := c.readStackCounters()
	if err != nil {
		return nil, err
	}

	ccPoint, err := c.readTCPCCPoint()
	if err != nil {
		return nil, err
	}

	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	seconds := now.Sub(c.prev.at).Seconds()
	if seconds <= 0 {
		seconds = 0
	}

	points := make([]Point, 0, len(interfaceStats)+2)
	for _, stat := range interfaceStats {
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
				"tx_bytes":         {Raw: float64(deltaSent), Kind: KindNumber},
				"rx_bytes":         {Raw: float64(deltaRecv), Kind: KindNumber},
				"tx_bytes_per_sec": {Raw: float64(txRate), Kind: KindNumber},
				"rx_bytes_per_sec": {Raw: float64(rxRate), Kind: KindNumber},
				"tx_pkt":           {Raw: float64(txPktRate), Kind: KindNumber},
				"rx_pkt":           {Raw: float64(rxPktRate), Kind: KindNumber},
				"rx_err":           {Raw: float64(deltaRecvErr), Kind: KindNumber},
				"tx_err":           {Raw: float64(deltaSentErr), Kind: KindNumber},
				"rx_drop":          {Raw: float64(deltaRecvDrop), Kind: KindNumber},
				"tx_drop":          {Raw: float64(deltaSentDrop), Kind: KindNumber},
			},
		})
	}

	stackPoint := c.buildStackPoint(stackCounters)
	points = append(points, stackPoint)

	if ccPoint != nil {
		points = append(points, *ccPoint)
	}

	c.prev = netSnapshot{
		at:       now,
		counters: make(map[string]netio.IOCountersStat, len(interfaceStats)),
		stack:    stackCounters,
	}
	for _, stat := range interfaceStats {
		c.prev.counters[stat.Name] = stat
	}

	return points, nil
}

// readStackCounters reads host-level TCP/UDP counters from procfs.
// Params: none.
// Returns: parsed stack counters or read/parse error.
func (c *NETCollector) readStackCounters() (netStackCounters, error) {
	snmpPayload, err := c.readFile(procSNMPPath)
	if err != nil {
		return netStackCounters{}, fmt.Errorf("read %s: %w", procSNMPPath, err)
	}
	netstatPayload, err := c.readFile(procNetstatPath)
	if err != nil {
		return netStackCounters{}, fmt.Errorf("read %s: %w", procNetstatPath, err)
	}

	snmpSections, err := parseProcCounterSections(snmpPayload)
	if err != nil {
		return netStackCounters{}, fmt.Errorf("parse %s: %w", procSNMPPath, err)
	}
	netstatSections, err := parseProcCounterSections(netstatPayload)
	if err != nil {
		return netStackCounters{}, fmt.Errorf("parse %s: %w", procNetstatPath, err)
	}

	return netStackCounters{
		tcpActiveOpens: counterOrZero(snmpSections, "Tcp", "ActiveOpens"),
		tcpPassiveOpen: counterOrZero(snmpSections, "Tcp", "PassiveOpens"),
		tcpRetransSegs: counterOrZero(snmpSections, "Tcp", "RetransSegs"),
		tcpOutRsts:     counterOrZero(snmpSections, "Tcp", "OutRsts"),
		tcpTimeouts:    counterOrZero(netstatSections, "TcpExt", "TCPTimeouts"),

		udpInDatagrams: counterOrZero(snmpSections, "Udp", "InDatagrams"),
		udpOutDgrams:   counterOrZero(snmpSections, "Udp", "OutDatagrams"),
		udpInErrors:    counterOrZero(snmpSections, "Udp", "InErrors"),
		udpNoPorts:     counterOrZero(snmpSections, "Udp", "NoPorts"),
		udpRcvbufErrs:  counterOrZero(snmpSections, "Udp", "RcvbufErrors"),
		udpSndbufErrs:  counterOrZero(snmpSections, "Udp", "SndbufErrors"),
	}, nil
}

// readTCPCCPoint reads socket snapshots and emits one by-CC point (global algorithm on host).
// Params: none.
// Returns: cc point when enabled, nil when disabled, or error.
func (c *NETCollector) readTCPCCPoint() (*Point, error) {
	if c.tcpCCTopN == 0 {
		return nil, nil
	}

	samples, err := collectTCPSocketSamples(c.readFile, c.tcpCCTopN)
	if err != nil {
		return nil, err
	}

	algoPayload, err := c.readFile(procTCPCCPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", procTCPCCPath, err)
	}
	algo := strings.TrimSpace(string(algoPayload))
	if algo == "" {
		algo = "unknown"
	}

	sockets := uint64(len(samples))
	txQueueSum := uint64(0)
	rxQueueSum := uint64(0)
	retransSum := uint64(0)
	cwndSum := uint64(0)
	for _, sample := range samples {
		txQueueSum += sample.txQueueBytes
		rxQueueSum += sample.rxQueueBytes
		retransSum += sample.retransmits
		cwndSum += sample.cwndSegs
	}

	avgCwnd := uint64(0)
	if sockets > 0 {
		avgCwnd = cwndSum / sockets
	}

	point := Point{
		Key: "cc:" + algo,
		Values: map[string]Value{
			"tcp_sockets":         {Raw: float64(sockets), Kind: KindNumber},
			"tcp_tx_queue_bytes":  {Raw: float64(txQueueSum), Kind: KindNumber},
			"tcp_rx_queue_bytes":  {Raw: float64(rxQueueSum), Kind: KindNumber},
			"tcp_retrans_pending": {Raw: float64(retransSum), Kind: KindNumber},
			"tcp_cwnd_segs":       {Raw: float64(avgCwnd), Kind: KindNumber},
		},
	}

	return &point, nil
}

// buildStackPoint converts cumulative stack counters into per-scrape deltas.
// Params: current stack counters from procfs.
// Returns: key=total point with tcp_*/udp_* values.
func (c *NETCollector) buildStackPoint(current netStackCounters) Point {
	previous := c.prev.stack
	hasPrev := !c.prev.at.IsZero()

	tcpActiveOpens := uint64(0)
	tcpPassiveOpens := uint64(0)
	tcpRetransSegs := uint64(0)
	tcpTimeouts := uint64(0)
	tcpOutRsts := uint64(0)
	udpInDatagrams := uint64(0)
	udpOutDatagrams := uint64(0)
	udpInErrors := uint64(0)
	udpNoPorts := uint64(0)
	udpRcvbufErrors := uint64(0)
	udpSndbufErrors := uint64(0)

	if hasPrev {
		tcpActiveOpens = positiveDelta(current.tcpActiveOpens, previous.tcpActiveOpens)
		tcpPassiveOpens = positiveDelta(current.tcpPassiveOpen, previous.tcpPassiveOpen)
		tcpRetransSegs = positiveDelta(current.tcpRetransSegs, previous.tcpRetransSegs)
		tcpTimeouts = positiveDelta(current.tcpTimeouts, previous.tcpTimeouts)
		tcpOutRsts = positiveDelta(current.tcpOutRsts, previous.tcpOutRsts)
		udpInDatagrams = positiveDelta(current.udpInDatagrams, previous.udpInDatagrams)
		udpOutDatagrams = positiveDelta(current.udpOutDgrams, previous.udpOutDgrams)
		udpInErrors = positiveDelta(current.udpInErrors, previous.udpInErrors)
		udpNoPorts = positiveDelta(current.udpNoPorts, previous.udpNoPorts)
		udpRcvbufErrors = positiveDelta(current.udpRcvbufErrs, previous.udpRcvbufErrs)
		udpSndbufErrors = positiveDelta(current.udpSndbufErrs, previous.udpSndbufErrs)
	}

	return Point{
		Key: "total",
		Values: map[string]Value{
			"tcp_active_opens": {Raw: float64(tcpActiveOpens), Kind: KindNumber},
			"tcp_passive_opens": {
				Raw:  float64(tcpPassiveOpens),
				Kind: KindNumber,
			},
			"tcp_retrans_segs": {Raw: float64(tcpRetransSegs), Kind: KindNumber},
			"tcp_timeouts":     {Raw: float64(tcpTimeouts), Kind: KindNumber},
			"tcp_out_rsts":     {Raw: float64(tcpOutRsts), Kind: KindNumber},

			"udp_in_datagrams": {Raw: float64(udpInDatagrams), Kind: KindNumber},
			"udp_out_datagrams": {
				Raw:  float64(udpOutDatagrams),
				Kind: KindNumber,
			},
			"udp_in_errors":     {Raw: float64(udpInErrors), Kind: KindNumber},
			"udp_no_ports":      {Raw: float64(udpNoPorts), Kind: KindNumber},
			"udp_rcvbuf_errors": {Raw: float64(udpRcvbufErrors), Kind: KindNumber},
			"udp_sndbuf_errors": {Raw: float64(udpSndbufErrors), Kind: KindNumber},
		},
	}
}

// parseProcCounterSections parses two-line procfs counter sections (<name>: keys / <name>: values).
// Params: payload from /proc/net/snmp or /proc/net/netstat.
// Returns: section->field->value map or parse error.
func parseProcCounterSections(payload []byte) (map[string]map[string]uint64, error) {
	sections := make(map[string]map[string]uint64, 8)
	scanner := bufio.NewScanner(bytes.NewReader(payload))

	for scanner.Scan() {
		headerLine := strings.TrimSpace(scanner.Text())
		if headerLine == "" {
			continue
		}
		if !scanner.Scan() {
			break
		}
		valueLine := strings.TrimSpace(scanner.Text())
		if valueLine == "" {
			continue
		}

		headerFields := strings.Fields(headerLine)
		valueFields := strings.Fields(valueLine)
		if len(headerFields) < 2 || len(valueFields) < 2 {
			continue
		}

		sectionName := strings.TrimSuffix(headerFields[0], ":")
		valueSectionName := strings.TrimSuffix(valueFields[0], ":")
		if sectionName == "" || sectionName != valueSectionName {
			continue
		}

		fieldCount := len(headerFields) - 1
		if valuesCount := len(valueFields) - 1; valuesCount < fieldCount {
			fieldCount = valuesCount
		}
		if fieldCount <= 0 {
			continue
		}

		section := sections[sectionName]
		if section == nil {
			section = make(map[string]uint64, fieldCount)
			sections[sectionName] = section
		}

		for idx := 0; idx < fieldCount; idx++ {
			key := headerFields[idx+1]
			parsed, ok := parseProcCounterValue(valueFields[idx+1])
			if !ok {
				continue
			}
			section[key] = parsed
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan procfs counters: %w", err)
	}

	return sections, nil
}

// parseProcCounterValue parses one procfs numeric value and ignores negatives.
// Params: raw numeric token.
// Returns: parsed uint64 value and success flag.
func parseProcCounterValue(raw string) (uint64, bool) {
	if signed, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if signed < 0 {
			return 0, false
		}
		return uint64(signed), true
	}
	unsigned, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return unsigned, true
}

// counterOrZero returns one parsed counter or zero when key is missing.
// Params: parsed sections map and section/field lookup keys.
// Returns: counter value or 0.
func counterOrZero(sections map[string]map[string]uint64, section string, field string) uint64 {
	sectionMap := sections[section]
	if sectionMap == nil {
		return 0
	}
	return sectionMap[field]
}

// collectTCPSocketSamples reads /proc/net/tcp* sockets and returns top-N by queue pressure.
// Params: readFile procfs reader dependency and limit top-N sample count (0 means all).
// Returns: sampled sockets or parse error.
func collectTCPSocketSamples(readFile func(string) ([]byte, error), limit int) ([]tcpSocketSample, error) {
	tcpPayload, err := readFile(procTCPPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", procTCPPath, err)
	}
	tcp6Payload, err := readFile(procTCP6Path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", procTCP6Path, err)
	}

	samples4, err := parseTCPSocketTable(tcpPayload)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", procTCPPath, err)
	}
	samples6, err := parseTCPSocketTable(tcp6Payload)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", procTCP6Path, err)
	}

	samples := append(samples4, samples6...)
	if limit <= 0 || len(samples) <= limit {
		return samples, nil
	}

	sort.Slice(samples, func(i int, j int) bool {
		leftScore := samples[i].txQueueBytes + samples[i].rxQueueBytes
		rightScore := samples[j].txQueueBytes + samples[j].rxQueueBytes
		if leftScore == rightScore {
			if samples[i].retransmits == samples[j].retransmits {
				return samples[i].cwndSegs > samples[j].cwndSegs
			}
			return samples[i].retransmits > samples[j].retransmits
		}
		return leftScore > rightScore
	})
	return samples[:limit], nil
}

// parseTCPSocketTable parses one /proc/net/tcp* table into socket samples.
// Params: payload table content.
// Returns: parsed socket samples or parse error.
func parseTCPSocketTable(payload []byte) ([]tcpSocketSample, error) {
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	samples := make([]tcpSocketSample, 0, 64)
	firstLine := true

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if firstLine {
			firstLine = false
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue
		}
		if strings.EqualFold(fields[3], tcpListenStateHex) {
			continue
		}

		txQueue, rxQueue, ok := parseHexPair(fields[4])
		if !ok {
			continue
		}
		retransmits, ok := parseHexUint(fields[6])
		if !ok {
			continue
		}

		cwnd := uint64(0)
		if len(fields) >= 2 {
			parsedCWND, parsed := parseDecOrHexUint(fields[len(fields)-2])
			if parsed {
				cwnd = parsedCWND
			}
		}

		samples = append(samples, tcpSocketSample{
			txQueueBytes: txQueue,
			rxQueueBytes: rxQueue,
			retransmits:  retransmits,
			cwndSegs:     cwnd,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan tcp table: %w", err)
	}
	return samples, nil
}

// parseHexPair parses "<hex>:<hex>" token into two uint64 values.
// Params: raw token.
// Returns: left/right values and success flag.
func parseHexPair(raw string) (uint64, uint64, bool) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return 0, 0, false
	}
	left, ok := parseHexUint(parts[0])
	if !ok {
		return 0, 0, false
	}
	right, ok := parseHexUint(parts[1])
	if !ok {
		return 0, 0, false
	}
	return left, right, true
}

// parseHexUint parses hex uint token.
// Params: raw hex token.
// Returns: parsed value and success flag.
func parseHexUint(raw string) (uint64, bool) {
	parsed, err := strconv.ParseUint(raw, 16, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

// parseDecOrHexUint parses decimal first then hex fallback.
// Params: raw numeric token.
// Returns: parsed value and success flag.
func parseDecOrHexUint(raw string) (uint64, bool) {
	if value, err := strconv.ParseUint(raw, 10, 64); err == nil {
		return value, true
	}
	return parseHexUint(raw)
}

// positiveDelta returns non-negative monotonically increasing delta.
// Params: current counter and previous counter value.
// Returns: counter delta or 0 when counter reset is detected.
func positiveDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

// ratePerSecond converts delta over elapsed seconds into per-second rate.
// Params: delta value and elapsed seconds.
// Returns: per-second rate as uint64.
func ratePerSecond(delta uint64, seconds float64) uint64 {
	if seconds <= 0 {
		return 0
	}
	return uint64(float64(delta) / seconds)
}
