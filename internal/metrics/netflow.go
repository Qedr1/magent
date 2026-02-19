package metrics

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"magent/internal/match"

	"golang.org/x/sys/unix"
)

const (
	etherHeaderLen = 14

	etherTypeIPv4  = 0x0800
	etherTypeIPv6  = 0x86dd
	etherTypeVLAN  = 0x8100
	etherTypeQinQ  = 0x88a8
	etherTypeQinQ2 = 0x9100

	ipProtocolTCP = 6
	ipProtocolUDP = 17

	netflowCounterShards = 64
	defaultFlowIdleTTL   = 10 * time.Second
)

type flowTuple struct {
	iface string
	proto uint8

	srcAddr [16]byte
	dstAddr [16]byte
	srcV4   bool
	dstV4   bool

	srcPort uint16
	dstPort uint16
}

type flowCounter struct {
	bytes   uint64
	packets uint64
	flows   uint64
}

type flowCounterShard struct {
	mu       sync.Mutex
	counters map[flowTuple]flowCounter
	udpSeen  map[flowTuple]int64
}

type packetSource struct {
	iface string
	fd    int
}

// NETFLOWCollector captures raw packets via AF_PACKET and aggregates top flow tuples.
// Params: metricName emitted into event.metric, ifaceMasks wildcard interface selectors, topN emitted flow limit.
// Returns: configured NETFLOW collector.
type NETFLOWCollector struct {
	metricName    string
	ifaceMasks    []string
	ifacePatterns []match.WildcardPattern
	topN          int
	flowIdle      time.Duration

	mu      sync.Mutex
	started bool
	stopped bool
	sources map[string]*packetSource
	shards  []flowCounterShard
}

// NewNETFLOWCollector creates a built-in netflow collector.
// Params: metricName emitted into event.metric, ifaceMasks wildcard interface selectors, topN emitted flow limit.
// Returns: NETFLOW collector instance.
func NewNETFLOWCollector(metricName string, ifaceMasks []string, topN uint32, flowIdleTimeout time.Duration) *NETFLOWCollector {
	masks := make([]string, 0, len(ifaceMasks))
	patterns := make([]match.WildcardPattern, 0, len(ifaceMasks))
	for _, pattern := range ifaceMasks {
		trimmed := strings.TrimSpace(pattern)
		if trimmed == "" {
			continue
		}
		masks = append(masks, trimmed)
		if compiled, ok := match.CompileWildcard(trimmed); ok {
			patterns = append(patterns, compiled)
		}
	}

	limit := int(topN)
	if limit <= 0 {
		limit = 20
	}
	if flowIdleTimeout <= 0 {
		flowIdleTimeout = defaultFlowIdleTTL
	}

	shards := make([]flowCounterShard, netflowCounterShards)
	for idx := range shards {
		shards[idx].counters = make(map[flowTuple]flowCounter, 64)
		shards[idx].udpSeen = make(map[flowTuple]int64, 64)
	}

	return &NETFLOWCollector{
		metricName:    strings.TrimSpace(metricName),
		ifaceMasks:    masks,
		ifacePatterns: patterns,
		topN:          limit,
		flowIdle:      flowIdleTimeout,
		sources:       make(map[string]*packetSource),
		shards:        shards,
	}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *NETFLOWCollector) Name() string {
	return c.metricName
}

// Scrape snapshots counters accumulated since previous scrape and resets the interval.
// Params: ctx for startup lifecycle and shutdown hook.
// Returns: top-N flow points or error.
func (c *NETFLOWCollector) Scrape(ctx context.Context) ([]Point, error) {
	if err := c.ensureStarted(ctx); err != nil {
		return nil, err
	}

	if err := c.syncSources(); err != nil {
		return nil, err
	}

	snapshot := c.swapCounters()
	if len(snapshot) == 0 {
		return nil, nil
	}
	return buildFlowPoints(snapshot, c.topN), nil
}

// ensureStarted initializes lifecycle hooks once.
// Params: ctx worker lifecycle context.
// Returns: nil.
func (c *NETFLOWCollector) ensureStarted(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()

	go func() {
		<-ctx.Done()
		c.stop()
	}()

	return nil
}

// syncSources reconciles packet sockets for interfaces matching configured masks.
// Params: none.
// Returns: error when no matching/open interfaces exist.
func (c *NETFLOWCollector) syncSources() error {
	matched, err := resolveMatchingInterfaces(c.ifacePatterns, c.ifaceMasks)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return fmt.Errorf("collector stopped")
	}

	for ifaceName, source := range c.sources {
		if _, exists := matched[ifaceName]; exists {
			continue
		}
		_ = unix.Close(source.fd)
		delete(c.sources, ifaceName)
	}

	openErrs := make([]string, 0)
	for ifaceName, iface := range matched {
		if _, exists := c.sources[ifaceName]; exists {
			continue
		}
		source, openErr := openPacketSource(iface)
		if openErr != nil {
			openErrs = append(openErrs, fmt.Sprintf("%s: %v", ifaceName, openErr))
			continue
		}
		c.sources[ifaceName] = source
		go c.captureLoop(source)
	}

	if len(c.sources) == 0 {
		if len(openErrs) > 0 {
			return fmt.Errorf("no netflow sockets opened (%s)", strings.Join(openErrs, "; "))
		}
		return fmt.Errorf("no active netflow interfaces matched")
	}

	return nil
}

// captureLoop reads raw frames from one socket and updates in-memory counters.
// Params: source is one interface socket.
// Returns: none.
func (c *NETFLOWCollector) captureLoop(source *packetSource) {
	buffer := make([]byte, 65535)

	for {
		n, _, err := unix.Recvfrom(source.fd, buffer, 0)
		if err != nil {
			if err == unix.EINTR || err == unix.EAGAIN {
				continue
			}
			return
		}
		if n <= 0 {
			continue
		}

		parsed, ok := parseFlowTuple(source.iface, buffer[:n])
		if !ok {
			continue
		}

		now := time.Now().UnixNano()
		shard := c.counterShard(parsed.tuple)
		shard.mu.Lock()
		c.updateCounter(shard, parsed, now)
		shard.mu.Unlock()
	}
}

// stop closes all packet sockets and marks collector as stopped.
// Params: none.
// Returns: none.
func (c *NETFLOWCollector) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}
	c.stopped = true

	for ifaceName, source := range c.sources {
		_ = unix.Close(source.fd)
		delete(c.sources, ifaceName)
	}
}

// swapCounters atomically extracts and clears interval counters.
// Params: none.
// Returns: snapshot of counters since previous scrape.
func (c *NETFLOWCollector) swapCounters() map[flowTuple]flowCounter {
	snapshot := make(map[flowTuple]flowCounter)
	idleCutoff := time.Now().Add(-c.flowIdle).UnixNano()
	for idx := range c.shards {
		shard := &c.shards[idx]

		shard.mu.Lock()
		for tuple, seenAt := range shard.udpSeen {
			if seenAt < idleCutoff {
				delete(shard.udpSeen, tuple)
			}
		}
		if len(shard.counters) == 0 {
			shard.mu.Unlock()
			continue
		}

		for tuple, counter := range shard.counters {
			snapshot[tuple] = counter
		}
		shard.counters = make(map[flowTuple]flowCounter, len(shard.counters))
		shard.mu.Unlock()
	}

	return snapshot
}

// updateCounter updates one shard counter with packet and flow-start semantics.
// Params: shard target shard; parsed packet tuple/size/flags; observedAt is packet receive time in unix nanos.
// Returns: none.
func (c *NETFLOWCollector) updateCounter(shard *flowCounterShard, parsed parsedFlow, observedAt int64) {
	counter := shard.counters[parsed.tuple]
	counter.bytes += parsed.packetBytes
	counter.packets++

	switch parsed.tuple.proto {
	case ipProtocolTCP:
		if parsed.tcpStart {
			counter.flows++
		}
	case ipProtocolUDP:
		lastSeen, exists := shard.udpSeen[parsed.tuple]
		if !exists || observedAt-lastSeen >= c.flowIdle.Nanoseconds() {
			counter.flows++
		}
		shard.udpSeen[parsed.tuple] = observedAt
	}

	shard.counters[parsed.tuple] = counter
}

// counterShard resolves counter shard for tuple updates.
// Params: tuple flow identity.
// Returns: pointer to shard storing this tuple.
func (c *NETFLOWCollector) counterShard(tuple flowTuple) *flowCounterShard {
	hash := hashFlowTuple(tuple)
	index := int(hash % uint64(len(c.shards)))
	return &c.shards[index]
}

// hashFlowTuple computes stable hash for one flow tuple.
// Params: tuple flow identity.
// Returns: unsigned hash value.
func hashFlowTuple(tuple flowTuple) uint64 {
	const (
		offset = uint64(1469598103934665603)
		prime  = uint64(1099511628211)
	)

	hash := offset
	for idx := 0; idx < len(tuple.iface); idx++ {
		hash ^= uint64(tuple.iface[idx])
		hash *= prime
	}

	hash ^= uint64(tuple.proto)
	hash *= prime
	if tuple.srcV4 {
		hash ^= 1
	}
	hash *= prime
	if tuple.dstV4 {
		hash ^= 1
	}
	hash *= prime

	for _, byteValue := range tuple.srcAddr {
		hash ^= uint64(byteValue)
		hash *= prime
	}
	for _, byteValue := range tuple.dstAddr {
		hash ^= uint64(byteValue)
		hash *= prime
	}

	hash ^= uint64(tuple.srcPort)
	hash *= prime
	hash ^= uint64(tuple.dstPort)
	hash *= prime
	return hash
}

// resolveMatchingInterfaces expands wildcard patterns to active interfaces.
// Params: patterns compiled wildcard patterns; masks original wildcard strings for diagnostics.
// Returns: interface map by name or error when no matches.
func resolveMatchingInterfaces(patterns []match.WildcardPattern, masks []string) (map[string]net.Interface, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("netflow.ifaces is required")
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list interfaces: %w", err)
	}

	matched := make(map[string]net.Interface)
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		name := strings.TrimSpace(iface.Name)
		if name == "" {
			continue
		}

		for _, pattern := range patterns {
			if pattern.Match(name) {
				matched[name] = iface
				break
			}
		}
	}

	if len(matched) == 0 {
		return nil, fmt.Errorf("no active interfaces match masks %q", strings.Join(masks, ","))
	}

	return matched, nil
}

// openPacketSource opens AF_PACKET raw socket for one interface.
// Params: iface network interface metadata.
// Returns: packet source or socket error.
func openPacketSource(iface net.Interface) (*packetSource, error) {
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, int(htons(unix.ETH_P_ALL)))
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}

	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, 4<<20); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("set rcvbuf: %w", err)
	}

	link := &unix.SockaddrLinklayer{
		Ifindex:  iface.Index,
		Protocol: htons(unix.ETH_P_ALL),
	}
	if err := unix.Bind(fd, link); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("bind iface %s: %w", iface.Name, err)
	}

	return &packetSource{
		iface: iface.Name,
		fd:    fd,
	}, nil
}

// htons converts host-order uint16 into network byte order.
// Params: value host-order integer.
// Returns: network-order integer.
func htons(value uint16) uint16 {
	return (value<<8)&0xff00 | value>>8
}

// parsedFlow is one parsed packet with tuple identity, packet size and flow-start hint.
// Params: none.
// Returns: parsed packet fields.
type parsedFlow struct {
	tuple       flowTuple
	packetBytes uint64
	tcpStart    bool
}

// parseFlowTuple parses one Ethernet frame and extracts TCP/UDP flow tuple.
// Params: iface source interface name, frame raw Ethernet bytes.
// Returns: parsed flow and parse success flag.
func parseFlowTuple(iface string, frame []byte) (parsedFlow, bool) {
	etherType, payload, ok := parseEthernet(frame)
	if !ok {
		return parsedFlow{}, false
	}

	switch etherType {
	case etherTypeIPv4:
		return parseIPv4Tuple(iface, payload)
	case etherTypeIPv6:
		return parseIPv6Tuple(iface, payload)
	default:
		return parsedFlow{}, false
	}
}

// parseEthernet strips Ethernet header and optional VLAN headers.
// Params: frame raw Ethernet bytes.
// Returns: final EtherType, L3 payload, and parse success flag.
func parseEthernet(frame []byte) (uint16, []byte, bool) {
	if len(frame) < etherHeaderLen {
		return 0, nil, false
	}

	etherType := binary.BigEndian.Uint16(frame[12:14])
	offset := etherHeaderLen
	for etherType == etherTypeVLAN || etherType == etherTypeQinQ || etherType == etherTypeQinQ2 {
		if len(frame) < offset+4 {
			return 0, nil, false
		}
		etherType = binary.BigEndian.Uint16(frame[offset+2 : offset+4])
		offset += 4
	}

	if len(frame) <= offset {
		return 0, nil, false
	}

	return etherType, frame[offset:], true
}

// parseIPv4Tuple parses IPv4 packet into TCP/UDP tuple.
// Params: iface source interface name; packet IPv4 bytes.
// Returns: tuple, packet bytes, and parse success flag.
func parseIPv4Tuple(iface string, packet []byte) (parsedFlow, bool) {
	if len(packet) < 20 {
		return parsedFlow{}, false
	}

	version := packet[0] >> 4
	if version != 4 {
		return parsedFlow{}, false
	}

	ihl := int(packet[0]&0x0f) * 4
	if ihl < 20 || len(packet) < ihl {
		return parsedFlow{}, false
	}

	totalLength := int(binary.BigEndian.Uint16(packet[2:4]))
	if totalLength < ihl {
		return parsedFlow{}, false
	}
	if totalLength > len(packet) {
		totalLength = len(packet)
	}

	flagsOffset := binary.BigEndian.Uint16(packet[6:8])
	if flagsOffset&0x1fff != 0 {
		return parsedFlow{}, false
	}

	proto := packet[9]
	if proto != ipProtocolTCP && proto != ipProtocolUDP {
		return parsedFlow{}, false
	}

	if len(packet) < ihl+4 {
		return parsedFlow{}, false
	}

	tuple := flowTuple{
		iface:   iface,
		proto:   proto,
		srcV4:   true,
		dstV4:   true,
		srcPort: binary.BigEndian.Uint16(packet[ihl : ihl+2]),
		dstPort: binary.BigEndian.Uint16(packet[ihl+2 : ihl+4]),
	}
	copy(tuple.srcAddr[12:16], packet[12:16])
	copy(tuple.dstAddr[12:16], packet[16:20])

	tcpStart := false
	if proto == ipProtocolTCP {
		if len(packet) < ihl+20 {
			return parsedFlow{}, false
		}
		flags := packet[ihl+13]
		tcpStart = flags&0x02 != 0 && flags&0x10 == 0
	}

	return parsedFlow{
		tuple:       tuple,
		packetBytes: uint64(totalLength),
		tcpStart:    tcpStart,
	}, true
}

// parseIPv6Tuple parses IPv6 packet into TCP/UDP tuple.
// Params: iface source interface name; packet IPv6 bytes.
// Returns: tuple, packet bytes, and parse success flag.
func parseIPv6Tuple(iface string, packet []byte) (parsedFlow, bool) {
	if len(packet) < 40 {
		return parsedFlow{}, false
	}

	version := packet[0] >> 4
	if version != 6 {
		return parsedFlow{}, false
	}

	payloadLength := int(binary.BigEndian.Uint16(packet[4:6]))
	totalLength := payloadLength + 40
	if totalLength > len(packet) {
		totalLength = len(packet)
	}

	nextHeader := packet[6]
	offset := 40

	for {
		switch nextHeader {
		case 0, 43, 60:
			if len(packet) < offset+2 {
				return parsedFlow{}, false
			}
			headerLength := (int(packet[offset+1]) + 1) * 8
			nextHeader = packet[offset]
			offset += headerLength
		case 44:
			if len(packet) < offset+8 {
				return parsedFlow{}, false
			}
			fragmentOffset := (binary.BigEndian.Uint16(packet[offset+2:offset+4]) >> 3) & 0x1fff
			nextHeader = packet[offset]
			offset += 8
			if fragmentOffset != 0 {
				return parsedFlow{}, false
			}
		case 51:
			if len(packet) < offset+2 {
				return parsedFlow{}, false
			}
			headerLength := (int(packet[offset+1]) + 2) * 4
			nextHeader = packet[offset]
			offset += headerLength
		case 50:
			return parsedFlow{}, false
		default:
			goto transport
		}

		if len(packet) < offset {
			return parsedFlow{}, false
		}
	}

transport:
	if nextHeader != ipProtocolTCP && nextHeader != ipProtocolUDP {
		return parsedFlow{}, false
	}
	if len(packet) < offset+4 {
		return parsedFlow{}, false
	}

	tuple := flowTuple{
		iface:   iface,
		proto:   nextHeader,
		srcPort: binary.BigEndian.Uint16(packet[offset : offset+2]),
		dstPort: binary.BigEndian.Uint16(packet[offset+2 : offset+4]),
	}
	copy(tuple.srcAddr[:], packet[8:24])
	copy(tuple.dstAddr[:], packet[24:40])

	tcpStart := false
	if nextHeader == ipProtocolTCP {
		if len(packet) < offset+20 {
			return parsedFlow{}, false
		}
		flags := packet[offset+13]
		tcpStart = flags&0x02 != 0 && flags&0x10 == 0
	}

	return parsedFlow{
		tuple:       tuple,
		packetBytes: uint64(totalLength),
		tcpStart:    tcpStart,
	}, true
}

type flowRow struct {
	tuple   flowTuple
	counter flowCounter
}

type flowTopHeap struct {
	rows  []flowRow
	limit int
}

// Len reports current heap length.
// Params: none.
// Returns: element count.
func (h flowTopHeap) Len() int {
	return len(h.rows)
}

// Less defines min-heap order where root is the "worst" row among kept top-N.
// Params: i/j indexes in heap storage.
// Returns: true when row i must be ordered before row j.
func (h flowTopHeap) Less(i, j int) bool {
	return flowRowBetter(h.rows[j], h.rows[i])
}

// Swap swaps heap elements.
// Params: i/j indexes in heap storage.
// Returns: none.
func (h flowTopHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

// Push appends one element into heap storage.
// Params: value heap element.
// Returns: none.
func (h *flowTopHeap) Push(value any) {
	h.rows = append(h.rows, value.(flowRow))
}

// Pop removes and returns last heap element.
// Params: none.
// Returns: removed heap element.
func (h *flowTopHeap) Pop() any {
	last := len(h.rows) - 1
	item := h.rows[last]
	h.rows = h.rows[:last]
	return item
}

// Offer inserts one row while preserving only top-N best rows.
// Params: row candidate.
// Returns: none.
func (h *flowTopHeap) Offer(row flowRow) {
	if h.limit <= 0 {
		return
	}

	if len(h.rows) < h.limit {
		heap.Push(h, row)
		return
	}
	if flowRowBetter(row, h.rows[0]) {
		h.rows[0] = row
		heap.Fix(h, 0)
	}
}

// flowRowBetter compares rows using stable top ordering.
// Params: left/right rows.
// Returns: true when left should be ranked before right.
func flowRowBetter(left, right flowRow) bool {
	if left.counter.bytes != right.counter.bytes {
		return left.counter.bytes > right.counter.bytes
	}
	if left.counter.packets != right.counter.packets {
		return left.counter.packets > right.counter.packets
	}
	if left.tuple.iface != right.tuple.iface {
		return left.tuple.iface < right.tuple.iface
	}
	if left.tuple.proto != right.tuple.proto {
		return left.tuple.proto < right.tuple.proto
	}
	if left.tuple.srcV4 != right.tuple.srcV4 {
		return left.tuple.srcV4
	}
	if cmp := bytes.Compare(left.tuple.srcAddr[:], right.tuple.srcAddr[:]); cmp != 0 {
		return cmp < 0
	}
	if left.tuple.srcPort != right.tuple.srcPort {
		return left.tuple.srcPort < right.tuple.srcPort
	}
	if left.tuple.dstV4 != right.tuple.dstV4 {
		return left.tuple.dstV4
	}
	if cmp := bytes.Compare(left.tuple.dstAddr[:], right.tuple.dstAddr[:]); cmp != 0 {
		return cmp < 0
	}
	return left.tuple.dstPort < right.tuple.dstPort
}

// buildFlowPoints converts tuple counters into sorted top-N metric points.
// Params: counters snapshot map; topN limit.
// Returns: sorted point slice.
func buildFlowPoints(counters map[flowTuple]flowCounter, topN int) []Point {
	if len(counters) == 0 {
		return nil
	}

	limit := len(counters)
	if topN > 0 && topN < limit {
		limit = topN
	}

	top := &flowTopHeap{
		rows:  make([]flowRow, 0, limit),
		limit: limit,
	}
	heap.Init(top)

	for tuple, counter := range counters {
		top.Offer(flowRow{tuple: tuple, counter: counter})
	}

	rows := make([]flowRow, len(top.rows))
	copy(rows, top.rows)
	sort.Slice(rows, func(i, j int) bool {
		return flowRowBetter(rows[i], rows[j])
	})

	points := make([]Point, 0, len(rows))
	for idx := range rows {
		row := rows[idx]
		points = append(points, Point{
			Key: formatFlowKey(row.tuple),
			Values: map[string]Value{
				"bytes":   {Raw: float64(row.counter.bytes), Kind: KindNumber},
				"packets": {Raw: float64(row.counter.packets), Kind: KindNumber},
				"flows":   {Raw: float64(row.counter.flows), Kind: KindNumber},
			},
		})
	}

	return points
}

// formatFlowKey renders tuple key in canonical form.
// Params: tuple parsed flow tuple.
// Returns: key string `iface|proto|src_ip|src_port|dst_ip|dst_port`.
func formatFlowKey(tuple flowTuple) string {
	proto := "ip"
	switch tuple.proto {
	case ipProtocolTCP:
		proto = "tcp"
	case ipProtocolUDP:
		proto = "udp"
	default:
		proto = strconv.Itoa(int(tuple.proto))
	}

	srcIP := formatIP(tuple.srcAddr, tuple.srcV4)
	dstIP := formatIP(tuple.dstAddr, tuple.dstV4)

	return tuple.iface +
		"|" + proto +
		"|" + srcIP +
		"|" + strconv.FormatUint(uint64(tuple.srcPort), 10) +
		"|" + dstIP +
		"|" + strconv.FormatUint(uint64(tuple.dstPort), 10)
}

// formatIP converts 16-byte storage into string with IPv4/IPv6 semantics.
// Params: addr 16-byte address, isV4 selects IPv4 output mode.
// Returns: human-readable IP string.
func formatIP(addr [16]byte, isV4 bool) string {
	if isV4 {
		var v4 [4]byte
		copy(v4[:], addr[12:16])
		return netip.AddrFrom4(v4).String()
	}
	return netip.AddrFrom16(addr).String()
}
