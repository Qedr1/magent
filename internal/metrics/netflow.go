package metrics

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"

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
}

type packetSource struct {
	iface string
	fd    int
}

// NETFLOWCollector captures raw packets via AF_PACKET and aggregates top flow tuples.
// Params: metricName emitted into event.metric, ifaceMasks wildcard interface selectors, topN emitted flow limit.
// Returns: configured NETFLOW collector.
type NETFLOWCollector struct {
	metricName string
	ifaceMasks []string
	topN       int

	mu       sync.Mutex
	started  bool
	stopped  bool
	sources  map[string]*packetSource
	counters map[flowTuple]flowCounter
}

// NewNETFLOWCollector creates a built-in netflow collector.
// Params: metricName emitted into event.metric, ifaceMasks wildcard interface selectors, topN emitted flow limit.
// Returns: NETFLOW collector instance.
func NewNETFLOWCollector(metricName string, ifaceMasks []string, topN uint32) *NETFLOWCollector {
	masks := make([]string, 0, len(ifaceMasks))
	for _, pattern := range ifaceMasks {
		trimmed := strings.TrimSpace(pattern)
		if trimmed == "" {
			continue
		}
		masks = append(masks, trimmed)
	}

	limit := int(topN)
	if limit <= 0 {
		limit = 20
	}

	return &NETFLOWCollector{
		metricName: strings.TrimSpace(metricName),
		ifaceMasks: masks,
		topN:       limit,
		sources:    make(map[string]*packetSource),
		counters:   make(map[flowTuple]flowCounter),
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
	matched, err := resolveMatchingInterfaces(c.ifaceMasks)
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

		tuple, packetBytes, ok := parseFlowTuple(source.iface, buffer[:n])
		if !ok {
			continue
		}

		c.mu.Lock()
		counter := c.counters[tuple]
		counter.bytes += packetBytes
		counter.packets++
		c.counters[tuple] = counter
		c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := c.counters
	c.counters = make(map[flowTuple]flowCounter, len(snapshot))
	return snapshot
}

// resolveMatchingInterfaces expands wildcard patterns to active interfaces.
// Params: masks wildcard patterns.
// Returns: interface map by name or error when no matches.
func resolveMatchingInterfaces(masks []string) (map[string]net.Interface, error) {
	if len(masks) == 0 {
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

		for _, pattern := range masks {
			if match.WildcardMatch(pattern, name) {
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

// parseFlowTuple parses one Ethernet frame and extracts TCP/UDP flow tuple.
// Params: iface source interface name, frame raw Ethernet bytes.
// Returns: tuple, packet bytes, and parse success flag.
func parseFlowTuple(iface string, frame []byte) (flowTuple, uint64, bool) {
	etherType, payload, ok := parseEthernet(frame)
	if !ok {
		return flowTuple{}, 0, false
	}

	switch etherType {
	case etherTypeIPv4:
		return parseIPv4Tuple(iface, payload)
	case etherTypeIPv6:
		return parseIPv6Tuple(iface, payload)
	default:
		return flowTuple{}, 0, false
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
func parseIPv4Tuple(iface string, packet []byte) (flowTuple, uint64, bool) {
	if len(packet) < 20 {
		return flowTuple{}, 0, false
	}

	version := packet[0] >> 4
	if version != 4 {
		return flowTuple{}, 0, false
	}

	ihl := int(packet[0]&0x0f) * 4
	if ihl < 20 || len(packet) < ihl {
		return flowTuple{}, 0, false
	}

	totalLength := int(binary.BigEndian.Uint16(packet[2:4]))
	if totalLength < ihl {
		return flowTuple{}, 0, false
	}
	if totalLength > len(packet) {
		totalLength = len(packet)
	}

	flagsOffset := binary.BigEndian.Uint16(packet[6:8])
	if flagsOffset&0x1fff != 0 {
		return flowTuple{}, 0, false
	}

	proto := packet[9]
	if proto != ipProtocolTCP && proto != ipProtocolUDP {
		return flowTuple{}, 0, false
	}

	if len(packet) < ihl+4 {
		return flowTuple{}, 0, false
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

	return tuple, uint64(totalLength), true
}

// parseIPv6Tuple parses IPv6 packet into TCP/UDP tuple.
// Params: iface source interface name; packet IPv6 bytes.
// Returns: tuple, packet bytes, and parse success flag.
func parseIPv6Tuple(iface string, packet []byte) (flowTuple, uint64, bool) {
	if len(packet) < 40 {
		return flowTuple{}, 0, false
	}

	version := packet[0] >> 4
	if version != 6 {
		return flowTuple{}, 0, false
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
				return flowTuple{}, 0, false
			}
			headerLength := (int(packet[offset+1]) + 1) * 8
			nextHeader = packet[offset]
			offset += headerLength
		case 44:
			if len(packet) < offset+8 {
				return flowTuple{}, 0, false
			}
			fragmentOffset := (binary.BigEndian.Uint16(packet[offset+2:offset+4]) >> 3) & 0x1fff
			nextHeader = packet[offset]
			offset += 8
			if fragmentOffset != 0 {
				return flowTuple{}, 0, false
			}
		case 51:
			if len(packet) < offset+2 {
				return flowTuple{}, 0, false
			}
			headerLength := (int(packet[offset+1]) + 2) * 4
			nextHeader = packet[offset]
			offset += headerLength
		case 50:
			return flowTuple{}, 0, false
		default:
			goto transport
		}

		if len(packet) < offset {
			return flowTuple{}, 0, false
		}
	}

transport:
	if nextHeader != ipProtocolTCP && nextHeader != ipProtocolUDP {
		return flowTuple{}, 0, false
	}
	if len(packet) < offset+4 {
		return flowTuple{}, 0, false
	}

	tuple := flowTuple{
		iface:   iface,
		proto:   nextHeader,
		srcPort: binary.BigEndian.Uint16(packet[offset : offset+2]),
		dstPort: binary.BigEndian.Uint16(packet[offset+2 : offset+4]),
	}
	copy(tuple.srcAddr[:], packet[8:24])
	copy(tuple.dstAddr[:], packet[24:40])

	return tuple, uint64(totalLength), true
}

// buildFlowPoints converts tuple counters into sorted top-N metric points.
// Params: counters snapshot map; topN limit.
// Returns: sorted point slice.
func buildFlowPoints(counters map[flowTuple]flowCounter, topN int) []Point {
	type row struct {
		tuple   flowTuple
		counter flowCounter
	}

	rows := make([]row, 0, len(counters))
	for tuple, counter := range counters {
		rows = append(rows, row{
			tuple:   tuple,
			counter: counter,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].counter.bytes != rows[j].counter.bytes {
			return rows[i].counter.bytes > rows[j].counter.bytes
		}
		if rows[i].counter.packets != rows[j].counter.packets {
			return rows[i].counter.packets > rows[j].counter.packets
		}
		if rows[i].tuple.iface != rows[j].tuple.iface {
			return rows[i].tuple.iface < rows[j].tuple.iface
		}
		if rows[i].tuple.proto != rows[j].tuple.proto {
			return rows[i].tuple.proto < rows[j].tuple.proto
		}
		if rows[i].tuple.srcV4 != rows[j].tuple.srcV4 {
			return rows[i].tuple.srcV4
		}
		if cmp := bytes.Compare(rows[i].tuple.srcAddr[:], rows[j].tuple.srcAddr[:]); cmp != 0 {
			return cmp < 0
		}
		if rows[i].tuple.srcPort != rows[j].tuple.srcPort {
			return rows[i].tuple.srcPort < rows[j].tuple.srcPort
		}
		if rows[i].tuple.dstV4 != rows[j].tuple.dstV4 {
			return rows[i].tuple.dstV4
		}
		if cmp := bytes.Compare(rows[i].tuple.dstAddr[:], rows[j].tuple.dstAddr[:]); cmp != 0 {
			return cmp < 0
		}
		return rows[i].tuple.dstPort < rows[j].tuple.dstPort
	})

	limit := len(rows)
	if topN > 0 && topN < limit {
		limit = topN
	}

	points := make([]Point, 0, limit)
	for idx := 0; idx < limit; idx++ {
		row := rows[idx]
		points = append(points, Point{
			Key: formatFlowKey(row.tuple),
			Values: map[string]Value{
				"bytes":   {Raw: float64(row.counter.bytes), Kind: KindNumber},
				"packets": {Raw: float64(row.counter.packets), Kind: KindNumber},
				"flows":   {Raw: 1, Kind: KindNumber},
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

// wildcardMatch evaluates '*' wildcard pattern against value.
// Params: pattern may contain '*' wildcards; value is compared text.
// Returns: true on pattern match.
