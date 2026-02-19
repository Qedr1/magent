package metrics

import (
	"encoding/binary"
	"net/netip"
	"testing"
	"time"
)

// TestParseFlowTuple_IPv4TCP verifies IPv4 TCP parsing into tuple key and byte size.
// Params: testing.T for assertions.
// Returns: none.
func TestParseFlowTuple_IPv4TCP(t *testing.T) {
	frame := makeEthernetIPv4Frame(
		0x0800,
		"10.1.1.1",
		"10.1.1.2",
		6,
		55000,
		443,
		40,
		0x02,
	)

	parsed, ok := parseFlowTuple("eth0", frame)
	if !ok {
		t.Fatalf("expected tuple parse success")
	}
	tuple := parsed.tuple
	packetBytes := parsed.packetBytes
	if packetBytes != 40 {
		t.Fatalf("unexpected packet bytes: %d", packetBytes)
	}
	if !parsed.tcpStart {
		t.Fatalf("expected SYN packet to mark flow start")
	}

	key := formatFlowKey(tuple)
	want := "eth0|tcp|10.1.1.1|55000|10.1.1.2|443"
	if key != want {
		t.Fatalf("unexpected key: got=%q want=%q", key, want)
	}
}

// TestParseFlowTuple_VLANIPv4 verifies VLAN frame handling.
// Params: testing.T for assertions.
// Returns: none.
func TestParseFlowTuple_VLANIPv4(t *testing.T) {
	base := makeEthernetIPv4Frame(
		0x0800,
		"192.168.1.10",
		"192.168.1.11",
		17,
		53000,
		53,
		32,
		0,
	)

	frame := make([]byte, 0, len(base)+4)
	frame = append(frame, base[:12]...)
	frame = append(frame, 0x81, 0x00, 0x00, 0x01)
	frame = append(frame, base[12:]...)

	parsed, ok := parseFlowTuple("eth1", frame)
	if !ok {
		t.Fatalf("expected vlan tuple parse success")
	}
	tuple := parsed.tuple
	packetBytes := parsed.packetBytes
	if packetBytes != 32 {
		t.Fatalf("unexpected packet bytes: %d", packetBytes)
	}
	if got := formatFlowKey(tuple); got != "eth1|udp|192.168.1.10|53000|192.168.1.11|53" {
		t.Fatalf("unexpected key: %q", got)
	}
}

// TestParseFlowTuple_IPv6UDP verifies IPv6 UDP parsing into tuple key and byte size.
// Params: testing.T for assertions.
// Returns: none.
func TestParseFlowTuple_IPv6UDP(t *testing.T) {
	frame := makeEthernetIPv6UDPFrame(
		"2001:db8::1",
		"2001:db8::2",
		40000,
		8125,
		8,
	)

	parsed, ok := parseFlowTuple("enp1s0", frame)
	if !ok {
		t.Fatalf("expected ipv6 tuple parse success")
	}
	tuple := parsed.tuple
	packetBytes := parsed.packetBytes
	if packetBytes != 56 {
		t.Fatalf("unexpected packet bytes: %d", packetBytes)
	}
	if got := formatFlowKey(tuple); got != "enp1s0|udp|2001:db8::1|40000|2001:db8::2|8125" {
		t.Fatalf("unexpected key: %q", got)
	}
}

// TestBuildFlowPoints_TopN verifies descending bytes ordering and top-N clipping.
// Params: testing.T for assertions.
// Returns: none.
func TestBuildFlowPoints_TopN(t *testing.T) {
	tupleA := makeFlowTuple("eth0", 6, [4]byte{10, 0, 0, 1}, [4]byte{10, 0, 0, 2}, 10000, 443)
	tupleB := makeFlowTuple("eth0", 6, [4]byte{10, 0, 0, 3}, [4]byte{10, 0, 0, 4}, 10001, 443)
	tupleC := makeFlowTuple("eth1", 17, [4]byte{10, 0, 0, 5}, [4]byte{10, 0, 0, 6}, 12000, 53)

	points := buildFlowPoints(map[flowTuple]flowCounter{
		tupleA: {bytes: 100, packets: 2, flows: 1},
		tupleB: {bytes: 300, packets: 4, flows: 3},
		tupleC: {bytes: 200, packets: 3, flows: 2},
	}, 2)

	if len(points) != 2 {
		t.Fatalf("unexpected points len: %d", len(points))
	}
	if points[0].Key != "eth0|tcp|10.0.0.3|10001|10.0.0.4|443" {
		t.Fatalf("unexpected top key: %q", points[0].Key)
	}
	if got := points[0].Values["flows"].Raw; got != float64(3) {
		t.Fatalf("unexpected flows value: %v", got)
	}
	if got := points[1].Key; got != "eth1|udp|10.0.0.5|12000|10.0.0.6|53" {
		t.Fatalf("unexpected second key: %q", got)
	}
}

// TestUpdateCounter_TCPFlowStart verifies TCP flow counting only on SYN without ACK.
// Params: testing.T for assertions.
// Returns: none.
func TestUpdateCounter_TCPFlowStart(t *testing.T) {
	collector := NewNETFLOWCollector("netflow", []string{"lo"}, 20, 10*time.Second)
	shard := &flowCounterShard{
		counters: make(map[flowTuple]flowCounter),
		udpSeen:  make(map[flowTuple]int64),
	}

	tuple := makeFlowTuple("lo", ipProtocolTCP, [4]byte{127, 0, 0, 1}, [4]byte{127, 0, 0, 1}, 50000, 19091)
	collector.updateCounter(shard, parsedFlow{tuple: tuple, packetBytes: 100, tcpStart: true}, 1)
	collector.updateCounter(shard, parsedFlow{tuple: tuple, packetBytes: 120, tcpStart: false}, 2)

	counter := shard.counters[tuple]
	if counter.bytes != 220 || counter.packets != 2 {
		t.Fatalf("unexpected packet counters: bytes=%d packets=%d", counter.bytes, counter.packets)
	}
	if counter.flows != 1 {
		t.Fatalf("unexpected flows: %d", counter.flows)
	}
}

// TestUpdateCounter_UDPIdleTimeout verifies UDP flow counting by inactivity timeout.
// Params: testing.T for assertions.
// Returns: none.
func TestUpdateCounter_UDPIdleTimeout(t *testing.T) {
	collector := NewNETFLOWCollector("netflow", []string{"lo"}, 20, 10*time.Second)
	shard := &flowCounterShard{
		counters: make(map[flowTuple]flowCounter),
		udpSeen:  make(map[flowTuple]int64),
	}

	tuple := makeFlowTuple("lo", ipProtocolUDP, [4]byte{127, 0, 0, 1}, [4]byte{127, 0, 0, 1}, 51000, 53)
	collector.updateCounter(shard, parsedFlow{tuple: tuple, packetBytes: 90}, 0)
	collector.updateCounter(shard, parsedFlow{tuple: tuple, packetBytes: 110}, int64(5*time.Second))
	collector.updateCounter(shard, parsedFlow{tuple: tuple, packetBytes: 130}, int64(16*time.Second))

	counter := shard.counters[tuple]
	if counter.bytes != 330 || counter.packets != 3 {
		t.Fatalf("unexpected packet counters: bytes=%d packets=%d", counter.bytes, counter.packets)
	}
	if counter.flows != 2 {
		t.Fatalf("unexpected udp flows: %d", counter.flows)
	}
}

// TestSwapCounters_ResetsPerScrape verifies interval counters are emitted once and then reset.
// Params: testing.T for assertions.
// Returns: none.
func TestSwapCounters_ResetsPerScrape(t *testing.T) {
	collector := NewNETFLOWCollector("netflow", []string{"lo"}, 20, 10*time.Second)
	tuple := makeFlowTuple("lo", ipProtocolTCP, [4]byte{127, 0, 0, 1}, [4]byte{127, 0, 0, 1}, 40000, 443)

	shard := collector.counterShard(tuple)
	shard.mu.Lock()
	collector.updateCounter(shard, parsedFlow{tuple: tuple, packetBytes: 200, tcpStart: true}, 1)
	shard.mu.Unlock()

	first := collector.swapCounters()
	if len(first) != 1 {
		t.Fatalf("unexpected first snapshot size: %d", len(first))
	}
	if got := first[tuple]; got.bytes != 200 || got.packets != 1 || got.flows != 1 {
		t.Fatalf("unexpected first snapshot counters: %+v", got)
	}

	second := collector.swapCounters()
	if len(second) != 0 {
		t.Fatalf("expected empty second snapshot, got: %d", len(second))
	}
}

// makeEthernetIPv4Frame builds a minimal Ethernet+IPv4+TCP/UDP frame for parser tests.
// Params: etherType is L2 type, src/dst are IPv4 strings, proto is L4 protocol, ports are transport ports, totalLen is IPv4 total length.
// Returns: raw frame bytes.
func makeEthernetIPv4Frame(etherType uint16, src string, dst string, proto uint8, srcPort uint16, dstPort uint16, totalLen uint16, tcpFlags uint8) []byte {
	if totalLen < 20 {
		totalLen = 20
	}

	frame := make([]byte, 14+int(totalLen))
	binary.BigEndian.PutUint16(frame[12:14], etherType)

	ip := frame[14:]
	ip[0] = 0x45
	binary.BigEndian.PutUint16(ip[2:4], totalLen)
	ip[8] = 64
	ip[9] = proto

	srcOctets := [4]byte{}
	dstOctets := [4]byte{}
	parseIPv4(src, &srcOctets)
	parseIPv4(dst, &dstOctets)
	copy(ip[12:16], srcOctets[:])
	copy(ip[16:20], dstOctets[:])

	if len(ip) >= 24 {
		binary.BigEndian.PutUint16(ip[20:22], srcPort)
		binary.BigEndian.PutUint16(ip[22:24], dstPort)
	}
	if proto == 6 && len(ip) >= 40 {
		ip[33] = tcpFlags
	}

	return frame
}

// makeEthernetIPv6UDPFrame builds a minimal Ethernet+IPv6+UDP frame for parser tests.
// Params: src/dst are IPv6 strings, ports are transport ports, payloadLen is UDP payload length.
// Returns: raw frame bytes.
func makeEthernetIPv6UDPFrame(src string, dst string, srcPort uint16, dstPort uint16, payloadLen uint16) []byte {
	ipPayloadLen := payloadLen + 8
	frame := make([]byte, 14+40+int(ipPayloadLen))
	binary.BigEndian.PutUint16(frame[12:14], 0x86dd)

	ip := frame[14:]
	ip[0] = 0x60
	binary.BigEndian.PutUint16(ip[4:6], ipPayloadLen)
	ip[6] = 17
	ip[7] = 64

	srcAddr := parseIPv6(src)
	dstAddr := parseIPv6(dst)
	copy(ip[8:24], srcAddr[:])
	copy(ip[24:40], dstAddr[:])

	udp := ip[40:]
	binary.BigEndian.PutUint16(udp[0:2], srcPort)
	binary.BigEndian.PutUint16(udp[2:4], dstPort)
	binary.BigEndian.PutUint16(udp[4:6], ipPayloadLen)

	return frame
}

// makeFlowTuple builds a tuple helper for top-N tests.
// Params: iface/proto and v4 endpoints/ports.
// Returns: flow tuple value.
func makeFlowTuple(iface string, proto uint8, src [4]byte, dst [4]byte, srcPort uint16, dstPort uint16) flowTuple {
	tuple := flowTuple{
		iface:   iface,
		proto:   proto,
		srcV4:   true,
		dstV4:   true,
		srcPort: srcPort,
		dstPort: dstPort,
	}
	copy(tuple.srcAddr[12:16], src[:])
	copy(tuple.dstAddr[12:16], dst[:])
	return tuple
}

// parseIPv4 parses dotted IPv4 string into 4-byte buffer.
// Params: value dotted IPv4 string, out destination.
// Returns: none.
func parseIPv4(value string, out *[4]byte) {
	addr, err := netip.ParseAddr(value)
	if err != nil || !addr.Is4() {
		return
	}
	v4 := addr.As4()
	copy(out[:], v4[:])
}

// parseIPv6 parses IPv6 string into 16-byte buffer.
// Params: value IPv6 string.
// Returns: 16-byte IPv6 address.
func parseIPv6(value string) [16]byte {
	var out [16]byte
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return out
	}
	return addr.As16()
}
