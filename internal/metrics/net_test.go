package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	netio "github.com/shirou/gopsutil/v4/net"
)

// TestParseProcCounterSections verifies procfs key/value section parsing.
// Params: testing.T for assertions.
// Returns: none.
func TestParseProcCounterSections(t *testing.T) {
	payload := []byte(
		"Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens RetransSegs OutRsts\n" +
			"Tcp: 1 200 120000 -1 11 22 33 44\n" +
			"Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\n" +
			"Udp: 7 8 9 10 11 12\n",
	)

	sections, err := parseProcCounterSections(payload)
	if err != nil {
		t.Fatalf("parseProcCounterSections() error: %v", err)
	}
	if got := counterOrZero(sections, "Tcp", "ActiveOpens"); got != 11 {
		t.Fatalf("unexpected Tcp.ActiveOpens: %d", got)
	}
	if got := counterOrZero(sections, "Tcp", "RetransSegs"); got != 33 {
		t.Fatalf("unexpected Tcp.RetransSegs: %d", got)
	}
	if got := counterOrZero(sections, "Tcp", "MaxConn"); got != 0 {
		t.Fatalf("expected negative MaxConn to be ignored, got: %d", got)
	}
	if got := counterOrZero(sections, "Udp", "OutDatagrams"); got != 10 {
		t.Fatalf("unexpected Udp.OutDatagrams: %d", got)
	}
}

// TestCollectTCPSocketSamples verifies /proc/net/tcp* parsing and top-N limiting.
// Params: testing.T for assertions.
// Returns: none.
func TestCollectTCPSocketSamples(t *testing.T) {
	tcpPayload := []byte(
		"sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n" +
			"0: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 1 1 0 0 10 0\n" +
			"1: 00000000:1F91 00000000:0000 01 00000010:00000020 00:00000000 00000002 0 0 1 1 0 0 20 0\n" +
			"2: 00000000:1F92 00000000:0000 01 00000040:00000001 00:00000000 00000004 0 0 1 1 0 0 10 0\n",
	)
	tcp6Payload := []byte(
		"sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n" +
			"0: 00000000000000000000000000000000:1F93 00000000000000000000000000000000:0000 01 00000005:00000005 00:00000000 00000001 0 0 1 1 0 0 30 0\n",
	)

	readFile := func(path string) ([]byte, error) {
		switch path {
		case procTCPPath:
			return tcpPayload, nil
		case procTCP6Path:
			return tcp6Payload, nil
		default:
			return nil, fmt.Errorf("unexpected path: %s", path)
		}
	}

	samples, err := collectTCPSocketSamples(readFile, 1)
	if err != nil {
		t.Fatalf("collectTCPSocketSamples() error: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("unexpected sample count: %d", len(samples))
	}
	if samples[0].txQueueBytes != 0x40 {
		t.Fatalf("unexpected top sample tx_queue: %d", samples[0].txQueueBytes)
	}
	if samples[0].rxQueueBytes != 0x01 {
		t.Fatalf("unexpected top sample rx_queue: %d", samples[0].rxQueueBytes)
	}
	if samples[0].retransmits != 4 {
		t.Fatalf("unexpected top sample retransmits: %d", samples[0].retransmits)
	}
	if samples[0].cwndSegs != 10 {
		t.Fatalf("unexpected top sample cwnd: %d", samples[0].cwndSegs)
	}
}

// TestNETCollectorScrape verifies interface deltas, stack deltas, and by-CC point generation.
// Params: testing.T for assertions.
// Returns: none.
func TestNETCollectorScrape(t *testing.T) {
	collector := NewNETCollector("net", 2)

	ioCall := 0
	collector.readIOCounters = func(_ context.Context, _ bool) ([]netio.IOCountersStat, error) {
		ioCall++
		if ioCall == 1 {
			return []netio.IOCountersStat{
				{
					Name:        "eth0",
					BytesSent:   100,
					BytesRecv:   200,
					PacketsSent: 10,
					PacketsRecv: 20,
					Errin:       1,
					Errout:      2,
					Dropin:      3,
					Dropout:     4,
				},
			}, nil
		}
		return []netio.IOCountersStat{
			{
				Name:        "eth0",
				BytesSent:   400,
				BytesRecv:   900,
				PacketsSent: 40,
				PacketsRecv: 80,
				Errin:       3,
				Errout:      4,
				Dropin:      5,
				Dropout:     9,
			},
		}, nil
	}

	snmpPayload := []byte(
		"Tcp: ActiveOpens PassiveOpens RetransSegs OutRsts\n" +
			"Tcp: 10 20 30 40\n" +
			"Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\n" +
			"Udp: 100 200 300 400 500 600\n",
	)
	netstatPayload := []byte(
		"TcpExt: SyncookiesSent TCPTimeouts\n" +
			"TcpExt: 1 50\n",
	)
	tcpPayload := []byte(
		"sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n" +
			"0: 00000000:1F91 00000000:0000 01 00000010:00000020 00:00000000 00000002 0 0 1 1 0 0 20 0\n",
	)
	tcp6Payload := []byte(
		"sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n" +
			"0: 00000000000000000000000000000000:1F93 00000000000000000000000000000000:0000 01 00000001:00000002 00:00000000 00000001 0 0 1 1 0 0 10 0\n",
	)

	collector.readFile = func(path string) ([]byte, error) {
		switch path {
		case procSNMPPath:
			return snmpPayload, nil
		case procNetstatPath:
			return netstatPayload, nil
		case procTCPPath:
			return tcpPayload, nil
		case procTCP6Path:
			return tcp6Payload, nil
		case procTCPCCPath:
			return []byte("cubic\n"), nil
		default:
			return nil, fmt.Errorf("unexpected path: %s", path)
		}
	}

	first, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("first scrape error: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("unexpected first point count: %d", len(first))
	}

	ifacePoint := mustFindPoint(t, first, "eth0")
	if ifacePoint.Values["tx_bytes"].Raw != 0 {
		t.Fatalf("expected first tx_bytes=0, got: %v", ifacePoint.Values["tx_bytes"].Raw)
	}
	totalPoint := mustFindPoint(t, first, "total")
	if totalPoint.Values["tcp_active_opens"].Raw != 0 {
		t.Fatalf("expected first tcp_active_opens=0, got: %v", totalPoint.Values["tcp_active_opens"].Raw)
	}
	ccPoint := mustFindPoint(t, first, "cc:cubic")
	if ccPoint.Values["tcp_sockets"].Raw != 2 {
		t.Fatalf("unexpected first tcp_sockets: %v", ccPoint.Values["tcp_sockets"].Raw)
	}

	snmpPayload = []byte(
		"Tcp: ActiveOpens PassiveOpens RetransSegs OutRsts\n" +
			"Tcp: 16 27 39 44\n" +
			"Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\n" +
			"Udp: 120 202 303 450 503 607\n",
	)
	netstatPayload = []byte(
		"TcpExt: SyncookiesSent TCPTimeouts\n" +
			"TcpExt: 1 58\n",
	)

	collector.mu.Lock()
	collector.prev.at = time.Now().Add(-2 * time.Second)
	collector.mu.Unlock()

	second, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("second scrape error: %v", err)
	}
	if len(second) != 3 {
		t.Fatalf("unexpected second point count: %d", len(second))
	}

	ifacePoint = mustFindPoint(t, second, "eth0")
	if ifacePoint.Values["tx_bytes"].Raw != 300 {
		t.Fatalf("unexpected second tx_bytes: %v", ifacePoint.Values["tx_bytes"].Raw)
	}
	if ifacePoint.Values["rx_bytes"].Raw != 700 {
		t.Fatalf("unexpected second rx_bytes: %v", ifacePoint.Values["rx_bytes"].Raw)
	}

	totalPoint = mustFindPoint(t, second, "total")
	if totalPoint.Values["tcp_active_opens"].Raw != 6 {
		t.Fatalf("unexpected second tcp_active_opens: %v", totalPoint.Values["tcp_active_opens"].Raw)
	}
	if totalPoint.Values["tcp_timeouts"].Raw != 8 {
		t.Fatalf("unexpected second tcp_timeouts: %v", totalPoint.Values["tcp_timeouts"].Raw)
	}
	if totalPoint.Values["udp_out_datagrams"].Raw != 50 {
		t.Fatalf("unexpected second udp_out_datagrams: %v", totalPoint.Values["udp_out_datagrams"].Raw)
	}
}

// mustFindPoint finds point by key.
// Params: t for assertions, points list, and key to match.
// Returns: matching point or test failure.
func mustFindPoint(t *testing.T, points []Point, key string) Point {
	t.Helper()
	for _, point := range points {
		if point.Key == key {
			return point
		}
	}
	t.Fatalf("point with key %q not found", key)
	return Point{}
}
