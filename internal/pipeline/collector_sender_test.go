package pipeline

import (
	"context"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"magent/internal/vectorpb/vectorpb"
)

type vectorCaptureServer struct {
	vectorpb.UnimplementedVectorServer

	mu     sync.Mutex
	calls  int
	latest *vectorpb.PushEventsRequest
}

// PushEvents captures incoming requests for assertions.
// Params: ctx rpc context; request decoded protobuf payload.
// Returns: empty response or server error.
func (s *vectorCaptureServer) PushEvents(_ context.Context, request *vectorpb.PushEventsRequest) (*vectorpb.PushEventsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.latest = request
	return &vectorpb.PushEventsResponse{}, nil
}

// HealthCheck returns serving status for grpc compatibility.
// Params: ctx rpc context; request health request payload.
// Returns: serving response.
func (s *vectorCaptureServer) HealthCheck(_ context.Context, _ *vectorpb.HealthCheckRequest) (*vectorpb.HealthCheckResponse, error) {
	return &vectorpb.HealthCheckResponse{Status: vectorpb.ServingStatus_SERVING}, nil
}

// Snapshot returns captured request state under mutex.
// Params: none.
// Returns: call count and latest request pointer.
func (s *vectorCaptureServer) Snapshot() (int, *vectorpb.PushEventsRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.latest
}

// TestVectorGRPCSender_Encode validates protobuf mapping for one event payload.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_Encode(t *testing.T) {
	sender := &VectorGRPCSender{}
	events := []Event{
		{
			DT:      1000,
			DTS:     2000,
			Metric:  "cpu",
			DC:      "dc1",
			Host:    "host1",
			Project: "infra",
			Role:    "db",
			Key:     "total",
			Data: map[string]map[string]any{
				"util": {
					"last": uint8(71),
					"p90":  uint8(95),
				},
			},
		},
	}

	payload, err := sender.Encode(events, "")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var request vectorpb.PushEventsRequest
	if err := proto.Unmarshal(payload, &request); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}

	if len(request.Events) != 1 {
		t.Fatalf("unexpected events count: %d", len(request.Events))
	}

	logEvent := request.Events[0].GetLog()
	if logEvent == nil {
		t.Fatalf("expected log event wrapper")
	}

	root := logEvent.GetValue().GetMap().GetFields()
	if got := string(root["metric"].GetRawBytes()); got != "cpu" {
		t.Fatalf("unexpected metric field: %q", got)
	}
	if got := root["dt"].GetInteger(); got != 1000 {
		t.Fatalf("unexpected dt field: %d", got)
	}
	if got := string(root["key"].GetRawBytes()); got != "total" {
		t.Fatalf("unexpected key field: %q", got)
	}

	utilNode := root["data"].GetMap().GetFields()["util"].GetMap().GetFields()
	if got := utilNode["last"].GetInteger(); got != 71 {
		t.Fatalf("unexpected data.util.last: %d", got)
	}
	if got := utilNode["p90"].GetInteger(); got != 95 {
		t.Fatalf("unexpected data.util.p90: %d", got)
	}
}

// TestVectorGRPCSender_EncodeRejectsOverflow validates explicit overflow failure.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_EncodeRejectsOverflow(t *testing.T) {
	sender := &VectorGRPCSender{}
	events := []Event{
		{
			DT:      1,
			DTS:     1,
			Metric:  "cpu",
			DC:      "dc1",
			Host:    "host1",
			Project: "infra",
			Role:    "db",
			Key:     "total",
			Data: map[string]map[string]any{
				"x": {"last": uint64(math.MaxUint64)},
			},
		},
	}

	if _, err := sender.Encode(events, ""); err == nil {
		t.Fatalf("expected overflow encode error")
	}
}

// TestVectorGRPCSender_Send validates gRPC PushEvents delivery path.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_Send(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	server := grpc.NewServer()
	capture := &vectorCaptureServer{}
	vectorpb.RegisterVectorServer(server, capture)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	sender := &VectorGRPCSender{}
	events := []Event{
		{
			DT:      1,
			DTS:     2,
			Metric:  "ram",
			DC:      "dc1",
			Host:    "host1",
			Project: "infra",
			Role:    "db",
			Key:     "total",
			Data: map[string]map[string]any{
				"used": {
					"last": uint64(42),
				},
			},
		},
	}

	payload, err := sender.Encode(events, "127.0.0.1")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sender.Send(ctx, listener.Addr().String(), payload, 2*time.Second, false); err != nil {
		t.Fatalf("send: %v", err)
	}

	calls, latest := capture.Snapshot()
	if calls != 1 {
		t.Fatalf("unexpected call count: %d", calls)
	}
	if latest == nil || len(latest.Events) != 1 {
		t.Fatalf("expected single delivered event")
	}

	logEvent := latest.Events[0].GetLog()
	if logEvent == nil || logEvent.Value == nil || logEvent.Value.GetMap() == nil {
		t.Fatalf("expected delivered log event with map payload")
	}
	root := logEvent.Value.GetMap().GetFields()
	hostIP := string(root["host_ip"].GetRawBytes())
	if hostIP != "127.0.0.1" {
		t.Fatalf("unexpected host_ip field: %q", hostIP)
	}
}

// TestVectorGRPCSender_LocalIPForAddressRespectsCanceledContext verifies fast cancel on ip detection.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_LocalIPForAddressRespectsCanceledContext(t *testing.T) {
	sender := &VectorGRPCSender{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	_, err := sender.LocalIP(ctx, "127.0.0.1:65534", 2*time.Second)
	if err == nil {
		t.Fatalf("expected LocalIP error for canceled context")
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("expected fast cancel, got elapsed=%v", elapsed)
	}
}

// startVectorCaptureServer runs a capture gRPC server on a random local port.
// Params: t test handle.
// Returns: server address and capture state.
func startVectorCaptureServer(t *testing.T) (string, *vectorCaptureServer) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	capture := &vectorCaptureServer{}
	vectorpb.RegisterVectorServer(server, capture)

	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	return listener.Addr().String(), capture
}

// TestVectorGRPCSender_SendGzip validates gzip-compressed PushEvents delivery.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_SendGzip(t *testing.T) {
	address, capture := startVectorCaptureServer(t)

	sender := &VectorGRPCSender{}
	events := []Event{
		{
			DT:      1,
			DTS:     2,
			Metric:  "ram",
			DC:      "dc1",
			Host:    "host1",
			Project: "infra",
			Role:    "db",
			Key:     "total",
			Data: map[string]map[string]any{
				"used": {"last": uint64(42)},
			},
		},
	}

	payload, err := sender.Encode(events, "")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sender.Send(ctx, address, payload, 2*time.Second, true); err != nil {
		t.Fatalf("send gzip: %v", err)
	}

	calls, latest := capture.Snapshot()
	if calls != 1 || latest == nil || len(latest.Events) != 1 {
		t.Fatalf("expected single delivered event over gzip, calls=%d", calls)
	}
}

// TestVectorGRPCSender_Check verifies health probing against a serving collector.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_Check(t *testing.T) {
	address, _ := startVectorCaptureServer(t)

	sender := &VectorGRPCSender{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sender.Check(ctx, address, time.Second); err != nil {
		t.Fatalf("check against serving collector: %v", err)
	}
}

// TestVectorGRPCSender_CheckTCPFallback verifies TCP fallback when HealthCheck RPC is unimplemented.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_CheckTCPFallback(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	sender := &VectorGRPCSender{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sender.Check(ctx, listener.Addr().String(), time.Second); err != nil {
		t.Fatalf("expected TCP fallback probe success, got %v", err)
	}
}

// TestVectorGRPCSender_CheckFailure verifies probe error for unreachable address.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_CheckFailure(t *testing.T) {
	sender := &VectorGRPCSender{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sender.Check(ctx, "127.0.0.1:1", 200*time.Millisecond); err == nil {
		t.Fatalf("expected probe error for unreachable address")
	}
}

// TestVectorGRPCSender_SendUnreachableRespectsCallTimeout verifies send without blocking dial.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_SendUnreachableRespectsCallTimeout(t *testing.T) {
	sender := &VectorGRPCSender{}

	started := time.Now()
	err := sender.Send(context.Background(), "127.0.0.1:1", []byte("x"), 200*time.Millisecond, false)
	if err == nil {
		t.Fatalf("expected send error for unreachable address")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("send must not block on dial, elapsed=%v", elapsed)
	}
}

// TestVectorGRPCSender_LocalIPWithoutListener verifies route-based source IP resolution during collector outage.
// Params: testing.T for assertions.
// Returns: none.
func TestVectorGRPCSender_LocalIPWithoutListener(t *testing.T) {
	sender := &VectorGRPCSender{}

	ip, err := sender.LocalIP(context.Background(), "127.0.0.1:65534", time.Second)
	if err != nil {
		t.Fatalf("LocalIP must resolve without listening collector: %v", err)
	}
	if ip != "127.0.0.1" {
		t.Fatalf("unexpected local ip: %q", ip)
	}
}
