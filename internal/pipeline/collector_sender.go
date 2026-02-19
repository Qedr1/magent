package pipeline

import (
	"context"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"magent/internal/vectorpb/eventpb"
	"magent/internal/vectorpb/vectorpb"
)

// CollectorSender encodes event batches and sends prepared payloads.
// Params: batch of events and destination address.
// Returns: encoded payload and send status.
type CollectorSender interface {
	Encode(events []Event) ([]byte, error)
	SendBatch(ctx context.Context, address string, events []Event, timeout time.Duration) error
	Send(ctx context.Context, address string, payload []byte, timeout time.Duration) error
}

// VectorGRPCSender sends Vector Protocol v2 payloads over gRPC.
// Params: none.
// Returns: sender implementation.
type VectorGRPCSender struct {
	mu      sync.RWMutex
	clients map[string]vectorpb.VectorClient
	conns   map[string]*grpc.ClientConn
	localIP map[string]string
}

// Close closes all cached gRPC connections and clears sender caches.
// Params: none.
// Returns: first close error when present.
func (s *VectorGRPCSender) Close() error {
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.clients = nil
	s.localIP = nil
	s.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Encode serializes a batch into protobuf PushEventsRequest payload.
// Params: events batch.
// Returns: protobuf payload or encode error.
func (s *VectorGRPCSender) Encode(events []Event) ([]byte, error) {
	request, err := buildPushEventsRequest(events)
	if err != nil {
		return nil, err
	}

	payload, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal push request: %w", err)
	}
	return payload, nil
}

// SendBatch encodes events directly into request and pushes them to one Vector address.
// Params: ctx lifecycle context; address destination host:port; events batch; timeout dial/call timeout.
// Returns: send error on encode/connect/rpc failure.
func (s *VectorGRPCSender) SendBatch(
	ctx context.Context,
	address string,
	events []Event,
	timeout time.Duration,
) error {
	request, err := buildPushEventsRequest(events)
	if err != nil {
		return err
	}
	return s.sendPreparedRequest(ctx, address, request, timeout)
}

// Send decodes and pushes payload to one Vector address with timeout.
// Params: ctx lifecycle context; address destination host:port; payload encoded push request; timeout dial/call timeout.
// Returns: send error on decode/connect/rpc failure.
func (s *VectorGRPCSender) Send(ctx context.Context, address string, payload []byte, timeout time.Duration) error {
	var request vectorpb.PushEventsRequest
	if err := proto.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("unmarshal push request: %w", err)
	}
	return s.sendPreparedRequest(ctx, address, &request, timeout)
}

// sendPreparedRequest sends a prepared push request to one Vector address.
// Params: ctx lifecycle context; address destination host:port; request prepared payload; timeout dial/call timeout.
// Returns: send error on connect/rpc failure.
func (s *VectorGRPCSender) sendPreparedRequest(
	ctx context.Context,
	address string,
	request *vectorpb.PushEventsRequest,
	timeout time.Duration,
) error {
	addr := strings.TrimSpace(address)
	if addr == "" {
		return fmt.Errorf("collector address is empty")
	}

	hostIP, err := s.localIPForAddress(ctx, addr, timeout)
	if err == nil && hostIP != "" {
		injectHostIP(request, hostIP)
	}

	client, err := s.clientForAddress(ctx, addr, timeout)
	if err != nil {
		return err
	}

	callCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if _, err := client.PushEvents(callCtx, request); err != nil {
		s.dropAddress(addr)
		return fmt.Errorf("push events %s: %w", addr, err)
	}
	return nil
}

// buildPushEventsRequest builds protobuf request from internal events.
// Params: events batch.
// Returns: protobuf request or conversion error.
func buildPushEventsRequest(events []Event) (*vectorpb.PushEventsRequest, error) {
	request := &vectorpb.PushEventsRequest{
		Events: make([]*eventpb.EventWrapper, 0, len(events)),
	}

	for idx, event := range events {
		wrapped, err := encodeEventWrapper(event)
		if err != nil {
			return nil, fmt.Errorf("encode event[%d]: %w", idx, err)
		}
		request.Events = append(request.Events, wrapped)
	}

	return request, nil
}

// clientForAddress returns cached gRPC client or dials and stores a new one.
// Params: ctx lifecycle context; address destination host:port; timeout dial timeout.
// Returns: reusable Vector gRPC client or error.
func (s *VectorGRPCSender) clientForAddress(
	ctx context.Context,
	address string,
	timeout time.Duration,
) (vectorpb.VectorClient, error) {
	s.mu.RLock()
	if client, ok := s.clients[address]; ok {
		s.mu.RUnlock()
		return client, nil
	}
	s.mu.RUnlock()

	dialCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	conn, err := grpc.DialContext(
		dialCtx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", address, err)
	}

	client := vectorpb.NewVectorClient(conn)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients == nil {
		s.clients = make(map[string]vectorpb.VectorClient)
	}
	if s.conns == nil {
		s.conns = make(map[string]*grpc.ClientConn)
	}
	if s.localIP == nil {
		s.localIP = make(map[string]string)
	}
	if cached, exists := s.clients[address]; exists {
		_ = conn.Close()
		return cached, nil
	}
	s.clients[address] = client
	s.conns[address] = conn
	return client, nil
}

// dropAddress removes cached client/connection for one address.
// Params: address destination host:port.
// Returns: none.
func (s *VectorGRPCSender) dropAddress(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, exists := s.conns[address]
	if !exists {
		return
	}
	delete(s.conns, address)
	delete(s.clients, address)
	delete(s.localIP, address)
	_ = conn.Close()
}

// localIPForAddress resolves and caches local source IP for destination address.
// Params: address destination host:port; timeout dial timeout.
// Returns: local source IP or error.
func (s *VectorGRPCSender) localIPForAddress(ctx context.Context, address string, timeout time.Duration) (string, error) {
	s.mu.RLock()
	if ip, ok := s.localIP[address]; ok {
		s.mu.RUnlock()
		return ip, nil
	}
	s.mu.RUnlock()

	dialTimeout := timeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(dialCtx, "tcp", address)
	if err != nil {
		return "", fmt.Errorf("resolve local ip for %s: %w", address, err)
	}
	defer conn.Close()

	localAddr, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok || localAddr.IP == nil {
		return "", fmt.Errorf("resolve local ip for %s: unexpected local addr", address)
	}
	ip := localAddr.IP.String()

	s.mu.Lock()
	if s.localIP == nil {
		s.localIP = make(map[string]string)
	}
	if _, exists := s.localIP[address]; !exists {
		s.localIP[address] = ip
	}
	s.mu.Unlock()

	return ip, nil
}

// injectHostIP adds host_ip field to all log events in request.
// Params: request decoded push request; hostIP local source ip.
// Returns: none.
func injectHostIP(request *vectorpb.PushEventsRequest, hostIP string) {
	if request == nil || hostIP == "" {
		return
	}

	encoded := encodeRawString(hostIP)

	for idx := range request.Events {
		logEvent := request.Events[idx].GetLog()
		if logEvent == nil {
			continue
		}
		if logEvent.Value == nil {
			continue
		}
		rootMap := logEvent.Value.GetMap()
		if rootMap == nil {
			continue
		}
		if rootMap.Fields == nil {
			rootMap.Fields = make(map[string]*eventpb.Value)
		}
		rootMap.Fields["host_ip"] = encoded
	}
}

// encodeEventWrapper converts internal event payload into Vector EventWrapper.log.
// Params: event unified payload.
// Returns: encoded EventWrapper or conversion error.
func encodeEventWrapper(event Event) (*eventpb.EventWrapper, error) {
	dataValue, err := encodeMetricData(event.Data)
	if err != nil {
		return nil, fmt.Errorf("encode log.data: %w", err)
	}

	dtValue, err := encodeUint64(event.DT)
	if err != nil {
		return nil, fmt.Errorf("encode log.dt: %w", err)
	}
	dtsValue, err := encodeUint64(event.DTS)
	if err != nil {
		return nil, fmt.Errorf("encode log.dts: %w", err)
	}

	logValue := &eventpb.Value{
		Kind: &eventpb.Value_Map{
			Map: &eventpb.ValueMap{
				Fields: map[string]*eventpb.Value{
					"dt":      dtValue,
					"dts":     dtsValue,
					"metric":  encodeRawString(event.Metric),
					"dc":      encodeRawString(event.DC),
					"host":    encodeRawString(event.Host),
					"project": encodeRawString(event.Project),
					"role":    encodeRawString(event.Role),
					"key":     encodeRawString(event.Key),
					"data":    dataValue,
				},
			},
		},
	}

	return &eventpb.EventWrapper{
		Event: &eventpb.EventWrapper_Log{
			Log: &eventpb.Log{
				Value: logValue,
			},
		},
	}, nil
}

// encodeMetricData converts event data map into protobuf map value.
// Params: data metric payload map[var]map[agg]value.
// Returns: protobuf map value or conversion error.
func encodeMetricData(data map[string]map[string]any) (*eventpb.Value, error) {
	outer := make(map[string]*eventpb.Value, len(data))
	for varName, aggMap := range data {
		inner := make(map[string]*eventpb.Value, len(aggMap))
		for aggName, raw := range aggMap {
			encoded, err := encodeValue(raw)
			if err != nil {
				return nil, fmt.Errorf("encode %q.%q: %w", varName, aggName, err)
			}
			inner[aggName] = encoded
		}
		outer[varName] = &eventpb.Value{
			Kind: &eventpb.Value_Map{
				Map: &eventpb.ValueMap{Fields: inner},
			},
		}
	}

	return &eventpb.Value{
		Kind: &eventpb.Value_Map{
			Map: &eventpb.ValueMap{Fields: outer},
		},
	}, nil
}

// encodeValue converts Go value into Vector protobuf Value recursively.
// Params: value raw payload element.
// Returns: protobuf value or conversion error.
func encodeValue(value any) (*eventpb.Value, error) {
	switch typed := value.(type) {
	case nil:
		return &eventpb.Value{
			Kind: &eventpb.Value_Null{
				Null: eventpb.ValueNull_NULL_VALUE,
			},
		}, nil
	case string:
		return encodeRawString(typed), nil
	case []byte:
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return &eventpb.Value{
			Kind: &eventpb.Value_RawBytes{
				RawBytes: cloned,
			},
		}, nil
	case bool:
		return &eventpb.Value{
			Kind: &eventpb.Value_Boolean{
				Boolean: typed,
			},
		}, nil
	case uint8:
		return encodeUint64(uint64(typed))
	case uint16:
		return encodeUint64(uint64(typed))
	case uint32:
		return encodeUint64(uint64(typed))
	case uint64:
		return encodeUint64(typed)
	case uint:
		return encodeUint64(uint64(typed))
	case int8:
		return encodeInt64(int64(typed))
	case int16:
		return encodeInt64(int64(typed))
	case int32:
		return encodeInt64(int64(typed))
	case int64:
		return encodeInt64(typed)
	case int:
		return encodeInt64(int64(typed))
	case float32:
		return encodeFloat64(float64(typed))
	case float64:
		return encodeFloat64(typed)
	case map[string]any:
		return encodeMap(typed)
	case map[string]string:
		converted := make(map[string]any, len(typed))
		for key, item := range typed {
			converted[key] = item
		}
		return encodeMap(converted)
	case map[string]map[string]any:
		converted := make(map[string]any, len(typed))
		for key, item := range typed {
			converted[key] = item
		}
		return encodeMap(converted)
	case []any:
		items := make([]*eventpb.Value, 0, len(typed))
		for idx, item := range typed {
			encoded, err := encodeValue(item)
			if err != nil {
				return nil, fmt.Errorf("encode array item[%d]: %w", idx, err)
			}
			items = append(items, encoded)
		}
		return &eventpb.Value{
			Kind: &eventpb.Value_Array{
				Array: &eventpb.ValueArray{Items: items},
			},
		}, nil
	case []string:
		items := make([]*eventpb.Value, 0, len(typed))
		for _, item := range typed {
			items = append(items, &eventpb.Value{
				Kind: &eventpb.Value_RawBytes{RawBytes: []byte(item)},
			})
		}
		return &eventpb.Value{
			Kind: &eventpb.Value_Array{
				Array: &eventpb.ValueArray{Items: items},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", typed)
	}
}

// encodeRawString converts string to protobuf raw bytes value.
// Params: value string payload.
// Returns: protobuf raw-bytes value.
func encodeRawString(value string) *eventpb.Value {
	return &eventpb.Value{
		Kind: &eventpb.Value_RawBytes{
			RawBytes: []byte(value),
		},
	}
}

// encodeMap converts map payload into protobuf map Value.
// Params: values map.
// Returns: protobuf map value or conversion error.
func encodeMap(values map[string]any) (*eventpb.Value, error) {
	fields := make(map[string]*eventpb.Value, len(values))
	for key, item := range values {
		encoded, err := encodeValue(item)
		if err != nil {
			return nil, fmt.Errorf("encode map field %q: %w", key, err)
		}
		fields[key] = encoded
	}

	return &eventpb.Value{
		Kind: &eventpb.Value_Map{
			Map: &eventpb.ValueMap{Fields: fields},
		},
	}, nil
}

// encodeUint64 converts unsigned value to protobuf integer with overflow check.
// Params: value unsigned integer.
// Returns: protobuf integer value or overflow error.
func encodeUint64(value uint64) (*eventpb.Value, error) {
	if value > math.MaxInt64 {
		return nil, fmt.Errorf("uint64 value %d overflows int64 protobuf field", value)
	}
	return &eventpb.Value{
		Kind: &eventpb.Value_Integer{
			Integer: int64(value),
		},
	}, nil
}

// encodeInt64 converts signed integer to protobuf integer.
// Params: value signed integer.
// Returns: protobuf integer value.
func encodeInt64(value int64) (*eventpb.Value, error) {
	return &eventpb.Value{
		Kind: &eventpb.Value_Integer{
			Integer: value,
		},
	}, nil
}

// encodeFloat64 converts float value to protobuf float with finite check.
// Params: value float number.
// Returns: protobuf float value or error on NaN/Inf.
func encodeFloat64(value float64) (*eventpb.Value, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("non-finite float value")
	}
	return &eventpb.Value{
		Kind: &eventpb.Value_Float{
			Float: value,
		},
	}, nil
}
