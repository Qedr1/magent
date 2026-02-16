package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// MaxPointsJSONBytes is the maximum accepted size for one external JSON payload.
const MaxPointsJSONBytes = 16 << 20

type pointValueObjectJSON struct {
	Kind  json.RawMessage `json:"kind"`
	Value json.RawMessage `json:"value"`
	Last  json.RawMessage `json:"last"`
}

// ParsePointsJSON parses the shared JSON contract used by script/http sources.
// Params: payload is raw JSON bytes (root object or array of objects).
// Returns: parsed point list or contract error.
func ParsePointsJSON(payload []byte) ([]Point, error) {
	if len(payload) > MaxPointsJSONBytes {
		return nil, fmt.Errorf("JSON payload exceeds %d bytes", MaxPointsJSONBytes)
	}

	return parsePointsPayload(payload)
}

// ParsePointsJSONFromReader parses the shared JSON contract used by script/http sources.
// Params: r provides JSON bytes (root object or array of objects).
// Returns: parsed point list or contract error.
func ParsePointsJSONFromReader(r io.Reader) ([]Point, error) {
	if r == nil {
		return nil, fmt.Errorf("nil reader")
	}

	lim := &io.LimitedReader{R: r, N: int64(MaxPointsJSONBytes) + 1}
	payload, err := io.ReadAll(lim)
	if err != nil {
		return nil, fmt.Errorf("read JSON payload: %w", err)
	}
	if len(payload) > MaxPointsJSONBytes {
		return nil, fmt.Errorf("JSON payload exceeds %d bytes", MaxPointsJSONBytes)
	}

	return parsePointsPayload(payload)
}

// parsePointsPayload parses root payload in object/array forms.
// Params: payload raw JSON bytes.
// Returns: parsed points or contract error.
func parsePointsPayload(payload []byte) ([]Point, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("decode JSON: unexpected end of JSON input")
	}

	switch trimmed[0] {
	case '{':
		point, err := parsePointRecordRaw(trimmed)
		if err != nil {
			return nil, err
		}
		return []Point{point}, nil
	case '[':
		var records []json.RawMessage
		if err := json.Unmarshal(trimmed, &records); err != nil {
			return nil, fmt.Errorf("decode JSON: %w", err)
		}
		points := make([]Point, 0, len(records))
		for idx, record := range records {
			point, err := parsePointRecordRaw(record)
			if err != nil {
				return nil, fmt.Errorf("items[%d]: %w", idx, err)
			}
			points = append(points, point)
		}
		return points, nil
	default:
		return nil, fmt.Errorf("root JSON must be object or array")
	}
}

// parsePointRecord converts one source object into metric Point.
// Params: record object with key/data.
// Returns: parsed point or contract error.
func parsePointRecordRaw(raw []byte) (Point, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return Point{}, fmt.Errorf("point record must be an object")
	}

	var record map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &record); err != nil {
		return Point{}, fmt.Errorf("decode JSON: %w", err)
	}

	keyRaw, ok := record["key"]
	if !ok {
		return Point{}, fmt.Errorf("missing key field")
	}
	var key string
	if err := json.Unmarshal(keyRaw, &key); err != nil {
		return Point{}, fmt.Errorf("key must be string")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return Point{}, fmt.Errorf("key cannot be empty")
	}

	dataRaw, ok := record["data"]
	if !ok {
		return Point{}, fmt.Errorf("missing data field")
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(dataRaw, &data); err != nil {
		return Point{}, fmt.Errorf("data must be object")
	}
	if len(data) == 0 {
		return Point{}, fmt.Errorf("data cannot be empty")
	}

	values := make(map[string]Value, len(data))
	for varName, rawValue := range data {
		name := strings.TrimSpace(varName)
		if name == "" {
			return Point{}, fmt.Errorf("data contains empty variable name")
		}

		value, err := parsePointValueRaw(name, rawValue)
		if err != nil {
			return Point{}, fmt.Errorf("data.%s: %w", name, err)
		}
		values[name] = value
	}

	return Point{
		Key:    key,
		Values: values,
	}, nil
}

// parsePointValueRaw converts one source variable payload into Value.
// Params: varName metric variable name; raw variable value bytes.
// Returns: typed value or contract error.
func parsePointValueRaw(varName string, raw json.RawMessage) (Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return Value{}, fmt.Errorf("value is empty")
	}

	switch trimmed[0] {
	case '{':
		return parsePointValueObjectRaw(varName, trimmed)
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(trimmed, &b); err != nil {
			return Value{}, fmt.Errorf("unsupported value type bool")
		}
		if b {
			return Value{Raw: 1, Kind: KindNumber}, nil
		}
		return Value{Raw: 0, Kind: KindNumber}, nil
	case '"', '[', 'n':
		return Value{}, fmt.Errorf("unsupported value type %q", string(trimmed[:1]))
	default:
		number, err := parseJSONNumberRaw(trimmed)
		if err != nil {
			return Value{}, err
		}
		return Value{Raw: number, Kind: inferValueKind(varName)}, nil
	}
}

// parsePointValueObjectRaw parses extended value object with value/last and optional kind.
// Params: varName metric variable name; raw object payload.
// Returns: typed value or contract error.
func parsePointValueObjectRaw(varName string, raw []byte) (Value, error) {
	var valueObject pointValueObjectJSON
	if err := json.Unmarshal(raw, &valueObject); err != nil {
		return Value{}, fmt.Errorf("invalid value object: %w", err)
	}

	kind := inferValueKind(varName)
	if len(valueObject.Kind) > 0 {
		parsedKind, err := parseValueKindRaw(valueObject.Kind)
		if err != nil {
			return Value{}, err
		}
		kind = parsedKind
	}

	valueRaw := bytes.TrimSpace(valueObject.Value)
	if len(valueRaw) == 0 {
		valueRaw = bytes.TrimSpace(valueObject.Last)
	}
	if len(valueRaw) == 0 {
		return Value{}, fmt.Errorf("value object must contain value or last field")
	}

	number, err := parseJSONNumberRaw(valueRaw)
	if err != nil {
		return Value{}, err
	}

	return Value{
		Raw:  number,
		Kind: kind,
	}, nil
}

// parseValueKindRaw parses optional kind selector.
// Params: raw kind field.
// Returns: parsed ValueKind or error.
func parseValueKindRaw(raw []byte) (ValueKind, error) {
	var kindString string
	if err := json.Unmarshal(raw, &kindString); err != nil {
		return KindNumber, fmt.Errorf("kind must be string")
	}

	switch strings.TrimSpace(strings.ToLower(kindString)) {
	case "number", "num", "uint64":
		return KindNumber, nil
	case "percent", "pct", "%", "uint8_percent":
		return KindPercent, nil
	default:
		return KindNumber, fmt.Errorf("unsupported kind %q", kindString)
	}
}

// parseJSONNumberRaw parses supported numeric JSON token into finite float64.
// Params: raw numeric token bytes.
// Returns: parsed float or error.
func parseJSONNumberRaw(raw []byte) (float64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("value must be numeric")
	}

	parsed, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("value must be numeric")
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("number must be finite")
	}
	return parsed, nil
}

// inferValueKind infers percent kind for standard util variable names.
// Params: varName metric variable name.
// Returns: inferred ValueKind.
func inferValueKind(varName string) ValueKind {
	name := strings.TrimSpace(strings.ToLower(varName))
	if name == "util" || strings.HasSuffix(name, "_util") {
		return KindPercent
	}
	return KindNumber
}
