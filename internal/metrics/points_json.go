package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
)

// MaxPointsJSONBytes is the maximum accepted size for one external JSON payload.
const MaxPointsJSONBytes = 16 << 20

// ParsePointsJSON parses the shared JSON contract used by script/http sources.
// Params: payload is raw JSON bytes (root object or array of objects).
// Returns: parsed point list or contract error.
func ParsePointsJSON(payload []byte) ([]Point, error) {
	if len(payload) > MaxPointsJSONBytes {
		return nil, fmt.Errorf("JSON payload exceeds %d bytes", MaxPointsJSONBytes)
	}

	return ParsePointsJSONFromReader(bytes.NewReader(payload))
}

// ParsePointsJSONFromReader parses the shared JSON contract used by script/http sources.
// Params: r provides JSON bytes (root object or array of objects).
// Returns: parsed point list or contract error.
func ParsePointsJSONFromReader(r io.Reader) ([]Point, error) {
	if r == nil {
		return nil, fmt.Errorf("nil reader")
	}

	lim := &io.LimitedReader{R: r, N: int64(MaxPointsJSONBytes) + 1}

	decoder := json.NewDecoder(lim)
	decoder.UseNumber()

	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	if err := ensureNoExtraJSON(decoder); err != nil {
		return nil, err
	}
	if lim.N <= 0 {
		return nil, fmt.Errorf("JSON payload exceeds %d bytes", MaxPointsJSONBytes)
	}

	switch value := raw.(type) {
	case map[string]any:
		point, err := parsePointRecord(value)
		if err != nil {
			return nil, err
		}
		return []Point{point}, nil
	case []any:
		points := make([]Point, 0, len(value))
		for idx, item := range value {
			record, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("items[%d] must be an object", idx)
			}
			point, err := parsePointRecord(record)
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

// ensureNoExtraJSON verifies that payload contains exactly one JSON value.
// Params: decoder positioned after first value.
// Returns: error when extra tokens are present.
func ensureNoExtraJSON(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected extra JSON value after root object")
		}
		return fmt.Errorf("invalid trailing JSON data: %w", err)
	}
	return nil
}

// parsePointRecord converts one source object into metric Point.
// Params: record object with key/data.
// Returns: parsed point or contract error.
func parsePointRecord(record map[string]any) (Point, error) {
	keyRaw, ok := record["key"]
	if !ok {
		return Point{}, fmt.Errorf("missing key field")
	}
	key, ok := keyRaw.(string)
	if !ok {
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
	data, ok := dataRaw.(map[string]any)
	if !ok {
		return Point{}, fmt.Errorf("data must be object")
	}
	if len(data) == 0 {
		return Point{}, fmt.Errorf("data cannot be empty")
	}

	values := make(map[string]Value, len(data))
	for varName, raw := range data {
		name := strings.TrimSpace(varName)
		if name == "" {
			return Point{}, fmt.Errorf("data contains empty variable name")
		}

		value, err := parsePointValue(name, raw)
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

// parsePointValue converts one source variable payload into Value.
// Params: varName metric variable name; raw variable value.
// Returns: typed value or contract error.
func parsePointValue(varName string, raw any) (Value, error) {
	switch typed := raw.(type) {
	case bool:
		if typed {
			return Value{Raw: 1, Kind: KindNumber}, nil
		}
		return Value{Raw: 0, Kind: KindNumber}, nil
	case json.Number:
		number, err := parseJSONNumber(typed)
		if err != nil {
			return Value{}, err
		}
		return Value{Raw: number, Kind: inferValueKind(varName)}, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return Value{}, fmt.Errorf("number must be finite")
		}
		return Value{Raw: typed, Kind: inferValueKind(varName)}, nil
	case map[string]any:
		return parsePointValueObject(varName, typed)
	default:
		return Value{}, fmt.Errorf("unsupported value type %T", raw)
	}
}

// parsePointValueObject parses extended value object with value/last and optional kind.
// Params: varName metric variable name; raw object payload.
// Returns: typed value or contract error.
func parsePointValueObject(varName string, raw map[string]any) (Value, error) {
	kind := inferValueKind(varName)
	if kindRaw, ok := raw["kind"]; ok {
		parsedKind, err := parseValueKind(kindRaw)
		if err != nil {
			return Value{}, err
		}
		kind = parsedKind
	}

	valueRaw, ok := raw["value"]
	if !ok {
		valueRaw, ok = raw["last"]
	}
	if !ok {
		return Value{}, fmt.Errorf("value object must contain value or last field")
	}

	number, err := parseFlexibleNumber(valueRaw)
	if err != nil {
		return Value{}, err
	}

	return Value{
		Raw:  number,
		Kind: kind,
	}, nil
}

// parseValueKind parses optional kind selector.
// Params: raw kind field.
// Returns: parsed ValueKind or error.
func parseValueKind(raw any) (ValueKind, error) {
	kindString, ok := raw.(string)
	if !ok {
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

// parseJSONNumber parses json.Number into finite float64.
// Params: raw JSON number.
// Returns: parsed float or error.
func parseJSONNumber(raw json.Number) (float64, error) {
	parsed, err := raw.Float64()
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", raw.String())
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("number must be finite")
	}
	return parsed, nil
}

// parseFlexibleNumber parses supported numeric representations into float64.
// Params: raw numeric value.
// Returns: parsed float or error.
func parseFlexibleNumber(raw any) (float64, error) {
	switch typed := raw.(type) {
	case json.Number:
		return parseJSONNumber(typed)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return 0, fmt.Errorf("number must be finite")
		}
		return typed, nil
	case float32:
		value := float64(typed)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, fmt.Errorf("number must be finite")
		}
		return value, nil
	case int:
		return float64(typed), nil
	case int8:
		return float64(typed), nil
	case int16:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case uint:
		return float64(typed), nil
	case uint8:
		return float64(typed), nil
	case uint16:
		return float64(typed), nil
	case uint32:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	default:
		return 0, fmt.Errorf("value must be numeric")
	}
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
