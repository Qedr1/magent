package pipeline

import (
	"fmt"
	"strconv"
	"strings"

	"magent/internal/match"
)

type conditionOperator string

const (
	operatorEQ conditionOperator = "="
	operatorNE conditionOperator = "!="
	operatorGT conditionOperator = ">"
	operatorLT conditionOperator = "<"
)

// DropCondition is one compiled drop_event expression.
// Params: raw condition and parsed parts.
// Returns: evaluatable drop condition.
type DropCondition struct {
	Raw   string
	Field string
	Op    conditionOperator
	Value string

	valueNumber float64
	valueIsNum  bool

	wildcard    match.WildcardPattern
	hasWildcard bool
}

// EventEvalContext is the input for drop_event condition evaluation.
// Params: metric/key and aggregated event data.
// Returns: condition evaluation context.
type EventEvalContext struct {
	Metric string
	Key    string
	Data   map[string]map[string]any
}

// parseDropCondition parses one drop_event expression.
// Params: expression in format <field><op><value>.
// Returns: compiled drop condition or parse error.
func parseDropCondition(expression string) (DropCondition, error) {
	raw := strings.TrimSpace(expression)
	if raw == "" {
		return DropCondition{}, fmt.Errorf("empty expression")
	}

	field, op, value, ok := splitCondition(raw)
	if !ok {
		return DropCondition{}, fmt.Errorf("invalid expression %q", raw)
	}
	if field == "" {
		return DropCondition{}, fmt.Errorf("field is empty in expression %q", raw)
	}
	if value == "" {
		return DropCondition{}, fmt.Errorf("value is empty in expression %q", raw)
	}

	condition := DropCondition{
		Raw:        raw,
		Field:      field,
		Op:         op,
		Value:      value,
		valueIsNum: false,
	}

	if parsed, ok := parseFloat(value); ok {
		condition.valueNumber = parsed
		condition.valueIsNum = true
	}

	if strings.Contains(value, "*") {
		compiled, ok := match.CompileWildcard(value)
		if ok {
			condition.wildcard = compiled
			condition.hasWildcard = true
		}
	}

	return condition, nil
}

// shouldDropEvent evaluates OR logic over all configured drop conditions.
// Params: conditions and event context.
// Returns: true when any condition matches.
func shouldDropEvent(conditions []DropCondition, ctx EventEvalContext) bool {
	for _, condition := range conditions {
		if evaluateDropCondition(condition, ctx) {
			return true
		}
	}
	return false
}

// evaluateDropCondition evaluates one drop condition.
// Params: condition and event context.
// Returns: true when condition matches.
func evaluateDropCondition(condition DropCondition, ctx EventEvalContext) bool {
	field := strings.TrimSpace(condition.Field)

	switch field {
	case "metric":
		return compareString(condition, ctx.Metric)
	case "key":
		return compareString(condition, ctx.Key)
	case "var":
		return compareVarNames(condition, ctx.Data)
	default:
		varData, ok := ctx.Data[field]
		if !ok {
			return false
		}
		lastValue, ok := varData["last"]
		if !ok {
			return false
		}
		return compareAny(condition, lastValue)
	}
}

// compareVarNames evaluates var-based conditions against event variable names.
// Params: condition compiled drop condition; data event data map.
// Returns: true when condition matches.
func compareVarNames(condition DropCondition, data map[string]map[string]any) bool {
	hasMatch := false
	for varName := range data {
		if matchConditionString(condition, varName) {
			hasMatch = true
			break
		}
	}

	switch condition.Op {
	case operatorEQ:
		return hasMatch
	case operatorNE:
		return !hasMatch
	default:
		return false
	}
}

// compareAny compares string/numeric values according to operator.
// Params: condition compiled drop condition; actual event value.
// Returns: true when comparison succeeds.
func compareAny(condition DropCondition, actual any) bool {
	actualNumber, actualIsNumber := toFloat64(actual)

	switch condition.Op {
	case operatorGT:
		return actualIsNumber && condition.valueIsNum && actualNumber > condition.valueNumber
	case operatorLT:
		return actualIsNumber && condition.valueIsNum && actualNumber < condition.valueNumber
	case operatorEQ:
		if actualIsNumber && condition.valueIsNum {
			return actualNumber == condition.valueNumber
		}
		return compareString(condition, stringify(actual))
	case operatorNE:
		if actualIsNumber && condition.valueIsNum {
			return actualNumber != condition.valueNumber
		}
		return compareString(condition, stringify(actual))
	default:
		return false
	}
}

// compareString compares strings with optional wildcard support.
// Params: condition compiled drop condition; actual event string value.
// Returns: true when comparison succeeds.
func compareString(condition DropCondition, actual string) bool {
	matched := matchConditionString(condition, actual)

	switch condition.Op {
	case operatorEQ:
		return matched
	case operatorNE:
		return !matched
	default:
		return false
	}
}

// matchConditionString checks one string value against condition literal/wildcard.
// Params: condition compiled drop condition; actual value from event.
// Returns: true when value matches condition.
func matchConditionString(condition DropCondition, actual string) bool {
	if condition.hasWildcard {
		return condition.wildcard.Match(actual)
	}
	return actual == condition.Value
}

// splitCondition splits raw expression into field/operator/value.
// Params: raw expression text.
// Returns: field, operator, value, and parse-ok flag.
func splitCondition(raw string) (string, conditionOperator, string, bool) {
	operators := []conditionOperator{operatorNE, operatorGT, operatorLT, operatorEQ}
	for _, op := range operators {
		parts := strings.SplitN(raw, string(op), 2)
		if len(parts) != 2 {
			continue
		}
		field := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		return field, op, value, true
	}
	return "", "", "", false
}

// toFloat64 converts numeric values into float64.
// Params: v is runtime value.
// Returns: float64 value and conversion success flag.
func toFloat64(v any) (float64, bool) {
	switch value := v.(type) {
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case int:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

// parseFloat parses string numeric value.
// Params: raw numeric string.
// Returns: float64 value and parse success flag.
func parseFloat(raw string) (float64, bool) {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// stringify converts any value into string representation.
// Params: v runtime value.
// Returns: string representation.
func stringify(v any) string {
	return fmt.Sprintf("%v", v)
}
