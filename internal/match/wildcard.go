package match

import "strings"

// WildcardMatch evaluates '*' wildcard pattern against value.
// Params: pattern may contain '*' wildcards; value is compared text.
// Returns: true on pattern match.
func WildcardMatch(pattern, value string) bool {
	p := strings.TrimSpace(pattern)
	if p == "" {
		return false
	}
	if p == "*" {
		return true
	}

	parts := strings.Split(p, "*")
	anchoredStart := !strings.HasPrefix(p, "*")
	anchoredEnd := !strings.HasSuffix(p, "*")

	cursor := 0
	partIndex := 0

	if anchoredStart {
		startPart := parts[0]
		if !strings.HasPrefix(value, startPart) {
			return false
		}
		cursor = len(startPart)
		partIndex = 1
	}

	lastIndex := len(parts) - 1
	loopLimit := len(parts)
	if anchoredEnd {
		loopLimit = lastIndex
	}

	for ; partIndex < loopLimit; partIndex++ {
		segment := parts[partIndex]
		if segment == "" {
			continue
		}
		offset := strings.Index(value[cursor:], segment)
		if offset < 0 {
			return false
		}
		cursor += offset + len(segment)
	}

	if anchoredEnd {
		endPart := parts[lastIndex]
		if endPart == "" {
			return true
		}
		return strings.HasSuffix(value, endPart)
	}

	return true
}
