package match

import "strings"

// WildcardPattern is a compiled '*' wildcard matcher.
// Params: internal split parts and anchor flags.
// Returns: reusable matcher for many Match calls.
type WildcardPattern struct {
	parts         []string
	anchoredStart bool
	anchoredEnd   bool
	matchAll      bool
}

// CompileWildcard compiles pattern into reusable wildcard matcher.
// Params: pattern may contain '*' wildcards.
// Returns: compiled matcher and false when pattern is empty.
func CompileWildcard(pattern string) (WildcardPattern, bool) {
	p := strings.TrimSpace(pattern)
	if p == "" {
		return WildcardPattern{}, false
	}
	if p == "*" {
		return WildcardPattern{matchAll: true}, true
	}

	return WildcardPattern{
		parts:         strings.Split(p, "*"),
		anchoredStart: !strings.HasPrefix(p, "*"),
		anchoredEnd:   !strings.HasSuffix(p, "*"),
	}, true
}

// Match evaluates compiled wildcard pattern against value.
// Params: value is compared text.
// Returns: true on pattern match.
func (p WildcardPattern) Match(value string) bool {
	if p.matchAll {
		return true
	}
	if len(p.parts) == 0 {
		return false
	}

	cursor := 0
	partIndex := 0

	if p.anchoredStart {
		startPart := p.parts[0]
		if !strings.HasPrefix(value, startPart) {
			return false
		}
		cursor = len(startPart)
		partIndex = 1
	}

	lastIndex := len(p.parts) - 1
	loopLimit := len(p.parts)
	if p.anchoredEnd {
		loopLimit = lastIndex
	}

	for ; partIndex < loopLimit; partIndex++ {
		segment := p.parts[partIndex]
		if segment == "" {
			continue
		}
		offset := strings.Index(value[cursor:], segment)
		if offset < 0 {
			return false
		}
		cursor += offset + len(segment)
	}

	if p.anchoredEnd {
		endPart := p.parts[lastIndex]
		if endPart == "" {
			return true
		}
		return strings.HasSuffix(value, endPart)
	}

	return true
}

// WildcardMatch evaluates '*' wildcard pattern against value.
// Params: pattern may contain '*' wildcards; value is compared text.
// Returns: true on pattern match.
func WildcardMatch(pattern, value string) bool {
	compiled, ok := CompileWildcard(pattern)
	if !ok {
		return false
	}
	return compiled.Match(value)
}
