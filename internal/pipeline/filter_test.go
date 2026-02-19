package pipeline

import (
	"testing"

	"magent/internal/match"
)

// TestIsVariableAllowed_FilterAndDrop verifies filter_var/drop_var precedence.
// Params: testing.T for assertions.
// Returns: none.
func TestIsVariableAllowed_FilterAndDrop(t *testing.T) {
	filterVar := compileWildcardPatterns([]string{"rx_*"})
	dropVar := compileWildcardPatterns([]string{"rx_err"})

	allowed := isVariableAllowedCompiled("rx_bytes_per_sec", filterVar, dropVar)
	if !allowed {
		t.Fatalf("expected rx_bytes_per_sec to pass filter_var")
	}

	dropped := isVariableAllowedCompiled("rx_err", filterVar, dropVar)
	if dropped {
		t.Fatalf("expected rx_err to be dropped by drop_var")
	}

	notFiltered := isVariableAllowedCompiled("tx_bytes_per_sec", filterVar, nil)
	if notFiltered {
		t.Fatalf("expected tx_bytes_per_sec to be dropped by filter_var")
	}
}

// TestDropCondition_KeyWildcard verifies wildcard evaluation on key.
// Params: testing.T for assertions.
// Returns: none.
func TestDropCondition_KeyWildcard(t *testing.T) {
	condition, err := parseDropCondition("key!=core*")
	if err != nil {
		t.Fatalf("parse condition: %v", err)
	}

	drop := shouldDropEvent([]DropCondition{condition}, EventEvalContext{
		Metric: "cpu",
		Key:    "total",
		Data: map[string]map[string]any{
			"util": {"last": uint8(20)},
		},
	})
	if !drop {
		t.Fatalf("expected drop for key total with key!=core*")
	}
}

// TestDropCondition_NumericAndVar verifies numeric and var conditions.
// Params: testing.T for assertions.
// Returns: none.
func TestDropCondition_NumericAndVar(t *testing.T) {
	numeric, err := parseDropCondition("iops>100")
	if err != nil {
		t.Fatalf("parse numeric condition: %v", err)
	}
	variable, err := parseDropCondition("var=rx_*")
	if err != nil {
		t.Fatalf("parse var condition: %v", err)
	}

	data := map[string]map[string]any{
		"iops":             {"last": uint64(150)},
		"rx_bytes_per_sec": {"last": uint64(1)},
	}

	if !shouldDropEvent([]DropCondition{numeric}, EventEvalContext{Data: data}) {
		t.Fatalf("expected numeric condition to drop event")
	}
	if !shouldDropEvent([]DropCondition{variable}, EventEvalContext{Data: data}) {
		t.Fatalf("expected var condition to drop event")
	}
}

// TestWildcardMatch_ComplexPatterns verifies matcher behavior for multiple '*' segments.
// Params: testing.T for assertions.
// Returns: none.
func TestWildcardMatch_ComplexPatterns(t *testing.T) {
	testCases := []struct {
		pattern string
		value   string
		match   bool
	}{
		{pattern: "*", value: "any", match: true},
		{pattern: "core*", value: "core10", match: true},
		{pattern: "*postgres*", value: "db-postgres-main", match: true},
		{pattern: "db*main", value: "db-postgres-main", match: true},
		{pattern: "db*main", value: "db-postgres-replica", match: false},
		{pattern: "*rx*err*", value: "net_rx_err_total", match: true},
		{pattern: "eth*1", value: "eth0", match: false},
	}

	for _, testCase := range testCases {
		got := match.WildcardMatch(testCase.pattern, testCase.value)
		if got != testCase.match {
			t.Fatalf(
				"unexpected wildcard result pattern=%q value=%q got=%v want=%v",
				testCase.pattern,
				testCase.value,
				got,
				testCase.match,
			)
		}
	}
}
