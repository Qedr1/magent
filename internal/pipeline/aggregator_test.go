package pipeline

import (
	"testing"

	"magent/internal/metrics"
)

// TestAggregateSeries_PercentilesNeedAtLeastFourSamples verifies pXX gate logic.
// Params: testing.T for assertions.
// Returns: none.
func TestAggregateSeries_PercentilesNeedAtLeastFourSamples(t *testing.T) {
	values := series{
		kind:   metrics.KindNumber,
		values: []float64{1, 2, 3},
	}

	out := aggregateSeries(values, []int{50, 90})

	if got := out["last"].(uint64); got != 3 {
		t.Fatalf("unexpected last: %d", got)
	}
	if got := out["p50"].(uint64); got != 0 {
		t.Fatalf("unexpected p50: %d", got)
	}
	if got := out["p90"].(uint64); got != 0 {
		t.Fatalf("unexpected p90: %d", got)
	}
}

// TestAggregateSeries_PercentNormalization verifies percent clamping and type.
// Params: testing.T for assertions.
// Returns: none.
func TestAggregateSeries_PercentNormalization(t *testing.T) {
	values := series{
		kind:   metrics.KindPercent,
		values: []float64{10, 20, 150, 90},
	}

	out := aggregateSeries(values, []int{50})

	if got := out["last"].(uint8); got != 90 {
		t.Fatalf("unexpected last: %d", got)
	}
	// Sorted values are 10,20,90,150 -> nearest rank p50 index 2 => 20.
	if got := out["p50"].(uint8); got != 20 {
		t.Fatalf("unexpected p50: %d", got)
	}
}

// TestAggregateSeries_ZeroSampleIsValid verifies that zero participates in percentile.
// Params: testing.T for assertions.
// Returns: none.
func TestAggregateSeries_ZeroSampleIsValid(t *testing.T) {
	values := series{
		kind:   metrics.KindNumber,
		values: []float64{0, 10, 20, 30},
	}

	out := aggregateSeries(values, []int{25})

	// nearest rank p25 with n=4 -> rank 1 -> first sorted value 0.
	if got := out["p25"].(uint64); got != 0 {
		t.Fatalf("unexpected p25: %d", got)
	}
}

// TestAggregateSeries_WithoutPercentiles verifies last-only aggregation mode.
// Params: testing.T for assertions.
// Returns: none.
func TestAggregateSeries_WithoutPercentiles(t *testing.T) {
	values := series{
		kind:   metrics.KindNumber,
		values: []float64{1, 2, 3, 4},
	}

	out := aggregateSeries(values, nil)

	if len(out) != 1 {
		t.Fatalf("expected only last aggregate, got keys=%v", out)
	}
	if got := out["last"].(uint64); got != 4 {
		t.Fatalf("unexpected last: %d", got)
	}
	if _, exists := out["p50"]; exists {
		t.Fatalf("did not expect percentile keys in last-only mode: %v", out)
	}
}
