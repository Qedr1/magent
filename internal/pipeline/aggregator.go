package pipeline

import (
	"math"
	"sort"
	"strconv"
	"sync"

	"magent/internal/metrics"
)

var percentileSortBufferPool = sync.Pool{
	New: func() any {
		return make([]float64, 0, 64)
	},
}

type series struct {
	kind   metrics.ValueKind
	values []float64
}

// aggregateSeries computes `last` and configured percentiles for one series.
// Params: samples series data and percentile list.
// Returns: normalized aggregate map with `last` and `pXX`.
func aggregateSeries(samples series, percentiles []int) map[string]any {
	aggregated := make(map[string]any, len(percentiles)+1)

	if len(samples.values) == 0 {
		aggregated["last"] = normalizeValue(0, samples.kind)
		for _, p := range percentiles {
			aggregated[percentileKey(p)] = normalizeValue(0, samples.kind)
		}
		return aggregated
	}

	last := samples.values[len(samples.values)-1]
	aggregated["last"] = normalizeValue(last, samples.kind)

	if len(samples.values) < 4 {
		for _, p := range percentiles {
			aggregated[percentileKey(p)] = normalizeValue(0, samples.kind)
		}
		return aggregated
	}

	sortedValues := borrowSortBuffer(len(samples.values))
	copy(sortedValues, samples.values)
	sort.Float64s(sortedValues)

	for _, p := range percentiles {
		aggregated[percentileKey(p)] = normalizeValue(nearestRankPercentile(sortedValues, p), samples.kind)
	}
	releaseSortBuffer(sortedValues)

	return aggregated
}

// borrowSortBuffer returns reusable float buffer for percentile sorting.
// Params: required size.
// Returns: slice with requested length.
func borrowSortBuffer(size int) []float64 {
	buffer := percentileSortBufferPool.Get().([]float64)
	if cap(buffer) < size {
		return make([]float64, size)
	}
	return buffer[:size]
}

// releaseSortBuffer returns float buffer into pool with capacity guard.
// Params: buffer previously borrowed for sorting.
// Returns: none.
func releaseSortBuffer(buffer []float64) {
	const maxPooledCapacity = 1 << 16
	if cap(buffer) > maxPooledCapacity {
		return
	}
	percentileSortBufferPool.Put(buffer[:0])
}

// nearestRankPercentile calculates nearest-rank percentile over sorted values.
// Params: sortedValues sample set sorted ascending and percentile in range 1..100.
// Returns: percentile value.
func nearestRankPercentile(sortedValues []float64, percentile int) float64 {
	rank := int(math.Ceil(float64(percentile) / 100 * float64(len(sortedValues))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sortedValues) {
		rank = len(sortedValues)
	}
	return sortedValues[rank-1]
}

// normalizeValue converts raw sample to output type by kind.
// Params: raw float value and kind selector.
// Returns: uint64 for numbers, uint8 for percents.
func normalizeValue(raw float64, kind metrics.ValueKind) any {
	if raw < 0 {
		raw = 0
	}

	rounded := math.Round(raw)

	switch kind {
	case metrics.KindPercent:
		if rounded > 100 {
			rounded = 100
		}
		return uint8(rounded)
	default:
		return uint64(rounded)
	}
}

// percentileKey renders percentile key name used in payload.
// Params: percentile integer value.
// Returns: key like p90.
func percentileKey(percentile int) string {
	return "p" + strconv.Itoa(percentile)
}
