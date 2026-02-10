package metrics

import "context"

// ValueKind identifies normalization kind for a metric value.
// Params: none.
// Returns: enum value for number/percent normalization.
type ValueKind uint8

const (
	// KindNumber represents non-percent numeric values normalized to uint64.
	KindNumber ValueKind = iota
	// KindPercent represents percent values normalized to uint8.
	KindPercent
)

// Value carries one numeric sample and its normalization kind.
// Params: raw float sample and kind.
// Returns: typed metric value for aggregation.
type Value struct {
	Raw  float64
	Kind ValueKind
}

// Point is one keyed metric sample with variable set.
// Params: key string and variable->value map.
// Returns: one scrape sample entity.
type Point struct {
	Key    string
	Values map[string]Value
}

// Collector scrapes one metric and returns keyed points.
// Params: context for cancellation and deadlines.
// Returns: point list or scrape error.
type Collector interface {
	Name() string
	Scrape(ctx context.Context) ([]Point, error)
}
