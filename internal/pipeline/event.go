package pipeline

// Event is the unified agent payload sent to downstream collector.
// Params: normalized metric data and mandatory tags.
// Returns: one metric event payload.
type Event struct {
	DT      uint64                    `json:"dt"`
	DTS     uint64                    `json:"dts"`
	Metric  string                    `json:"metric"`
	DC      string                    `json:"dc"`
	Host    string                    `json:"host"`
	Project string                    `json:"project"`
	Role    string                    `json:"role"`
	Key     string                    `json:"key"`
	Data    map[string]map[string]any `json:"data"`
}
