# Vector Monitoring via `http_client` (Prometheus)

## Goal
Collect real Vector internal processing metrics into ClickHouse through magent `[[metrics.http_client.<name>]]`.

Flow:
1. Vector exposes internal metrics on `http://127.0.0.1:19598/metrics` (`internal_metrics -> prometheus_exporter`).
2. magent `http_client` scrapes this endpoint with `format = "prometheus"`.
3. magent sends events to collector Vector (`127.0.0.1:6000`).
4. collector Vector flattens and inserts into ClickHouse table `vector_internal`.

## Files
- `docs/example/vector/vector.prometheus-exporter.example.toml`:
  example collector Vector config with Prometheus exporter enabled.
- `docs/example/vector/agent.vector-http-client.example.toml`:
  example magent config section for Vector monitoring via `http_client`.

## Key Points
- Table name in ClickHouse equals metric name from config:
  `[[metrics.http_client.vector_internal]]` -> table `vector_internal`.
- For Prometheus mode use:
  - `format = "prometheus"`
  - `filter_var = [ ... ]` (explicit metric allow-list by variable names)
  - `percentiles = []` (recommended: last-only for counters/gauges)

## Rule Processing Metrics
In Prometheus mode key is fixed (`total`), so query by variable names:
- `vector_component_received_events_total`
- `vector_component_sent_events_total`

Typical vars:
- `vector_component_received_events_total`
- `vector_component_sent_events_total`
- `vector_component_received_event_bytes_total`
- `vector_component_sent_event_bytes_total`
- `vector_component_errors_total`
- `vector_component_discarded_events_total`

Example query:
```sql
SELECT
  var,
  agg,
  value
FROM metrics.vector_internal
WHERE var IN ('vector_component_received_events_total','vector_component_sent_events_total')
ORDER BY dt DESC, var;
```
