# Detailed Plan

Status rules: `OPEN` by default; set to `DONE` only after user OK + tests pass; revert to `OPEN` on regressions.

- P#1 [DONE]: Bootstrap/runtime/config/logging baseline.
- P#2 [DONE]: Core worker lifecycle + window aggregation/normalization.
- P#3 [DONE]: Built-in collectors wave1 (`cpu,ram,swap,net`).
- P#4 [DONE]: Built-in collectors wave2 (`disk,fs,process`) + OR-threshold.
- P#5 [DONE]: Variable/event filters.
- P#6 [DONE]: Collector delivery (batch/failover/queue/Vector gRPC sender).
- P#7 [DONE]: Script metrics end-to-end + config validation.
- P#8 [DONE]: Config/examples/tests for script sections.
- P#9 [DONE]: Console token coloring rules (`string/ip/number`) for line format.
- P#10 [DONE]: Vector intake/flatten/log configuration.
- P#11 [DONE]: Vector flatten smoke + docs/examples.
- P#12 [DONE]: ClickHouse schema/bootstrap scripts.
- P#13 [DONE]: Full e2e `agent -> Vector -> ClickHouse`.
- P#14 [DONE]: Perf hot-path wave1 (`conn reuse`, aggregation, wildcard, log marshal guard).
- P#15 [DONE]: Perf hot-path wave2 (disk queue IO optimization).
- P#16 [DONE]: Perf hot-path wave3 (collector backpressure).
- P#17 [DONE]: Perf hot-path wave4 (MultiSink dispatch simplification).
- P#18 [DONE]: Perf hot-path wave5 (PROCESS metadata cache).
- P#19 [DONE]: Benchmark/pprof baseline vs current and capture numeric delta.
- P#20 [DONE]: Delivery modes coverage (`failover` in one collector + dual collectors fan-out) with unit+e2e checks.
- P#21 [OPEN]: 5h soak run (`scrape=10s`, `send=60s`, `pprof=on`) and final profile review.
- P#22 [DONE]: Add HTTP metrics sources: `http_server` (push) and `http_client` (GET) with shared aggregation/sending rules.
- P#23 [DONE]: Add e2e scripts for `http_server` and `http_client` ensuring ClickHouse rows (including percentiles) match sources.
- P#24 [DONE]: Make metric percentiles optional (`nil/[]` => last-only aggregation, no `pXX`) with tests.
- P#25 [DONE]: Add netflow intake path via `http_server` with ClickHouse materialized view (`netflow` raw -> `netflow_pairs`) and e2e correctness check.
- P#26 [DONE]: Move netflow examples under `docs/example/netflow`, switch `netflow_pairs.src_ip/dst_ip` to `IPv6`, and revalidate e2e in `metrics`.
