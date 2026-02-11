# PASP Context

## Stack/Versions
- Go: 1.23.1
- Vector: 0.53.0
- ClickHouse client: 25.11.1.558
- gopsutil: v4.25.9

## Runtime/Config
- Runtime: Linux; config format: TOML with `${VAR}` expansion.
- Required global tags: `dc`, `project`, `role`; `host` fallback: `os.Hostname()`.
- Worker model: one async worker per metric instance; global `scrape/send` with per-worker override.
- Delivery model: per-collector batching + failover by `addr[]` + optional disk queue per collector.

## Invariants
- Unified event schema from agent: `dt,dts,metric,dc,host,project,role,key,data`.
- Built-in metric names are emitted in lower-case: `cpu,ram,swap,net,disk,fs,process`.
- `key` is mandatory string; default `"total"` if natural key is absent.
- `data` shape: `var -> {last,pXX...}`.
- NET/DISK byte metrics are unified: `rx_bytes`,`tx_bytes`,`rx_bytes_per_sec`,`tx_bytes_per_sec`.
- Output normalization: non-percent -> `uint64`, percent -> `uint8`, math rounding.
- `0` is a valid sample; if samples `<4`, all `pXX=0`.
- Percentiles are computed at send window, not at scrape tick.
- PROCESS emit: OR-threshold over `cpu_util|ram_util|iops`; no thresholds -> worker skipped.

## Current Implementation State
- Core runtime/config/logger: implemented and validated.
- Collectors implemented: `cpu,ram,swap,net,disk,fs,process,script`.
- Script metrics: `[[metrics.script.<name>]]` with `path/timeout/env`, strict JSON stdout parsing.
- Filters: `drop_var`, `filter_var`, `drop_event` (`=,!=,>,<`, wildcard `*` for string `=`/`!=`).
- Transport: Vector Protocol v2 gRPC `PushEventsRequest` (`EventWrapper.log`).
- Queue: append-only disk queue per collector, persisted offset, retry drain, reject-new on limits.
- Vector/ClickHouse deploy assets + full local e2e (`agent -> Vector -> ClickHouse`) are present.
- Test tooling moved to `docs/tests/*`; DB bootstrap helper is `.docs/sql/create_db_and_tables.sh`.
- Runtime profiling supported by optional `[pprof]` (`listen` host:port).
- ClickHouse tables use lower-case names and schema: `dt/dts/dtv CODEC(DoubleDelta)`, `ORDER BY (dt, host, key, var)`, `TTL dt + INTERVAL 4 MONTH`.

## Performance State
- gRPC sender: connection reuse per address (drop/reconnect on send error).
- Aggregation: one sort per series for all configured percentiles.
- Wildcard matcher: string matcher (no regex compile in hot path).
- Log sink: JSON marshal only when debug level is enabled.
- Queue IO: persistent descriptors + batched/time-based offset sync + close-time forced flush.
- Collector backpressure: no immediate drop on full channel; wait for space or context cancel.
- MultiSink dispatch: sequential (no goroutine-per-event overhead).
- PROCESS collector: pid metadata cache for `name/exe`.

## Spec Sync (README)
- Queue finalization documented as `truncate + offset reset` (not file delete).
- Internal collector buffer behavior documented as backpressure.
- Vector transform documented as VRL `remap` without `route`.

## Execution detailed plan
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

## Validation
- Latest checks pass: `go test ./...`, `go test -race ./...`, `go vet ./...`.
- E2E pass: `run_agent_vector_clickhouse.sh`, `run_all_metrics_queue_batch.sh`.
- E2E pass: `run_collector_delivery_modes.sh` (failover rows>0, multi rows>0 for both collectors).
- E2E pass: net duplex verification (`download->rx`, `upload->tx`) with DB-backed ratio checks (`status=PASS`).
- P#19 max-load (60s): total `338064` rows, approx `5634.40 rows/s`.
