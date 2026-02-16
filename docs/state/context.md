# Project Context (magent)

This file is the single, self-contained global technical spec + current state for the project (agent-only source of truth).
If a detail is not stated here: treat it as unknown and ask the user.

## Stack/Versions (validated in this env)
- Go: 1.23.1 (`go.mod`)
- Vector: 0.53.0
- ClickHouse: 25.11.1.558
- gopsutil: v4.25.9

## Repo Map
- CLI: `cmd/magent/main.go` (`-config`, `-version`)
- Runtime: `internal/app/*` (config+logger+pprof+pipeline)
- Config: `internal/config/config.go`; example: `config.example.toml`
- Pipeline: `internal/pipeline/*` (workers, window aggregation, filters, collector sink, queue, sender, http ingest)
- Metrics: `internal/metrics/*` (built-ins incl. AF_PACKET netflow + script + http_client + shared typed points JSON parser)
- Match utils: `internal/match/*` (shared wildcard matcher used by filters and collectors)
- Vector configs: `deploy/vector/*` (Vector Protocol v2 receiver + VRL flatten)
- ClickHouse DDL/scripts: `deploy/clickhouse/*`
- Tests (bash e2e): `docs/tests/*`
- Examples: `docs/example/*` (script + built-in netflow + vector monitoring configs)
- Roadmap: `docs/state/detailed_plan.md`; changelog: `docs/state/changelog.md`

## Architecture/Flow
1. Pull scrape tick (built-ins incl. netflow/script/http-client) OR push ingest (http-server): source -> []Point{key, values[var]=Value{raw,kind}}.
2. Window aggregator: append samples per `{key,var}` until send tick.
3. Worker send tick: aggregate window -> Event -> sinks:
   - CollectorSink (fan-out to each `[[collector]]`; batch/failover/queue)
   - LogSink (debug-only JSON)
4. Transport: Vector Protocol v2 over gRPC -> Vector `source=vector(version=2)` -> VRL `remap` flattens -> ClickHouse sink inserts into per-metric tables.

MultiSink dispatch is sequential (no goroutine-per-event overhead). CollectorSink Consume blocks on backpressure (per-collector channel buffer: 4096 events).
Collector delivery path:
- Live in-memory batch uses `SendBatch` fast-path (direct protobuf request build + gRPC push; no intermediate payload decode).
- Disk queue/retry path keeps encoded payload (`Encode` + `Send`) for durability format compatibility.

## Time/Window Semantics
- `dt` (event time): Unix epoch milliseconds; first sample time in the send window (scrapeAt for pull, ingestAt for push); fallback `sendAt - interval`. Enforced: `dt < dts`.
- `dts` (send time): Unix epoch seconds; set at emit.
- Percentiles are computed once per send window (not per scrape tick).

## Event/Data Contract
- Internal event (`internal/pipeline/event.go`): `dt,dts,metric,dc,host,project,role,key,data`.
- `key` is always non-empty string: `trim(key)`; empty -> `"total"`.
- `data` shape: `map[var]map[agg]value`:
  - aggs: always `last`; plus `pXX` for configured percentiles.
  - algorithm: nearest-rank percentile over sorted samples; if samples `<4` => all `pXX=0`.
  - normalization: round; clamp negatives to 0; KindPercent additionally clamps to 0..100 and emits `uint8`, KindNumber emits `uint64`.
  - `0` is valid and preserved.
- Transport adds `host_ip` (string) on send: sender dials collector address and injects local source IP into Vector log payload (not part of internal Event struct).
- Encoding guard: outbound Vector integer field is `int64`; any `uint64 > MaxInt64` is rejected at encode time (batch is dropped with error log).

## Config (TOML; `${VAR}` expanded before decode)
Load pipeline: read file -> `os.ExpandEnv` -> TOML decode -> defaults -> validation (`internal/config/config.go`).

### `[global]` (required)
- required: `dc`, `project`, `role` (non-empty)
- optional: `host` (default `os.Hostname()`)

### Logging
- `[log.console]`: `enabled`, `level=debug|info|warn|error|panic`, `format=line|json`
- `[log.file]`: same + `path` (required when enabled)
- defaults: console `enabled` if both sinks disabled; console `level=info`, `format=line`; file `format=json`.
- console `line` is colored (level + token highlighting); JSON never colored.

### Pprof
- `[pprof]`: `enabled`, `listen` (host:port; default `127.0.0.1:6060` when enabled)
- endpoints: `/debug/pprof/*`

### ClickHouse (tooling-only for now)
- `[db.clickhouse]`: `host`(127.0.0.1), `port`(8123), `database`(metrics), `user`(default), `password`, `secure`, `dial_timeout`(5s)
- not used by agent runtime today (kept for docs/tests + future)

### Metric workers
- Defaults `[metrics]`: `scrape` (<=0 => 5s), `send` (<=0 => 30s), `percentiles` (optional).
- Common worker fields:
  - `name` (default `<metric>-<idx>`), `scrape`, `send`, `percentiles`
  - `filter_var` (keep patterns), `drop_var` (drop patterns), `drop_event` (OR conditions)
- Percentiles resolution:
  - worker `percentiles = [..]` -> use worker list
  - worker `percentiles = []` -> disable percentiles for worker (last-only)
  - worker `percentiles` omitted -> inherit `[metrics].percentiles`
  - both omitted/empty -> last-only (no `pXX` keys)

Metrics:
- `[[metrics.cpu]]` -> metric `cpu`
- `[[metrics.ram]]` -> `ram`
- `[[metrics.swap]]` -> `swap`
- `[[metrics.net]]` -> `net`
- `[[metrics.netflow]]` -> `netflow` (AF_PACKET raw capture; no cgo)
- `[[metrics.disk]]` -> `disk`
- `[[metrics.fs]]` -> `fs`

Process metric (special):
- `[[metrics.process]]`:
  - thresholds: `cpu_util`/`ram_util` (0..100), `iops` (>=0); OR logic on per-window max
  - if no thresholds set => worker skipped (warn)
  - default scrape interval: 20s (unless overridden), independent from `[metrics].scrape`
  - `KeepKnown=false` (no zero backfill)

Script metrics:
- `[[metrics.script.<metric_name>]]`:
  - required: `path`; `timeout>0` (default 5s); `env` map (default `{}`)
  - `<metric_name>` becomes `event.metric` and ClickHouse table name; keep it ClickHouse-safe (recommended: `[a-z][a-z0-9_]*`)

HTTP server metrics (push):
- `[[metrics.http_server.<metric_name>]]`:
  - required: `listen` (host:port), `path` (starts with `/`)
  - schedule: only `send` (no `scrape`); `max_pending` (default 4096) limits accepted batches in memory
  - policy on overload: keep old / drop new (HTTP `503`), success returns `204`
  - accepts `POST` body JSON in the External Metric JSON Contract below

HTTP client metrics (poll):
- `[[metrics.http_client.<metric_name>]]`:
  - required: `url`, `timeout>0` (default 5s)
  - optional: `format=json|prometheus` (default `json`)
  - optional for `format=prometheus`: `include=[metric_names...]` (required in this mode), `key_from_labels=[label,...]`, `var_mode=full|short` (default `full`)
  - schedule: `scrape` + `send`
  - HTTP: `GET` only; non-2xx is scrape error
  - `url` supports placeholders (path-escaped): `{dc},{host},{project},{role},{metric},{instance}`
  - `format=json`: response uses the External Metric JSON Contract below
  - `format=prometheus`: parse text exposition; only `counter` and `gauge` are ingested; each sample becomes one point (`key` from labels or `"total"`, `var` from metric name)

Netflow metric (pull):
- `[[metrics.netflow]]`:
  - required: `ifaces=[pattern,...]` (wildcards supported, eg `eth*`,`enp*`,`lo`)
  - optional: `top_n` (default 20), `scrape`, `send`, worker filters
  - default aggregation mode: last-only (`percentiles=[]` recommended)
  - runtime privilege: raw packet capture requires `root` or `CAP_NET_RAW`

### Collectors / Delivery
- `[[collector]]` (at least one required):
  - `name` (default `collector-<idx>`), `addr=[host:port,...]` (failover order), `timeout` (default 5s), `retry_interval` (default 3s)
- Batching `[collector.batch]`:
  - flush when `len(batch) >= max_events` OR `time_since(batch_start) >= max_age` (age check tick: 1s)
  - defaults: `max_events=200`, `max_age=5s`; must have `max_events>0` or `max_age>0`
- Disk queue `[collector.queue]` (optional durability):
  - `enabled`, `dir`, limits `max_events` and/or `max_age` (required when enabled)
  - files: `queue.bin` (records) + `offset.bin` (read offset); format `[u32 payload_len][u64 created_unix_sec] + payload`
  - drain on start + every `retry_interval`; full drain => truncate to 0 + offset reset
  - startup reindex scans from offset and truncates corrupted tail if found
  - best-effort: if enqueue fails (limits/IO) the batch is dropped (logged)

## Filters
- `filter_var` / `drop_var`: wildcard `*` match on var names (no regex).
- `drop_event`: OR list of `<field><op><value>`; ops `= != > <`.
  - string fields: `metric`, `key` (`*` wildcard only for `=`/`!=`)
  - `var`: matches against var names present (only `=`/`!=`)
  - any other field name is treated as `<var_name>` and compared against that var's `last` only.
- wildcard matcher implementation is shared (`internal/match/wildcard.go`) across filter engine and netflow iface matching.

## Built-in Metric Semantics
Keys are always strings; values are normalized as above.

- `cpu`: key `total` + `coreN`; var: `util` (%)
- `ram`: key `total`; vars: `total,used,free` (bytes), `util` (%)
- `swap`: key `total`; vars: `total,used` (bytes), `util` (%)
- `net`: key `<iface>`; vars:
  - `tx_bytes,rx_bytes` (delta bytes since previous scrape)
  - `tx_bytes_per_sec,rx_bytes_per_sec` (bytes/s)
  - `tx_pkt,rx_pkt` (pkt/s)
  - `tx_err,rx_err,tx_drop,rx_drop` (delta counters)
- `netflow`: key `iface|proto|src_ip|src_port|dst_ip|dst_port`; vars: `bytes,packets,flows` (window top-N by bytes; each emitted key has `flows=1`)
- `disk`: key `/dev/<name>`; vars:
  - `rx_io,tx_io` (ops/s)
  - `rx_bytes,tx_bytes` (delta bytes), `rx_bytes_per_sec,tx_bytes_per_sec` (bytes/s)
  - `rx_await,tx_await,await` (ms), `qdepth` (avg), `util` (%), `inflight` (count)
- `fs`: key `<mountpoint>`; vars:
  - `total,used,free,avail` (bytes; `avail==free`), `util` (%)
  - `inodes_total,inodes_used,inodes_free` (count), `inodes_util` (%)
  - `readonly` (0/1)
- `process`: key `proc.Name` (not pid/cmdline); vars: `cpu_util` (% clamped 0..100), `ram_util` (% of host), `iops` (ops/s). Note: multiple PIDs with same Name collide under same key.

## External Metric JSON Contract (script/http)
- Used by: script stdout, http-server POST body, http-client GET response.
- Max payload size: 16 MiB (`metrics.MaxPointsJSONBytes`).
- root: object or array of objects (each object -> one Point)
- object fields: `key` (string, non-empty), `data` (object, non-empty)
- `data.<var>` supports:
  - bool -> 0/1
  - number (finite)
  - object: `{value|last: number, kind?: string}` where kind in `number|num|uint64|percent|pct|%|uint8_percent`
- Kind inference when kind absent: `util` or `*_util` => percent; else number.
- Parser implementation uses typed `json.RawMessage` decode path (no `map[string]any` contract walk).

## Vector (Collector) Side: VRL Flattening (no custom parsers)
- Vector Protocol v2 source listens on `0.0.0.0:6000` in provided configs.
- Flatten is VRL `remap` only (no `route`):
  - `base_event = .; del(base_event.data)`
  - for each `.data[var][agg]` -> emit one row event:
    - `event = merge(base_event, {"var":var_name,"agg":agg_name,"value":to_int(agg_value) ?? 0})`
  - `. = events`
- `key` is preserved via `base_event`.
- ClickHouse sink (e2e configs): `table = "{{ .metric }}"`, `skip_unknown_fields=true`, `date_time_best_effort=true`.
- Real runtime collector config (`deploy/vector/clickhouse-e2e.toml`) also exposes Vector internal metrics:
  - source: `internal_metrics`
  - sink: `prometheus_exporter` on `127.0.0.1:19598` (`default_namespace="vector"`)

## ClickHouse Schema/Retention
- Table-per-metric; schema is identical across all metric tables.
- Template: `deploy/clickhouse/schema_metric.sql`:
  - `dt DateTime64(3) CODEC(DoubleDelta)`, `dts DateTime CODEC(DoubleDelta)`, `dtv DateTime DEFAULT now() CODEC(DoubleDelta)`
  - tags: `dc,host,project,role` (LowCardinality String), `host_ip IPv6 DEFAULT ::`
  - payload: `key,var,agg` (LowCardinality String), `value UInt64`
  - `PARTITION BY toYYYYMMDD(dt)`; `ORDER BY (dt, host, key)`; `TTL dt + INTERVAL 4 MONTH`
- Bootstrap:
  - built-ins: `bash deploy/clickhouse/create_builtin_tables.sh <db> "cpu,ram,swap,net,netflow,disk,fs,process"`
  - script metrics use the same DDL: `bash deploy/clickhouse/create_builtin_tables.sh <db> "<metric_name_1,metric_name_2>"`

## Ops (Build/Run/Test/E2E)
- Build: `make build` -> `bin/magent` (prod flags); optional `make build-upx`.
- Run: `./bin/magent -config <path>` (default config path: `config.toml`).
- Unit checks (2026-02-16): `go test ./...`, `go vet ./...` PASS.
- E2E scripts:
  - `bash docs/tests/run_agent_vector_clickhouse.sh [db] [table]`
  - `bash docs/tests/run_all_metrics_queue_batch.sh [db]`
  - `bash docs/tests/run_collector_delivery_modes.sh [failover_db] [multi_db_a] [multi_db_b]`
  - `bash docs/tests/run_http_server_e2e.sh [db]`
  - `bash docs/tests/run_http_client_e2e.sh [db]`
  - `bash docs/tests/run_http_client_vector_prom_e2e.sh [db]`
  - `bash docs/tests/run_netflow_pairs_e2e.sh [db]` (http-ingest raw->MV pairs path)
  - `bash docs/tests/run_netflow_builtin_e2e.sh [db]` (built-in AF_PACKET netflow path)
  - `bash docs/tests/run_p19_max_load.sh [db] [duration_s]`
  - `bash docs/tests/run_soak_pprof.sh [db] [soak_seconds] [cpu_profile_seconds]` -> `/tmp/magent-soak-pprof/*`
  - `bash docs/tests/chaos_failover/run.sh [chaos_seconds] [drain_timeout_s]`

Known test-script quirks (do not change semantics):
- `docs/tests/chaos_failover/run.sh` progress log checks queue bytes in `${QUEUE_DIR}/events.bin` but actual queue file is `${QUEUE_DIR}/queue.bin`; PASS criteria is distinct delivered keys, not queue_bytes.

## Performance State (P#28)
- Applied optimizations:
  - sender fast-path for live batches (`SendBatch`) with queue-path compatibility kept.
  - typed external JSON parsing (`json.RawMessage`) for script/http metrics.
  - window emit allocation cut: avoid building `seriesMap` unless `EmitFilter` is set.
  - script collector env cached at collector construction.
- 10-minute extreme all-metrics reprofiling:
  - baseline DB: `metrics_profile_10m`; optimized DB: `metrics_profile_10m_p28`
  - data checks: all metric tables populated; `countIf(dt>=dts)=0`; `netflow` has only `agg=last`.
  - pprof deltas:
    - CPU samples: `1.85s -> 1.63s` (`-11.89%`)
    - heap `alloc_space` total: `2292.97MB -> 1734.21MB` (`-24.37%`)
- P#40 optimization/reliability wave applied:
  - reliability: graceful final collector flush on shutdown now uses bounded background timeout context; constructor cleanup closes already-opened queue/listener resources on build errors.
  - runtime perf: wildcard/drop filters are precompiled; `http_client` Prometheus parser is precompiled per collector; netflow uses sharded counters and top-N min-heap selection.
  - validation: `go test ./...`, `go vet ./...`, short soak+pprof (`docs/tests/run_soak_pprof.sh metrics 120 20`) and high-load pprof run (`/tmp/magent-p40-pprof/*`) passed; dominant CPU remains syscall/gopsutil-heavy, expected for current collector model.

## Project plan (status snapshot)
- Detailed roadmap: `docs/state/detailed_plan.md`.
- Current: P#1..P#20 DONE; P#21 OPEN; P#22..P#28 DONE; P#29 OPEN; P#31 DONE; P#33..P#35 DONE; P#37..P#40 DONE.
