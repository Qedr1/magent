# Full Local E2E

- Script: `docs/tests/run_agent_vector_clickhouse.sh`
- Flow: `agent -> Vector -> ClickHouse`
- Default check table: `metrics.cpu`

## Run
- `bash ./docs/tests/run_agent_vector_clickhouse.sh`

## Custom target
- `bash ./docs/tests/run_agent_vector_clickhouse.sh <db_name> <table_name>`

## Extended all-metrics + batch/queue checks

- Script: `docs/tests/run_all_metrics_queue_batch.sh`
- Covers:
  - table creation and ingestion check for `cpu,ram,swap,net,disk,fs,process,db`
  - batch behavior validation (`max_events` vs `max_age`)
  - queue behavior validation (collector down -> queue growth, recovery -> drain)
- Run:
  - `bash ./docs/tests/run_all_metrics_queue_batch.sh`
  - `bash ./docs/tests/run_all_metrics_queue_batch.sh <db_name>`

## P#19 max-load benchmark

- Script: `docs/tests/run_p19_max_load.sh`
- Covers:
  - high-load run for all built-in metrics + process + script
  - inserts into `cpu,ram,swap,net,disk,fs,process,db`
  - prints per-table rows, total rows and approximate rows/sec
- Run:
  - `bash ./docs/tests/run_p19_max_load.sh`
  - `bash ./docs/tests/run_p19_max_load.sh <db_name> <duration_seconds>`

## Collector delivery modes

- Script: `docs/tests/run_collector_delivery_modes.sh`
- Covers:
  - failover inside one `[[collector]]` with `addr=[down,up]`
  - delivery into two independent `[[collector]]` sections
- Run:
  - `bash ./docs/tests/run_collector_delivery_modes.sh`
  - `bash ./docs/tests/run_collector_delivery_modes.sh <failover_db> <multi_db_a> <multi_db_b>`

## Collector Failover Chaos

- Folder: `docs/tests/chaos_failover/`
- Covers:
  - one `[[collector]]` with two addresses
  - random `vector-a/vector-b` stop/start (including both down)
  - no-loss check by deterministic script keys (`expected == delivered distinct`)
- Run:
- `bash ./docs/tests/chaos_failover/run.sh`
- `bash ./docs/tests/chaos_failover/run.sh <chaos_seconds> <drain_timeout_seconds>`

## Long Soak + pprof

- Script: `docs/tests/run_soak_pprof.sh`
- Covers:
  - long run with `scrape=10s`, `send=60s`
  - `pprof` endpoint capture (`cpu`, `heap`, goroutines)
  - ClickHouse row/dt-dts integrity summary
- Run:
  - `bash ./docs/tests/run_soak_pprof.sh`
- `bash ./docs/tests/run_soak_pprof.sh <db_name> <duration_seconds> <cpu_profile_seconds>`

## HTTP metrics e2e

- Script: `docs/tests/run_http_server_e2e.sh`
- Flow: `http-server (POST JSON) -> agent -> Vector -> ClickHouse`
- Run:
  - `bash ./docs/tests/run_http_server_e2e.sh`

- Script: `docs/tests/run_http_client_e2e.sh`
- Flow: `http-client (GET JSON) -> agent -> Vector -> ClickHouse`
- Run:
  - `bash ./docs/tests/run_http_client_e2e.sh`

- Script: `docs/tests/run_http_client_vector_prom_e2e.sh`
- Flow: `Vector internal_metrics + prometheus_exporter -> http-client (GET Prometheus text) -> agent -> Vector -> ClickHouse`
- Run:
  - `bash ./docs/tests/run_http_client_vector_prom_e2e.sh`
  - `bash ./docs/tests/run_http_client_vector_prom_e2e.sh <db_name>`

## Built-in netflow e2e

- Script: `docs/tests/run_netflow_builtin_e2e.sh`
- Flow: `metrics.netflow (AF_PACKET, no cgo) -> agent -> Vector -> ClickHouse (netflow + netflow_pairs MV)`
- Run:
  - `bash ./docs/tests/run_netflow_builtin_e2e.sh`
  - `bash ./docs/tests/run_netflow_builtin_e2e.sh <db_name>`
