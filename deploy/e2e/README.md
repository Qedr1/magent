# Full Local E2E

- Script: `deploy/e2e/run_agent_vector_clickhouse.sh`
- Flow: `agent -> Vector -> ClickHouse`
- Default check table: `metrics.cpu`

## Run
- `bash ./deploy/e2e/run_agent_vector_clickhouse.sh`

## Custom target
- `bash ./deploy/e2e/run_agent_vector_clickhouse.sh <db_name> <table_name>`

## Extended all-metrics + batch/queue checks

- Script: `deploy/e2e/run_all_metrics_queue_batch.sh`
- Covers:
  - table creation and ingestion check for `cpu,ram,swap,net,disk,fs,process,db`
  - batch behavior validation (`max_events` vs `max_age`)
  - queue behavior validation (collector down -> queue growth, recovery -> drain)
- Run:
  - `bash ./deploy/e2e/run_all_metrics_queue_batch.sh`
  - `bash ./deploy/e2e/run_all_metrics_queue_batch.sh <db_name>`

## P#19 max-load benchmark

- Script: `deploy/e2e/run_p19_max_load.sh`
- Covers:
  - high-load run for all built-in metrics + process + script
  - inserts into `cpu,ram,swap,net,disk,fs,process,db`
  - prints per-table rows, total rows and approximate rows/sec
- Run:
  - `bash ./deploy/e2e/run_p19_max_load.sh`
  - `bash ./deploy/e2e/run_p19_max_load.sh <db_name> <duration_seconds>`

## Collector delivery modes

- Script: `deploy/e2e/run_collector_delivery_modes.sh`
- Covers:
  - failover inside one `[[collector]]` with `addr=[down,up]`
  - delivery into two independent `[[collector]]` sections
- Run:
  - `bash ./deploy/e2e/run_collector_delivery_modes.sh`
  - `bash ./deploy/e2e/run_collector_delivery_modes.sh <failover_db> <multi_db_a> <multi_db_b>`
