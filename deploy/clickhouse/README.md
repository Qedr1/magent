# ClickHouse Schema Setup

## Base DDL template
- File: `deploy/clickhouse/schema_metric.sql`
- Placeholders:
  - `__DB__` - database name
  - `__TABLE__` - metric table name
- Core settings in template:
  - `dt/dts/dtv` use `CODEC(DoubleDelta)`
  - `ORDER BY (dt, host, key)`
  - `TTL dt + INTERVAL 4 MONTH`

## Create built-in metric tables
- Command:
  - `bash ./deploy/clickhouse/create_builtin_tables.sh metrics`
- Default tables: `cpu,ram,swap,net,disk,fs,process`

## Create one script table
- Command:
  - `bash ./deploy/clickhouse/create_script_table.sh metrics db`
- Here `db` is script metric name from `[[metrics.script.db]]`
