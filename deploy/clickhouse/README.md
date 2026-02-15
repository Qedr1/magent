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

## Create script metric tables
Script metrics use the same schema; the table name must match `event.metric` (e.g. `[[metrics.script.db]]` -> table `db`).

- Create one:
  - `bash ./deploy/clickhouse/create_builtin_tables.sh metrics "db"`
- Create multiple:
  - `bash ./deploy/clickhouse/create_builtin_tables.sh metrics "db,chaos"`
