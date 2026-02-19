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

## Netflow analytics table via materialized view
- Raw intake stays in universal metric table (example: `netflow`) with standard columns `key,var,agg,value`.
- Parsed analytics goes to `netflow_pairs` where composed `key` is split into columns:
  - `iface, proto, src_ip, src_port, dst_ip, dst_port`
  - counters: `bytes, packets, flows`
  - sort key: `ORDER BY (dt, host, iface)` (минимальный ключ для high-ingest)
  - `src_ip` and `dst_ip` are stored as `IPv6` (IPv4 values are normalized to mapped IPv6 form)

Create (raw + pairs + MV):
- `bash ./deploy/clickhouse/create_netflow_pairs.sh metrics netflow netflow_pairs mv_netflow_pairs`

Expected key format in raw table:
- `iface|proto|src_ip|src_port|dst_ip|dst_port`
