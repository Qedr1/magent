#!/usr/bin/env bash
set -euo pipefail

# Creates standard metric tables in ClickHouse.
# Params:
#   $1 - database name (default: metrics)
#   $2 - optional comma-separated table list (default: cpu,ram,swap,kernel,net,netflow,disk,fs,process)
# Return:
#   0 on success; non-zero on client/query failure.

DB_NAME="${1:-metrics}"
TABLE_LIST_RAW="${2:-cpu,ram,swap,kernel,net,netflow,disk,fs,process}"
CLICKHOUSE_CLIENT_BIN="${CLICKHOUSE_CLIENT_BIN:-clickhouse-client}"

IFS=',' read -r -a TABLES <<< "$TABLE_LIST_RAW"

"${CLICKHOUSE_CLIENT_BIN}" --query "CREATE DATABASE IF NOT EXISTS ${DB_NAME}"

for table in "${TABLES[@]}"; do
  table_trimmed="$(echo "${table}" | xargs)"
  if [[ -z "${table_trimmed}" ]]; then
    continue
  fi

  "${CLICKHOUSE_CLIENT_BIN}" --query "
CREATE TABLE IF NOT EXISTS ${DB_NAME}.${table_trimmed}
(
    dt DateTime64(3) CODEC(DoubleDelta),
    dts DateTime CODEC(DoubleDelta),
    dtv DateTime DEFAULT now() CODEC(DoubleDelta),
    dc LowCardinality(String),
    host LowCardinality(String),
    project LowCardinality(String),
    role LowCardinality(String),
    host_ip IPv6 DEFAULT toIPv6('::'),
    key LowCardinality(String),
    var LowCardinality(String),
    agg LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY (dt, host, key)
TTL dt + INTERVAL 4 MONTH"
done
