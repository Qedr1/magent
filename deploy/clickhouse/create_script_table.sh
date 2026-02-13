#!/usr/bin/env bash
set -euo pipefail

# Creates one script metric table in ClickHouse.
# Params:
#   $1 - database name (required)
#   $2 - table name (required, script metric name)
# Return:
#   0 on success; non-zero on validation/client/query failure.

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <db_name> <table_name>" >&2
  exit 1
fi

DB_NAME="$1"
TABLE_NAME="$2"
CLICKHOUSE_CLIENT_BIN="${CLICKHOUSE_CLIENT_BIN:-clickhouse-client}"

if [[ -z "${DB_NAME}" || -z "${TABLE_NAME}" ]]; then
  echo "db_name and table_name must be non-empty" >&2
  exit 1
fi

"${CLICKHOUSE_CLIENT_BIN}" --query "CREATE DATABASE IF NOT EXISTS ${DB_NAME}"
"${CLICKHOUSE_CLIENT_BIN}" --query "
CREATE TABLE IF NOT EXISTS ${DB_NAME}.${TABLE_NAME}
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
