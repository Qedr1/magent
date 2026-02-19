#!/usr/bin/env bash
set -euo pipefail

# Creates raw netflow table plus analytics table and materialized view:
#   raw metric table (`netflow` by default) -> `netflow_pairs` rows parsed from composed key.
# Params:
#   $1 - database name (default: metrics)
#   $2 - raw table name (default: netflow)
#   $3 - pairs table name (default: netflow_pairs)
#   $4 - materialized view name (default: mv_netflow_pairs)

DB_NAME="${1:-metrics}"
RAW_TABLE="${2:-netflow}"
PAIRS_TABLE="${3:-netflow_pairs}"
MV_TABLE="${4:-mv_netflow_pairs}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCHEMA_RAW="${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh"
SCHEMA_PAIRS="${ROOT_DIR}/deploy/clickhouse/schema_netflow_pairs.sql"
TMP_SQL="/tmp/schema-netflow-pairs-${DB_NAME}-${PAIRS_TABLE}.sql"

bash "${SCHEMA_RAW}" "${DB_NAME}" "${RAW_TABLE}"

sed \
  -e "s/__DB__/${DB_NAME}/g" \
  -e "s/__RAW_TABLE__/${RAW_TABLE}/g" \
  -e "s/__PAIRS_TABLE__/${PAIRS_TABLE}/g" \
  -e "s/__MV_TABLE__/${MV_TABLE}/g" \
  "${SCHEMA_PAIRS}" > "${TMP_SQL}"

clickhouse-client --multiquery < "${TMP_SQL}"
rm -f "${TMP_SQL}"

echo "Created: ${DB_NAME}.${RAW_TABLE} + ${DB_NAME}.${PAIRS_TABLE} via ${DB_NAME}.${MV_TABLE}"
