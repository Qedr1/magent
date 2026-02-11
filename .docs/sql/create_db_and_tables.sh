#!/usr/bin/env bash
set -euo pipefail

# Creates metrics database and standard tables in ClickHouse.
# Params:
#   $1 - database name (default: metrics)
#   $2 - optional comma-separated script tables (default: db)
# Returns:
#   0 on success; non-zero on setup/query failure.

DB_NAME="${1:-metrics}"
SCRIPT_TABLES_RAW="${2:-db}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "cpu,ram,swap,net,disk,fs,process"

IFS=',' read -r -a SCRIPT_TABLES <<< "${SCRIPT_TABLES_RAW}"
for table_name in "${SCRIPT_TABLES[@]}"; do
  table_trimmed="$(echo "${table_name}" | xargs)"
  if [[ -z "${table_trimmed}" ]]; then
    continue
  fi
  bash "${ROOT_DIR}/deploy/clickhouse/create_script_table.sh" "${DB_NAME}" "${table_trimmed}"
done

echo "created db=${DB_NAME} builtin_tables=cpu,ram,swap,net,disk,fs,process script_tables=${SCRIPT_TABLES_RAW}"
