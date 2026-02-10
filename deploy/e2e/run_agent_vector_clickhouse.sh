#!/usr/bin/env bash
set -euo pipefail

# Runs full local e2e: agent -> Vector -> ClickHouse.
# Params:
#   $1 - database name (default: metrics)
#   $2 - metric table name for check (default: cpu)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics}"
TABLE_NAME="${2:-cpu}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VECTOR_CONFIG="${ROOT_DIR}/deploy/vector/clickhouse-e2e.toml"
AGENT_CONFIG="/tmp/magent-e2e.toml"
VECTOR_LOG="/tmp/vector-e2e.log"
AGENT_LOG="/tmp/magent-e2e.log"
VECTOR_PID=""

cleanup() {
  if [[ -n "${VECTOR_PID}" ]]; then
    kill "${VECTOR_PID}" >/dev/null 2>&1 || true
    wait "${VECTOR_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cd "${ROOT_DIR}"

bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "${TABLE_NAME}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${TABLE_NAME}"

mkdir -p "${ROOT_DIR}/var/vector"
: > "${ROOT_DIR}/var/vector/flatten_clickhouse.log"

cat > "${AGENT_CONFIG}" <<'EOF'
[global]
dc = "dc1"
project = "infra"
role = "db"
host = ""

[log.console]
enabled = true
level = "info"
format = "line"

[log.file]
enabled = false
level = "info"
format = "json"
path = "./var/log/magent.log"

[metrics]
scrape = "1s"
send = "2s"
percentiles = [50,90]

[[metrics.cpu]]
name = "cpu-e2e"

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
timeout = "3s"
retry_interval = "1s"

[collector.batch]
max_events = 10
max_age = "2s"

[collector.queue]
enabled = false
dir = "./var/queue/e2e"
max_events = 1000
max_age = "1h"
EOF

vector --config "${VECTOR_CONFIG}" > "${VECTOR_LOG}" 2>&1 &
VECTOR_PID="$!"
sleep 2

timeout 18s go run ./cmd/magent -config "${AGENT_CONFIG}" > "${AGENT_LOG}" 2>&1 || true
sleep 3

ROW_COUNT="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${TABLE_NAME}")"
echo "clickhouse_rows=${ROW_COUNT}"

if [[ "${ROW_COUNT}" -eq 0 ]]; then
  echo "e2e failed: no rows inserted into ${DB_NAME}.${TABLE_NAME}" >&2
  exit 1
fi

echo "sample_rows:"
clickhouse-client --query "SELECT key,var,agg,value FROM ${DB_NAME}.${TABLE_NAME} ORDER BY dt DESC LIMIT 5 FORMAT TabSeparated"
echo "vector_flatten_log_lines:"
wc -l "${ROOT_DIR}/var/vector/flatten_clickhouse.log"
