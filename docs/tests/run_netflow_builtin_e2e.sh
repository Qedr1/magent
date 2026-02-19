#!/usr/bin/env bash
set -euo pipefail

# E2E: built-in metrics.netflow (AF_PACKET) -> Vector -> ClickHouse raw+MV.
# Pass criteria:
#   1) raw table has rows and only agg=last
#   2) netflow_pairs has rows
#   3) controlled local TCP flow (fixed local port) appears in pairs table
# Params:
#   $1 - database name (default: metrics_netflow_builtin_e2e)

DB_NAME="${1:-metrics_netflow_builtin_e2e}"
RAW_TABLE="netflow"
PAIRS_TABLE="netflow_pairs"
MV_TABLE="mv_netflow_pairs"
FLOW_SERVER_ADDR="127.0.0.1:19091"
VECTOR_ADDR="127.0.0.1:6100"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENT_LOG="/tmp/magent-netflow-builtin-e2e.log"
VECTOR_LOG="/tmp/vector-netflow-builtin-e2e.log"
AGENT_CONFIG="/tmp/magent-netflow-builtin-e2e.toml"
VECTOR_CONFIG="/tmp/vector-netflow-builtin-e2e.toml"
FLOW_DIR="/tmp/magent-netflow-builtin-www"
FLOW_FILE="${FLOW_DIR}/blob.bin"
FLOW_OUT="/tmp/magent-netflow-builtin-download.bin"
FLOW_SERVER_PID=""
VECTOR_PID=""
AGENT_PID=""

cleanup() {
  if [[ -n "${AGENT_PID}" ]]; then
    kill "${AGENT_PID}" >/dev/null 2>&1 || true
    wait "${AGENT_PID}" >/dev/null 2>&1 || true
    AGENT_PID=""
  fi
  if [[ -n "${VECTOR_PID}" ]]; then
    kill "${VECTOR_PID}" >/dev/null 2>&1 || true
    wait "${VECTOR_PID}" >/dev/null 2>&1 || true
    VECTOR_PID=""
  fi
  if [[ -n "${FLOW_SERVER_PID}" ]]; then
    kill "${FLOW_SERVER_PID}" >/dev/null 2>&1 || true
    wait "${FLOW_SERVER_PID}" >/dev/null 2>&1 || true
    FLOW_SERVER_PID=""
  fi
}
trap cleanup EXIT

cd "${ROOT_DIR}"

clickhouse-client --query "SELECT 1 FORMAT TSV" >/dev/null
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${DB_NAME}"
bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "${RAW_TABLE}"
clickhouse-client --query "DROP VIEW IF EXISTS ${DB_NAME}.${MV_TABLE}"
clickhouse-client --query "DROP TABLE IF EXISTS ${DB_NAME}.${PAIRS_TABLE}"
bash "${ROOT_DIR}/deploy/clickhouse/create_netflow_pairs.sh" "${DB_NAME}" "${RAW_TABLE}" "${PAIRS_TABLE}" "${MV_TABLE}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${RAW_TABLE}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${PAIRS_TABLE}"

sed \
  -e "s/database = \"metrics\"/database = \"${DB_NAME}\"/" \
  -e 's/timeout_secs = 5/timeout_secs = 1/' \
  -e "s/address = \"0.0.0.0:6000\"/address = \"${VECTOR_ADDR}\"/" \
  "${ROOT_DIR}/deploy/vector/clickhouse-e2e.toml" > "${VECTOR_CONFIG}"

vector --config "${VECTOR_CONFIG}" >"${VECTOR_LOG}" 2>&1 &
VECTOR_PID="$!"
sleep 2
if ! kill -0 "${VECTOR_PID}" >/dev/null 2>&1; then
  echo "FAIL: vector did not start (see ${VECTOR_LOG})" >&2
  exit 1
fi

mkdir -p "${FLOW_DIR}"
dd if=/dev/zero of="${FLOW_FILE}" bs=1M count=8 status=none
python3 -m http.server 19091 --bind 127.0.0.1 --directory "${FLOW_DIR}" >/tmp/magent-netflow-builtin-http.log 2>&1 &
FLOW_SERVER_PID="$!"
sleep 1

cat > "${AGENT_CONFIG}" <<CFG
[global]
dc = "dc1"
project = "infra"
role = "netflow"
host = ""

[log.console]
enabled = true
level = "error"
format = "line"

[log.file]
enabled = false
level = "info"
format = "json"
path = "./var/log/magent.log"

[metrics]
scrape = "1s"
send = "4s"
percentiles = [50, 90]

[[metrics.netflow]]
name = "netflow-lo"
ifaces = ["lo"]
top_n = 50
scrape = "1s"
send = "4s"

[[collector]]
name = "vector-local"
addr = ["${VECTOR_ADDR}"]
timeout = "2s"
retry_interval = "1s"

[collector.batch]
max_events = 1000
max_age = "2s"

[collector.queue]
enabled = false
dir = "/tmp/magent-queue-ignore"
max_events = 1000
max_age = "1h"
CFG

go run ./cmd/magent -config "${AGENT_CONFIG}" >"${AGENT_LOG}" 2>&1 &
AGENT_PID="$!"
sleep 2

for _ in $(seq 1 6); do
  curl -sS "http://${FLOW_SERVER_ADDR}/blob.bin" -o "${FLOW_OUT}"
  sleep 0.3
done

for _ in $(seq 1 20); do
  live_rows="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${RAW_TABLE}")"
  if [[ "${live_rows}" -gt 0 ]]; then
    break
  fi
  sleep 1
done

kill "${AGENT_PID}" >/dev/null 2>&1 || true
wait "${AGENT_PID}" >/dev/null 2>&1 || true
AGENT_PID=""

RAW_ROWS="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${RAW_TABLE}")"
RAW_NON_LAST="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${RAW_TABLE} WHERE agg != 'last'")"
PAIRS_ROWS="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${PAIRS_TABLE}")"

echo "raw_rows=${RAW_ROWS} raw_non_last=${RAW_NON_LAST} pairs_rows=${PAIRS_ROWS}"

if [[ "${RAW_ROWS}" -eq 0 ]]; then
  echo "FAIL: no rows in raw table ${DB_NAME}.${RAW_TABLE}" >&2
  exit 1
fi
if [[ "${RAW_NON_LAST}" -ne 0 ]]; then
  echo "FAIL: expected last-only rows for ${RAW_TABLE}, got non-last=${RAW_NON_LAST}" >&2
  exit 1
fi
if [[ "${PAIRS_ROWS}" -eq 0 ]]; then
  echo "FAIL: no rows in table ${DB_NAME}.${PAIRS_TABLE}" >&2
  exit 1
fi

FLOW_ROWS="$(clickhouse-client --query "
SELECT count()
FROM ${DB_NAME}.${PAIRS_TABLE}
WHERE proto = 'tcp'
  AND src_ip = toIPv6('127.0.0.1')
  AND dst_ip = toIPv6('127.0.0.1')
  AND (src_port = 19091 OR dst_port = 19091)
  AND bytes > 0
  AND packets > 0
")"

if [[ "${FLOW_ROWS}" -eq 0 ]]; then
  echo "FAIL: controlled flow not found in ${DB_NAME}.${PAIRS_TABLE}" >&2
  echo "debug pairs sample:" >&2
  clickhouse-client --query "SELECT dt, iface, proto, toString(src_ip), src_port, toString(dst_ip), dst_port, bytes, packets, flows FROM ${DB_NAME}.${PAIRS_TABLE} ORDER BY dt DESC LIMIT 20 FORMAT TabSeparated" >&2
  exit 1
fi

echo "sample_pairs:"
clickhouse-client --query "SELECT iface, proto, toString(src_ip), src_port, toString(dst_ip), dst_port, bytes, packets, flows FROM ${DB_NAME}.${PAIRS_TABLE} ORDER BY dt DESC LIMIT 10 FORMAT TabSeparated"

echo "OK: built-in netflow e2e passed"
