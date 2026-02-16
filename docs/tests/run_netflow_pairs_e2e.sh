#!/usr/bin/env bash
set -euo pipefail

# E2E: http_server netflow payload -> agent -> Vector -> ClickHouse raw table -> MV -> netflow_pairs.
# Pass criteria: rows in netflow_pairs match posted flow tuples and counters.
# Params:
#   $1 - database name (default: metrics_netflow_e2e)

DB_NAME="${1:-metrics_netflow_e2e}"
RAW_TABLE="netflow"
PAIRS_TABLE="netflow_pairs"
MV_TABLE="mv_netflow_pairs"
INGEST_ADDR="127.0.0.1:18092"
INGEST_PATH="/netflow"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENT_LOG="/tmp/magent-netflow-e2e.log"
VECTOR_LOG="/tmp/vector-netflow-e2e.log"
AGENT_CONFIG="/tmp/magent-netflow-e2e.toml"
VECTOR_CONFIG="/tmp/vector-netflow-e2e.toml"
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
}
trap cleanup EXIT

cd "${ROOT_DIR}"

clickhouse-client --query "SELECT 1 FORMAT TSV" >/dev/null
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${DB_NAME}"
clickhouse-client --query "DROP VIEW IF EXISTS ${DB_NAME}.${MV_TABLE}"
clickhouse-client --query "DROP TABLE IF EXISTS ${DB_NAME}.${PAIRS_TABLE}"
bash "${ROOT_DIR}/deploy/clickhouse/create_netflow_pairs.sh" "${DB_NAME}" "${RAW_TABLE}" "${PAIRS_TABLE}" "${MV_TABLE}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${RAW_TABLE}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${PAIRS_TABLE}"

SRC_TYPE="$(clickhouse-client --query "SELECT type FROM system.columns WHERE database='${DB_NAME}' AND table='${PAIRS_TABLE}' AND name='src_ip'")"
DST_TYPE="$(clickhouse-client --query "SELECT type FROM system.columns WHERE database='${DB_NAME}' AND table='${PAIRS_TABLE}' AND name='dst_ip'")"
if [[ "${SRC_TYPE}" != "IPv6" || "${DST_TYPE}" != "IPv6" ]]; then
  echo "FAIL: expected IPv6 src/dst columns, got src_ip=${SRC_TYPE} dst_ip=${DST_TYPE}" >&2
  exit 1
fi

sed \
  -e "s/database = \"metrics\"/database = \"${DB_NAME}\"/" \
  -e 's/timeout_secs = 5/timeout_secs = 1/' \
  "${ROOT_DIR}/deploy/vector/clickhouse-e2e.toml" > "${VECTOR_CONFIG}"

vector --config "${VECTOR_CONFIG}" >"${VECTOR_LOG}" 2>&1 &
VECTOR_PID="$!"
sleep 2

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
send = "2s"

[[metrics.http_server.${RAW_TABLE}]]
listen = "${INGEST_ADDR}"
path = "${INGEST_PATH}"
send = "2s"
percentiles = []
max_pending = 1000

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
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
sleep 1

PAYLOAD='[
  {"key":"eth0|tcp|10.0.0.1|55000|10.0.0.2|443","data":{"bytes":1200,"packets":12,"flows":1}},
  {"key":"eth0|udp|10.0.0.3|49000|10.0.0.4|53","data":{"bytes":800,"packets":8,"flows":1}}
]'

HTTP_CODE="$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  --data "${PAYLOAD}" \
  "http://${INGEST_ADDR}${INGEST_PATH}")"

if [[ "${HTTP_CODE}" != "204" ]]; then
  echo "FAIL: POST returned ${HTTP_CODE}" >&2
  exit 1
fi

sleep 4
kill "${AGENT_PID}" >/dev/null 2>&1 || true
wait "${AGENT_PID}" >/dev/null 2>&1 || true
AGENT_PID=""

RAW_ROWS=0
PAIRS_ROWS=0
for _ in $(seq 1 10); do
  RAW_ROWS="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${RAW_TABLE}")"
  PAIRS_ROWS="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${PAIRS_TABLE}")"
  if [[ "${RAW_ROWS}" -gt 0 && "${PAIRS_ROWS}" -gt 0 ]]; then
    break
  fi
  sleep 1
done
RAW_NON_LAST="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${RAW_TABLE} WHERE agg != 'last'")"
echo "raw_rows=${RAW_ROWS} pairs_rows=${PAIRS_ROWS} raw_non_last=${RAW_NON_LAST}"

if [[ "${RAW_ROWS}" -eq 0 ]]; then
  echo "FAIL: no rows in raw table ${DB_NAME}.${RAW_TABLE}" >&2
  exit 1
fi
if [[ "${PAIRS_ROWS}" -ne 2 ]]; then
  echo "FAIL: expected 2 rows in ${DB_NAME}.${PAIRS_TABLE}, got ${PAIRS_ROWS}" >&2
  exit 1
fi
if [[ "${RAW_NON_LAST}" -ne 0 ]]; then
  echo "FAIL: expected last-only raw rows, got non-last=${RAW_NON_LAST}" >&2
  exit 1
fi

assert_row() {
  local proto="$1"
  local src_ip="$2"
  local src_port="$3"
  local dst_ip="$4"
  local dst_port="$5"
  local bytes="$6"
  local packets="$7"
  local flows="$8"

  local got
  got="$(clickhouse-client --query "SELECT concat(toString(bytes),',',toString(packets),',',toString(flows)) FROM ${DB_NAME}.${PAIRS_TABLE} WHERE iface='eth0' AND proto='${proto}' AND src_ip=toIPv6('${src_ip}') AND src_port=${src_port} AND dst_ip=toIPv6('${dst_ip}') AND dst_port=${dst_port} ORDER BY dt DESC LIMIT 1")"
  local want="${bytes},${packets},${flows}"
  if [[ "${got}" != "${want}" ]]; then
    echo "FAIL: tuple ${proto} ${src_ip}:${src_port} -> ${dst_ip}:${dst_port}: got=${got} want=${want}" >&2
    exit 1
  fi
}

assert_row "tcp" "10.0.0.1" "55000" "10.0.0.2" "443" "1200" "12" "1"
assert_row "udp" "10.0.0.3" "49000" "10.0.0.4" "53" "800" "8" "1"

echo "sample_pairs:"
clickhouse-client --query "SELECT iface,proto,toString(src_ip),src_port,toString(dst_ip),dst_port,bytes,packets,flows FROM ${DB_NAME}.${PAIRS_TABLE} ORDER BY dt DESC, proto FORMAT TabSeparated"

echo "OK: netflow_pairs MV e2e passed"
