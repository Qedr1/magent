#!/usr/bin/env bash
set -euo pipefail

# E2E: http-server metric -> agent -> Vector -> ClickHouse.
# Verifies percentiles are computed and ClickHouse rows match expected values.
# Params:
#   $1 - database name (default: metrics_http_server_e2e)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics_http_server_e2e}"
TABLE_NAME="http_server_demo"
INGEST_ADDR="127.0.0.1:18090"
INGEST_PATH="/metrics"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENT_LOG="/tmp/magent-http-server-e2e.log"
VECTOR_LOG="/tmp/vector-http-server-e2e.log"
AGENT_CONFIG="/tmp/magent-http-server-e2e.toml"
VECTOR_CONFIG="/tmp/vector-http-server-e2e.toml"
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
bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "${TABLE_NAME}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${TABLE_NAME}"

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
role = "db"
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
percentiles = [50, 90, 99]

[[metrics.http_server.${TABLE_NAME}]]
listen = "${INGEST_ADDR}"
path = "${INGEST_PATH}"
send = "2s"
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

post_sample() {
  local util="$1"
  local jobs="$2"
  local code
  code="$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    --data "{\"key\":\"total\",\"data\":{\"util\":{\"last\":${util}},\"jobs\":{\"last\":${jobs}}}}" \
    "http://${INGEST_ADDR}${INGEST_PATH}")"
  if [[ "${code}" != "204" ]]; then
    echo "FAIL: POST returned http_code=${code}" >&2
    exit 1
  fi
}

# 4 samples in one send window => percentiles must be non-zero.
post_sample 10 1
post_sample 20 2
post_sample 30 3
post_sample 40 4

sleep 4

kill "${AGENT_PID}" >/dev/null 2>&1 || true
wait "${AGENT_PID}" >/dev/null 2>&1 || true
AGENT_PID=""

DRAIN_OK=0
for _ in $(seq 1 10); do
  ROWS="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${TABLE_NAME}")"
  if [[ "${ROWS}" -gt 0 ]]; then
    DRAIN_OK=1
    break
  fi
  sleep 1
done
if [[ "${DRAIN_OK}" -ne 1 ]]; then
  echo "FAIL: no rows inserted into ${DB_NAME}.${TABLE_NAME}" >&2
  exit 1
fi

DTS_TS="$(clickhouse-client --query "SELECT toUnixTimestamp(max(dts)) FROM ${DB_NAME}.${TABLE_NAME}")"
if [[ -z "${DTS_TS}" || "${DTS_TS}" -le 0 ]]; then
  echo "FAIL: no rows inserted into ${DB_NAME}.${TABLE_NAME}" >&2
  exit 1
fi

ch_val() {
  local var="$1"
  local agg="$2"
  clickhouse-client --query "SELECT value FROM ${DB_NAME}.${TABLE_NAME} WHERE dts = toDateTime(${DTS_TS}) AND key = 'total' AND var = '${var}' AND agg = '${agg}' ORDER BY dt DESC LIMIT 1"
}

assert_eq() {
  local got="$1"
  local want="$2"
  local label="$3"
  if [[ "${got}" != "${want}" ]]; then
    echo "FAIL: ${label}: got=${got} want=${want}" >&2
    exit 1
  fi
}

assert_eq "$(ch_val util last)" "40" "util.last"
assert_eq "$(ch_val util p50)" "20" "util.p50"
assert_eq "$(ch_val util p90)" "40" "util.p90"
assert_eq "$(ch_val util p99)" "40" "util.p99"

assert_eq "$(ch_val jobs last)" "4" "jobs.last"
assert_eq "$(ch_val jobs p50)" "2" "jobs.p50"
assert_eq "$(ch_val jobs p90)" "4" "jobs.p90"
assert_eq "$(ch_val jobs p99)" "4" "jobs.p99"

echo "OK: http_server e2e rows match expected aggregates"
