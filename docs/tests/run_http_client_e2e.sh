#!/usr/bin/env bash
set -euo pipefail

# E2E: http-client metric -> agent -> Vector -> ClickHouse.
# Verifies percentiles are computed and ClickHouse rows match expected values.
# Params:
#   $1 - database name (default: metrics_http_client_e2e)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics_http_client_e2e}"
TABLE_NAME="http_client_demo"
SRC_ADDR="127.0.0.1:18091"
SRC_PATH="/metrics"
COLLECTOR_ADDR="127.0.0.1:16001"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENT_LOG="/tmp/magent-http-client-e2e.log"
VECTOR_LOG="/tmp/vector-http-client-e2e.log"
SRC_LOG="/tmp/magent-http-client-source.log"
AGENT_CONFIG="/tmp/magent-http-client-e2e.toml"
VECTOR_CONFIG="/tmp/vector-http-client-e2e.toml"
VECTOR_PID=""
SRC_PID=""

cleanup() {
  if [[ -n "${SRC_PID}" ]]; then
    kill "${SRC_PID}" >/dev/null 2>&1 || true
    wait "${SRC_PID}" >/dev/null 2>&1 || true
    SRC_PID=""
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

python3 -u - <<PY >"${SRC_LOG}" 2>&1 &
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

PAYLOAD = {"key": "total", "data": {"util": {"last": 67}, "jobs": {"last": 7}}}
BODY = json.dumps(PAYLOAD).encode("utf-8")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "${SRC_PATH}":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(BODY)))
        self.end_headers()
        self.wfile.write(BODY)

    def log_message(self, *_args):
        return

HTTPServer(("127.0.0.1", 18091), Handler).serve_forever()
PY
SRC_PID="$!"
sleep 1

sed \
  -e "s/database = \"metrics\"/database = \"${DB_NAME}\"/" \
  -e 's/address = "0.0.0.0:6000"/address = "0.0.0.0:16001"/' \
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
scrape = "200ms"
send = "2s"
percentiles = [50, 90, 99]

[[metrics.http_client.${TABLE_NAME}]]
url = "http://${SRC_ADDR}${SRC_PATH}"
scrape = "200ms"
send = "2s"
timeout = "1s"

[[collector]]
name = "vector-local"
addr = ["${COLLECTOR_ADDR}"]
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

timeout 10s go run ./cmd/magent -config "${AGENT_CONFIG}" >"${AGENT_LOG}" 2>&1 || true
sleep 2

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

assert_eq "$(ch_val util last)" "67" "util.last"
assert_eq "$(ch_val util p50)" "67" "util.p50"
assert_eq "$(ch_val util p90)" "67" "util.p90"
assert_eq "$(ch_val util p99)" "67" "util.p99"

assert_eq "$(ch_val jobs last)" "7" "jobs.last"
assert_eq "$(ch_val jobs p50)" "7" "jobs.p50"
assert_eq "$(ch_val jobs p90)" "7" "jobs.p90"
assert_eq "$(ch_val jobs p99)" "7" "jobs.p99"

echo "OK: http_client e2e rows match expected aggregates"
