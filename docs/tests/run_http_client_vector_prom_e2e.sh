#!/usr/bin/env bash
set -euo pipefail

# E2E: vector internal metrics (Prometheus exporter) -> magent http_client(prometheus) -> Vector -> ClickHouse.
# Verifies selected Vector Prometheus metrics are scraped, parsed, and stored in ClickHouse.
# Params:
#   $1 - database name (default: metrics_http_client_vector_prom_e2e)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics_http_client_vector_prom_e2e}"
TABLE_NAME="vector_monitor"
COLLECTOR_ADDR="127.0.0.1:16000"
EXPORTER_METRICS_ADDR="127.0.0.1:19599"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENT_LOG="/tmp/magent-http-client-vector-prom-e2e.log"
COLLECTOR_VECTOR_LOG="/tmp/vector-http-client-vector-prom-collector.log"
EXPORTER_VECTOR_LOG="/tmp/vector-http-client-vector-prom-exporter.log"
AGENT_CONFIG="/tmp/magent-http-client-vector-prom-e2e.toml"
COLLECTOR_VECTOR_CONFIG="/tmp/vector-http-client-vector-prom-collector.toml"
EXPORTER_VECTOR_CONFIG="/tmp/vector-http-client-vector-prom-exporter.toml"

COLLECTOR_VECTOR_PID=""
EXPORTER_VECTOR_PID=""

cleanup() {
  if [[ -n "${COLLECTOR_VECTOR_PID}" ]]; then
    kill "${COLLECTOR_VECTOR_PID}" >/dev/null 2>&1 || true
    wait "${COLLECTOR_VECTOR_PID}" >/dev/null 2>&1 || true
    COLLECTOR_VECTOR_PID=""
  fi
  if [[ -n "${EXPORTER_VECTOR_PID}" ]]; then
    kill "${EXPORTER_VECTOR_PID}" >/dev/null 2>&1 || true
    wait "${EXPORTER_VECTOR_PID}" >/dev/null 2>&1 || true
    EXPORTER_VECTOR_PID=""
  fi
}
trap cleanup EXIT

cd "${ROOT_DIR}"

clickhouse-client --query "SELECT 1 FORMAT TSV" >/dev/null
bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "${TABLE_NAME}"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${TABLE_NAME}"

sed \
  -e "s/database = \"metrics\"/database = \"${DB_NAME}\"/" \
  -e 's/address = "0.0.0.0:6000"/address = "0.0.0.0:16000"/' \
  -e 's/address = "127.0.0.1:19598"/address = "127.0.0.1:19698"/' \
  -e 's/timeout_secs = 5/timeout_secs = 1/' \
  "${ROOT_DIR}/deploy/vector/clickhouse-e2e.toml" > "${COLLECTOR_VECTOR_CONFIG}"

vector --config "${COLLECTOR_VECTOR_CONFIG}" >"${COLLECTOR_VECTOR_LOG}" 2>&1 &
COLLECTOR_VECTOR_PID="$!"

cat > "${EXPORTER_VECTOR_CONFIG}" <<CFG
[sources.int]
type = "internal_metrics"

[sinks.prom]
type = "prometheus_exporter"
inputs = ["int"]
address = "${EXPORTER_METRICS_ADDR}"
default_namespace = "vector"

[api]
enabled = true
address = "127.0.0.1:19686"
playground = false
CFG

vector --config "${EXPORTER_VECTOR_CONFIG}" >"${EXPORTER_VECTOR_LOG}" 2>&1 &
EXPORTER_VECTOR_PID="$!"

for _ in $(seq 1 30); do
  if curl -fsS "http://${EXPORTER_METRICS_ADDR}/metrics" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

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
scrape = "500ms"
send = "2s"
percentiles = []

[[metrics.http_client.${TABLE_NAME}]]
name = "vector-monitor-prom"
url = "http://${EXPORTER_METRICS_ADDR}/metrics"
format = "prometheus"
filter_var = ["vector_build_info","vector_api_started_total"]
var_mode = "full"
scrape = "500ms"
send = "2s"
timeout = "1s"
percentiles = []

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

timeout 15s go run ./cmd/magent -config "${AGENT_CONFIG}" >"${AGENT_LOG}" 2>&1 || true
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

build_rows="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${TABLE_NAME} WHERE var='vector_build_info' AND agg='last'")"
api_rows="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${TABLE_NAME} WHERE var='vector_api_started_total' AND agg='last'")"
bad_key_rows="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${TABLE_NAME} WHERE var IN ('vector_build_info','vector_api_started_total') AND key != 'total'")"
non_last_rows="$(clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${TABLE_NAME} WHERE var IN ('vector_build_info','vector_api_started_total') AND agg != 'last'")"

if [[ "${build_rows}" -le 0 ]]; then
  echo "FAIL: vector_build_info rows missing" >&2
  exit 1
fi
if [[ "${api_rows}" -le 0 ]]; then
  echo "FAIL: vector_api_started_total rows missing" >&2
  exit 1
fi
if [[ "${bad_key_rows}" -ne 0 ]]; then
  echo "FAIL: unexpected key format for vector Prometheus rows" >&2
  exit 1
fi
if [[ "${non_last_rows}" -ne 0 ]]; then
  echo "FAIL: expected only agg=last for vector Prometheus rows" >&2
  exit 1
fi

echo "OK: http_client(prometheus) vector monitoring e2e rows are valid"
