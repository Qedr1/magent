#!/usr/bin/env bash
set -euo pipefail

# Runs collector delivery mode e2e checks:
#   1) failover inside one [[collector]] with two addr entries
#   2) delivery to two independent [[collector]] sections
# Params:
#   $1 - failover test db (default: metrics_failover)
#   $2 - multi collector db A (default: metrics_multi_a)
#   $3 - multi collector db B (default: metrics_multi_b)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

FAILOVER_DB="${1:-metrics_failover}"
MULTI_DB_A="${2:-metrics_multi_a}"
MULTI_DB_B="${3:-metrics_multi_b}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AGENT_LOG="/tmp/magent-e2e-collector-modes.log"
VECTOR_CONFIG_FAILOVER="/tmp/vector-failover.toml"
VECTOR_CONFIG_MULTI_A="/tmp/vector-multi-a.toml"
VECTOR_CONFIG_MULTI_B="/tmp/vector-multi-b.toml"
VECTOR_LOG_FAILOVER="/tmp/vector-failover.log"
VECTOR_LOG_MULTI_A="/tmp/vector-multi-a.log"
VECTOR_LOG_MULTI_B="/tmp/vector-multi-b.log"
VECTOR_PID_FAILOVER=""
VECTOR_PID_MULTI_A=""
VECTOR_PID_MULTI_B=""

cleanup() {
  if [[ -n "${VECTOR_PID_FAILOVER}" ]]; then
    kill "${VECTOR_PID_FAILOVER}" >/dev/null 2>&1 || true
    wait "${VECTOR_PID_FAILOVER}" >/dev/null 2>&1 || true
    VECTOR_PID_FAILOVER=""
  fi
  if [[ -n "${VECTOR_PID_MULTI_A}" ]]; then
    kill "${VECTOR_PID_MULTI_A}" >/dev/null 2>&1 || true
    wait "${VECTOR_PID_MULTI_A}" >/dev/null 2>&1 || true
    VECTOR_PID_MULTI_A=""
  fi
  if [[ -n "${VECTOR_PID_MULTI_B}" ]]; then
    kill "${VECTOR_PID_MULTI_B}" >/dev/null 2>&1 || true
    wait "${VECTOR_PID_MULTI_B}" >/dev/null 2>&1 || true
    VECTOR_PID_MULTI_B=""
  fi
}
trap cleanup EXIT

ch_count() {
  local db_name="$1"
  local table_name="$2"
  clickhouse-client --query "SELECT count() FROM ${db_name}.${table_name}"
}

prepare_cpu_table() {
  local db_name="$1"
  bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${db_name}" "cpu"
  clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${db_name}.cpu"
}

write_vector_config() {
  local config_path="$1"
  local listen_port="$2"
  local db_name="$3"

  cat > "${config_path}" <<CFG
[sources.agent_in]
type = "vector"
version = "2"
address = "0.0.0.0:${listen_port}"

[transforms.flatten_rows]
type = "remap"
inputs = ["agent_in"]
source = '''
events = []
base_event = .
del(base_event.data)

if is_object(.data) {
  for_each(object!(.data)) -> |var_name, agg_map| {
    if is_object(agg_map) {
      for_each(object!(agg_map)) -> |agg_name, agg_value| {
        event = merge(base_event, {
          "var": var_name,
          "agg": agg_name,
          "value": to_int(agg_value) ?? 0
        })
        events = push(events, event)
      }
    }
  }
}

. = events
'''

[sinks.clickhouse]
type = "clickhouse"
inputs = ["flatten_rows"]
endpoint = "http://127.0.0.1:8123"
database = "${db_name}"
table = "{{ .metric }}"
compression = "none"
skip_unknown_fields = true
date_time_best_effort = true

[sinks.clickhouse.batch]
max_events = 1000
timeout_secs = 1
CFG
}

start_vector() {
  local config_path="$1"
  local log_path="$2"
  vector --config "${config_path}" >"${log_path}" 2>&1 &
  echo "$!"
}

run_agent() {
  local config_path="$1"
  local seconds="$2"
  timeout "${seconds}s" go run ./cmd/magent -config "${config_path}" >"${AGENT_LOG}" 2>&1 || true
}

cd "${ROOT_DIR}"

clickhouse-client --query "SELECT 1 FORMAT TSV" >/dev/null

echo "[test-1] failover in one collector"
prepare_cpu_table "${FAILOVER_DB}"
write_vector_config "${VECTOR_CONFIG_FAILOVER}" "6000" "${FAILOVER_DB}"
VECTOR_PID_FAILOVER="$(start_vector "${VECTOR_CONFIG_FAILOVER}" "${VECTOR_LOG_FAILOVER}")"
sleep 2

cat > /tmp/magent-failover.toml <<CFG
[global]
dc = "dc1"
project = "infra"
role = "db"
host = ""

[log.console]
enabled = true
level = "warn"
format = "json"

[log.file]
enabled = false
level = "info"
format = "json"
path = "./var/log/magent.log"

[metrics]
scrape = "1s"
send = "2s"
percentiles = [50, 90]

[[metrics.cpu]]
name = "cpu-failover"

[[collector]]
name = "vector-failover"
addr = ["127.0.0.1:65531","127.0.0.1:6000"]
timeout = "1s"
retry_interval = "1s"

[collector.batch]
max_events = 10
max_age = "2s"

[collector.queue]
enabled = false
dir = "/tmp/magent-queue-failover-ignore"
max_events = 1000
max_age = "1h"
CFG

run_agent /tmp/magent-failover.toml 18
sleep 2

rows_failover="$(ch_count "${FAILOVER_DB}" "cpu")"
echo "[test-1] rows=${rows_failover}"
if [[ "${rows_failover}" -le 0 ]]; then
  echo "[test-1] FAIL: no rows inserted for failover scenario" >&2
  exit 1
fi
if ! rg -q "127\\.0\\.0\\.1:65531" "${AGENT_LOG}"; then
  echo "[test-1] FAIL: missing failed-address log evidence for failover attempt" >&2
  exit 1
fi

kill "${VECTOR_PID_FAILOVER}" >/dev/null 2>&1 || true
wait "${VECTOR_PID_FAILOVER}" >/dev/null 2>&1 || true
VECTOR_PID_FAILOVER=""

echo "[test-2] delivery to two independent collectors"
prepare_cpu_table "${MULTI_DB_A}"
prepare_cpu_table "${MULTI_DB_B}"

write_vector_config "${VECTOR_CONFIG_MULTI_A}" "6000" "${MULTI_DB_A}"
write_vector_config "${VECTOR_CONFIG_MULTI_B}" "6001" "${MULTI_DB_B}"

VECTOR_PID_MULTI_A="$(start_vector "${VECTOR_CONFIG_MULTI_A}" "${VECTOR_LOG_MULTI_A}")"
VECTOR_PID_MULTI_B="$(start_vector "${VECTOR_CONFIG_MULTI_B}" "${VECTOR_LOG_MULTI_B}")"
sleep 2

cat > /tmp/magent-multi-collector.toml <<CFG
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
scrape = "1s"
send = "2s"
percentiles = [50, 90]

[[metrics.cpu]]
name = "cpu-multi-collector"

[[collector]]
name = "vector-a"
addr = ["127.0.0.1:6000"]
timeout = "2s"
retry_interval = "1s"

[collector.batch]
max_events = 10
max_age = "2s"

[collector.queue]
enabled = false
dir = "/tmp/magent-queue-multi-a-ignore"
max_events = 1000
max_age = "1h"

[[collector]]
name = "vector-b"
addr = ["127.0.0.1:6001"]
timeout = "2s"
retry_interval = "1s"

[collector.batch]
max_events = 10
max_age = "2s"

[collector.queue]
enabled = false
dir = "/tmp/magent-queue-multi-b-ignore"
max_events = 1000
max_age = "1h"
CFG

run_agent /tmp/magent-multi-collector.toml 18
sleep 2

rows_multi_a="$(ch_count "${MULTI_DB_A}" "cpu")"
rows_multi_b="$(ch_count "${MULTI_DB_B}" "cpu")"
echo "[test-2] rows_db_a=${rows_multi_a} rows_db_b=${rows_multi_b}"

if [[ "${rows_multi_a}" -le 0 ]]; then
  echo "[test-2] FAIL: no rows inserted in collector A target db" >&2
  exit 1
fi
if [[ "${rows_multi_b}" -le 0 ]]; then
  echo "[test-2] FAIL: no rows inserted in collector B target db" >&2
  exit 1
fi

echo "[result] OK collector failover and multi-delivery checks passed"
