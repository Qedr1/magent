#!/usr/bin/env bash
set -euo pipefail

# Runs extended local e2e:
#   1) all metric tables ingestion check
#   2) batch flush behavior check (max_events vs max_age)
#   3) queue recovery check (collector down -> up)
# Params:
#   $1 - database name (default: metrics)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VECTOR_CONFIG_FAST="/tmp/vector-clickhouse-fast.toml"
VECTOR_LOG="/tmp/vector-e2e-advanced.log"
AGENT_LOG="/tmp/magent-e2e-advanced.log"
SCRIPT_METRIC_PATH="/tmp/magent-script-db.sh"
QUEUE_DIR="/tmp/magent-queue-e2e"
VECTOR_PID=""

cleanup() {
  if [[ -n "${VECTOR_PID}" ]]; then
    kill "${VECTOR_PID}" >/dev/null 2>&1 || true
    wait "${VECTOR_PID}" >/dev/null 2>&1 || true
    VECTOR_PID=""
  fi
}
trap cleanup EXIT

start_vector() {
  cleanup
  vector --config "${VECTOR_CONFIG_FAST}" >"${VECTOR_LOG}" 2>&1 &
  VECTOR_PID="$!"
  sleep 2
}

stop_vector() {
  cleanup
}

run_agent() {
  local config_path="$1"
  local seconds="$2"
  timeout "${seconds}s" go run ./cmd/magent -config "${config_path}" >"${AGENT_LOG}" 2>&1 || true
}

ch_count() {
  local table="$1"
  clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${table}"
}

truncate_tables() {
  local tables=("$@")
  for table in "${tables[@]}"; do
    clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${table}"
  done
}

offset_value() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    echo 0
    return 0
  fi

  local out
  out="$(od -An -tu8 -N8 "${path}" 2>/dev/null | xargs || true)"
  if [[ -z "${out}" ]]; then
    echo 0
    return 0
  fi
  echo "${out}"
}

cd "${ROOT_DIR}"

clickhouse-client --query "SELECT 1 FORMAT TSV" >/dev/null

sed \
  -e 's/max_events = 1000/max_events = 1/' \
  -e 's/timeout_secs = 5/timeout_secs = 1/' \
  -e "s/database = \"metrics\"/database = \"${DB_NAME}\"/" \
  "${ROOT_DIR}/deploy/vector/clickhouse-e2e.toml" > "${VECTOR_CONFIG_FAST}"

bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "cpu,ram,swap,net,disk,fs,process"
bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "db"
truncate_tables cpu ram swap net disk fs process db

cat > "${SCRIPT_METRIC_PATH}" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
echo '{"key":"total","data":{"util":42,"jobs":7}}'
SCRIPT
chmod +x "${SCRIPT_METRIC_PATH}"

cat > /tmp/magent-all-metrics.toml <<CFG
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
name = "cpu-all"

[[metrics.ram]]
name = "ram-all"

[[metrics.swap]]
name = "swap-all"

[[metrics.net]]
name = "net-all"

[[metrics.disk]]
name = "disk-all"

[[metrics.fs]]
name = "fs-all"

[[metrics.process]]
name = "process-all"
cpu_util = 0

[[metrics.script.db]]
name = "script-db-all"
path = "${SCRIPT_METRIC_PATH}"
timeout = "2s"

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
dir = "/tmp/magent-queue-ignore"
max_events = 1000
max_age = "1h"
CFG

echo "[test-1] all metrics ingestion"
start_vector
run_agent /tmp/magent-all-metrics.toml 24
sleep 2

ALL_TABLES=(cpu ram swap net disk fs process db)
for table in "${ALL_TABLES[@]}"; do
  count="$(ch_count "${table}")"
  echo "[test-1] table=${table} rows=${count}"
  if [[ "${count}" -eq 0 ]]; then
    echo "[test-1] FAIL: table ${table} has zero rows" >&2
    exit 1
  fi
done

echo "[test-2] batch behavior (max_events)"
truncate_tables cpu
cat > /tmp/magent-batch-events.toml <<CFG
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
send = "1s"
percentiles = [50, 90]

[[metrics.cpu]]
name = "cpu-batch-events"

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
timeout = "3s"
retry_interval = "1s"

[collector.batch]
max_events = 1
max_age = "30s"

[collector.queue]
enabled = false
dir = "/tmp/magent-queue-ignore"
max_events = 1000
max_age = "1h"
CFG

( timeout 10s go run ./cmd/magent -config /tmp/magent-batch-events.toml >"${AGENT_LOG}" 2>&1 || true ) &
AGENT_PID="$!"
first_non_zero_events=0
for sec in $(seq 1 8); do
  sleep 1
  c="$(ch_count cpu)"
  echo "[test-2] t=${sec}s rows=${c}"
  if [[ "${c}" -gt 0 && "${first_non_zero_events}" -eq 0 ]]; then
    first_non_zero_events="${sec}"
  fi
done
wait "${AGENT_PID}" || true
if [[ "${first_non_zero_events}" -eq 0 ]]; then
  echo "[test-2] FAIL: rows never appeared" >&2
  exit 1
fi

echo "[test-3] batch behavior (max_age)"
truncate_tables cpu
cat > /tmp/magent-batch-age.toml <<CFG
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
send = "1s"
percentiles = [50, 90]

[[metrics.cpu]]
name = "cpu-batch-age"

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
timeout = "3s"
retry_interval = "1s"

[collector.batch]
max_events = 1000
max_age = "4s"

[collector.queue]
enabled = false
dir = "/tmp/magent-queue-ignore"
max_events = 1000
max_age = "1h"
CFG

( timeout 12s go run ./cmd/magent -config /tmp/magent-batch-age.toml >"${AGENT_LOG}" 2>&1 || true ) &
AGENT_PID="$!"
first_non_zero_age=0
for sec in $(seq 1 10); do
  sleep 1
  c="$(ch_count cpu)"
  echo "[test-3] t=${sec}s rows=${c}"
  if [[ "${c}" -gt 0 && "${first_non_zero_age}" -eq 0 ]]; then
    first_non_zero_age="${sec}"
  fi
done
wait "${AGENT_PID}" || true
if [[ "${first_non_zero_age}" -eq 0 ]]; then
  echo "[test-3] FAIL: rows never appeared" >&2
  exit 1
fi

echo "[test-3] first_non_zero_events=${first_non_zero_events} first_non_zero_age=${first_non_zero_age}"
if [[ "${first_non_zero_age}" -lt "${first_non_zero_events}" ]]; then
  echo "[test-3] FAIL: age-trigger appeared earlier than events-trigger unexpectedly" >&2
  exit 1
fi

echo "[test-4] queue behavior"
stop_vector
truncate_tables cpu
rm -rf "${QUEUE_DIR}"

cat > /tmp/magent-queue.toml <<CFG
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
send = "1s"
percentiles = [50, 90]

[[metrics.cpu]]
name = "cpu-queue"

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
timeout = "1s"
retry_interval = "1s"

[collector.batch]
max_events = 2
max_age = "10s"

[collector.queue]
enabled = true
dir = "${QUEUE_DIR}"
max_events = 1000
max_age = "1h"
CFG

run_agent /tmp/magent-queue.toml 10
rows_down="$(ch_count cpu)"
queue_size_before="$(stat -c%s "${QUEUE_DIR}/queue.bin")"
offset_before="$(offset_value "${QUEUE_DIR}/offset.bin")"
echo "[test-4] vector_down rows=${rows_down} queue_bin_bytes=${queue_size_before} offset=${offset_before}"
if [[ "${rows_down}" -ne 0 ]]; then
  echo "[test-4] FAIL: rows appeared while vector was down" >&2
  exit 1
fi
if [[ "${queue_size_before}" -le 0 ]]; then
  echo "[test-4] FAIL: queue file did not grow while vector was down" >&2
  exit 1
fi

start_vector
run_agent /tmp/magent-queue.toml 12
sleep 2
rows_up="$(ch_count cpu)"
queue_size_after="$(stat -c%s "${QUEUE_DIR}/queue.bin")"
offset_after="$(offset_value "${QUEUE_DIR}/offset.bin")"
echo "[test-4] vector_up rows=${rows_up} queue_bin_bytes=${queue_size_after} offset=${offset_after}"
if [[ "${queue_size_after}" -ne 0 || "${offset_after}" -ne 0 ]]; then
  echo "[test-4] queue not fully drained on first recovery run; retrying drain pass"
  run_agent /tmp/magent-queue.toml 8
  sleep 2
  rows_up="$(ch_count cpu)"
  queue_size_after="$(stat -c%s "${QUEUE_DIR}/queue.bin")"
  offset_after="$(offset_value "${QUEUE_DIR}/offset.bin")"
  echo "[test-4] vector_up_retry rows=${rows_up} queue_bin_bytes=${queue_size_after} offset=${offset_after}"
fi
if [[ "${rows_up}" -le 0 ]]; then
  echo "[test-4] FAIL: no rows after vector recovery" >&2
  exit 1
fi
if [[ "${queue_size_after}" -ne 0 ]]; then
  echo "[test-4] FAIL: queue file not drained after recovery" >&2
  exit 1
fi
if [[ "${offset_after}" -ne 0 ]]; then
  echo "[test-4] FAIL: offset not reset after full drain" >&2
  exit 1
fi

echo "[result] OK all checks passed"
