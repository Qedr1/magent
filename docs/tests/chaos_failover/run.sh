#!/usr/bin/env bash
set -euo pipefail

CHAOS_SECONDS="${1:-300}"
DRAIN_TIMEOUT_SECONDS="${2:-180}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

VECTOR_A_CFG="${SCRIPT_DIR}/vector-a.toml"
VECTOR_B_CFG="${SCRIPT_DIR}/vector-b.toml"
VECTOR_A_LOG="/tmp/vector-chaos-a.log"
VECTOR_B_LOG="/tmp/vector-chaos-b.log"
AGENT_CFG="${SCRIPT_DIR}/agent.toml"
AGENT_BIN="/tmp/magent-chaos"
AGENT_RUN_LOG="/tmp/magent-chaos-run.log"
METRIC_SCRIPT_SRC="${SCRIPT_DIR}/metric-counter.sh"
METRIC_SCRIPT_DST="/tmp/magent-chaos-script.sh"
STATE_FILE="/tmp/magent-chaos-seq.txt"
STOP_FLAG="/tmp/magent-chaos-stop-generation"
QUEUE_DIR="/tmp/magent-chaos-queue"
SUMMARY_FILE="/tmp/magent-chaos-summary.txt"

VECTOR_A_PID=""
VECTOR_B_PID=""
AGENT_PID=""

now_ts() {
  date '+%Y-%m-%d %H:%M:%S %Z'
}

is_pid_running() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

start_vector_a() {
  if is_pid_running "${VECTOR_A_PID}"; then
    return 0
  fi
  vector --config "${VECTOR_A_CFG}" >"${VECTOR_A_LOG}" 2>&1 &
  VECTOR_A_PID="$!"
  sleep 1
}

start_vector_b() {
  if is_pid_running "${VECTOR_B_PID}"; then
    return 0
  fi
  vector --config "${VECTOR_B_CFG}" >"${VECTOR_B_LOG}" 2>&1 &
  VECTOR_B_PID="$!"
  sleep 1
}

stop_vector_a() {
  if is_pid_running "${VECTOR_A_PID}"; then
    kill "${VECTOR_A_PID}" >/dev/null 2>&1 || true
    wait "${VECTOR_A_PID}" >/dev/null 2>&1 || true
  fi
  VECTOR_A_PID=""
}

stop_vector_b() {
  if is_pid_running "${VECTOR_B_PID}"; then
    kill "${VECTOR_B_PID}" >/dev/null 2>&1 || true
    wait "${VECTOR_B_PID}" >/dev/null 2>&1 || true
  fi
  VECTOR_B_PID=""
}

cleanup() {
  stop_vector_a
  stop_vector_b
  if is_pid_running "${AGENT_PID}"; then
    kill "${AGENT_PID}" >/dev/null 2>&1 || true
    wait "${AGENT_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v vector >/dev/null 2>&1; then
  echo "vector_not_found"
  exit 1
fi
if ! command -v clickhouse-client >/dev/null 2>&1; then
  echo "clickhouse_client_not_found"
  exit 1
fi

rm -f "${STOP_FLAG}" "${STATE_FILE}" "${SUMMARY_FILE}"
rm -f "${VECTOR_A_LOG}" "${VECTOR_B_LOG}" "${AGENT_RUN_LOG}"
rm -rf "${QUEUE_DIR}"
mkdir -p "${QUEUE_DIR}"
cp "${METRIC_SCRIPT_SRC}" "${METRIC_SCRIPT_DST}"
chmod +x "${METRIC_SCRIPT_DST}"

cd "${ROOT_DIR}"
go build -o "${AGENT_BIN}" ./cmd/magent

bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "metrics" "chaos"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS metrics.chaos"

echo "start=$(now_ts)"
start_vector_a
start_vector_b

"${AGENT_BIN}" -config "${AGENT_CFG}" >"${AGENT_RUN_LOG}" 2>&1 &
AGENT_PID="$!"
sleep 2

if ! is_pid_running "${AGENT_PID}"; then
  echo "agent_start_failed"
  exit 1
fi

echo "chaos_phase_seconds=${CHAOS_SECONDS}"
end_epoch=$(( $(date +%s) + CHAOS_SECONDS ))
iteration=0

while (( $(date +%s) < end_epoch )); do
  iteration=$((iteration + 1))
  action=$((RANDOM % 6))
  case "${action}" in
    0)
      stop_vector_a
      act="stop_a"
      ;;
    1)
      stop_vector_b
      act="stop_b"
      ;;
    2)
      stop_vector_a
      stop_vector_b
      act="stop_both"
      ;;
    3)
      start_vector_a
      act="start_a"
      ;;
    4)
      start_vector_b
      act="start_b"
      ;;
    5)
      start_vector_a
      start_vector_b
      act="start_both"
      ;;
  esac

  a_state="down"
  b_state="down"
  if is_pid_running "${VECTOR_A_PID}"; then a_state="up"; fi
  if is_pid_running "${VECTOR_B_PID}"; then b_state="up"; fi

  seq_now=0
  if [[ -f "${STATE_FILE}" ]]; then
    seq_now="$(cat "${STATE_FILE}" 2>/dev/null || echo 0)"
  fi

  q_bytes=0
  if [[ -f "${QUEUE_DIR}/events.bin" ]]; then
    q_bytes="$(wc -c < "${QUEUE_DIR}/events.bin" | tr -d ' ')"
  fi

  echo "chaos_iter=${iteration} action=${act} vector_a=${a_state} vector_b=${b_state} seq=${seq_now} queue_bytes=${q_bytes} ts=$(now_ts)"

  sleep_sec=$((RANDOM % 8 + 3))
  sleep "${sleep_sec}"
done

touch "${STOP_FLAG}"

start_vector_a
start_vector_b

echo "drain_phase_start=$(now_ts)"
expected=0
if [[ -f "${STATE_FILE}" ]]; then
  expected="$(cat "${STATE_FILE}" 2>/dev/null || echo 0)"
fi

echo "expected_numeric_keys=${expected}"

success=0
drain_end=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
while (( $(date +%s) < drain_end )); do
  distinct_last="$(clickhouse-client --query "SELECT countDistinctIf(key, match(key, '^[0-9]+$') AND agg='last' AND var='value') FROM metrics.chaos")"
  rows_last="$(clickhouse-client --query "SELECT countIf(match(key, '^[0-9]+$') AND agg='last' AND var='value') FROM metrics.chaos")"
  bad_dt="$(clickhouse-client --query "SELECT countIf(dt >= dts) FROM metrics.chaos")"
  bad_ip="$(clickhouse-client --query "SELECT countIf(host_ip = toIPv6('::')) FROM metrics.chaos")"

  q_bytes=0
  if [[ -f "${QUEUE_DIR}/events.bin" ]]; then
    q_bytes="$(wc -c < "${QUEUE_DIR}/events.bin" | tr -d ' ')"
  fi

  echo "drain_check distinct_last=${distinct_last} rows_last=${rows_last} expected=${expected} bad_dt=${bad_dt} bad_ip=${bad_ip} queue_bytes=${q_bytes} ts=$(now_ts)"

  if [[ "${distinct_last}" -eq "${expected}" ]]; then
    success=1
    break
  fi

  sleep 2
done

status="FAIL"
if [[ "${success}" -eq 1 ]]; then
  status="PASS"
fi

final_distinct="$(clickhouse-client --query "SELECT countDistinctIf(key, match(key, '^[0-9]+$') AND agg='last' AND var='value') FROM metrics.chaos")"
final_rows_last="$(clickhouse-client --query "SELECT countIf(match(key, '^[0-9]+$') AND agg='last' AND var='value') FROM metrics.chaos")"
final_total_rows="$(clickhouse-client --query "SELECT count() FROM metrics.chaos")"
final_bad_dt="$(clickhouse-client --query "SELECT countIf(dt >= dts) FROM metrics.chaos")"
final_bad_ip="$(clickhouse-client --query "SELECT countIf(host_ip = toIPv6('::')) FROM metrics.chaos")"

{
  echo "status=${status}"
  echo "finished=$(now_ts)"
  echo "expected_numeric_keys=${expected}"
  echo "final_distinct_last=${final_distinct}"
  echo "final_rows_last=${final_rows_last}"
  echo "final_total_rows=${final_total_rows}"
  echo "final_bad_dt=${final_bad_dt}"
  echo "final_bad_ip=${final_bad_ip}"
  if [[ "${final_rows_last}" -gt "${expected}" ]]; then
    echo "note=duplicates_detected"
  fi
} | tee "${SUMMARY_FILE}"

if [[ "${status}" != "PASS" ]]; then
  exit 1
fi
