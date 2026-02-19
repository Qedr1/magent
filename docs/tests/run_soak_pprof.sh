#!/usr/bin/env bash
set -euo pipefail

# Runs long soak test with pprof enabled: agent -> Vector -> ClickHouse.
# Params:
#   $1 - database name (default: metrics)
#   $2 - soak duration in seconds (default: 7200)
#   $3 - CPU profile duration in seconds (default: 60)
# Returns:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics}"
SOAK_SECONDS="${2:-7200}"
CPU_PROFILE_SECONDS="${3:-60}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="/tmp/magent-soak-pprof"
VECTOR_CONFIG="/tmp/vector-soak-pprof.toml"
VECTOR_LOG="${OUT_DIR}/vector.log"
AGENT_CONFIG="/tmp/magent-soak-pprof.toml"
AGENT_LOG="${OUT_DIR}/agent.log"
AGENT_BIN="${OUT_DIR}/magent"
PPROF_ADDR="127.0.0.1:6060"

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

ch_count() {
  local table="$1"
  clickhouse-client --query "SELECT count() FROM ${DB_NAME}.${table}"
}

ch_bad_dt() {
  local table="$1"
  clickhouse-client --query "SELECT countIf(dt >= dts) FROM ${DB_NAME}.${table}"
}

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

mkdir -p "${OUT_DIR}"

for bin in vector clickhouse-client go curl; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "missing binary: ${bin}" >&2
    exit 1
  fi
done

cd "${ROOT_DIR}"

bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "cpu,ram,swap,net,disk,fs,process"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.cpu"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.ram"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.swap"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.net"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.disk"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.fs"
clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.process"

sed "s/database = \"metrics\"/database = \"${DB_NAME}\"/" "${ROOT_DIR}/deploy/vector/clickhouse-e2e.toml" > "${VECTOR_CONFIG}"

cat > "${AGENT_CONFIG}" <<CFG
[global]
dc = "dc1"
project = "infra"
role = "soak"
host = ""

[log.console]
enabled = true
level = "error"
format = "line"

[log.file]
enabled = true
level = "info"
format = "json"
path = "${OUT_DIR}/agent-file.log"

[pprof]
enabled = true
listen = "${PPROF_ADDR}"

[metrics]
scrape = "10s"
send = "60s"
percentiles = [50, 90, 99]

[[metrics.cpu]]
name = "cpu-soak"

[[metrics.ram]]
name = "ram-soak"

[[metrics.swap]]
name = "swap-soak"

[[metrics.net]]
name = "net-soak"

[[metrics.disk]]
name = "disk-soak"

[[metrics.fs]]
name = "fs-soak"

[[metrics.process]]
name = "process-soak"
cpu_util = 0

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
timeout = "3s"
retry_interval = "2s"

[collector.batch]
max_events = 500
max_age = "60s"

[collector.queue]
enabled = true
dir = "./var/queue/soak-pprof"
max_events = 500000
max_age = "24h"
CFG

go build -o "${AGENT_BIN}" ./cmd/magent

vector --config "${VECTOR_CONFIG}" > "${VECTOR_LOG}" 2>&1 &
VECTOR_PID="$!"
sleep 2

"${AGENT_BIN}" -config "${AGENT_CONFIG}" > "${AGENT_LOG}" 2>&1 &
AGENT_PID="$!"

for _ in $(seq 1 20); do
  if curl -fsS "http://${PPROF_ADDR}/debug/pprof/" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl -fsS "http://${PPROF_ADDR}/debug/pprof/" >/dev/null; then
  echo "pprof endpoint is not reachable on ${PPROF_ADDR}" >&2
  exit 1
fi

echo "soak_start=$(now_utc)" | tee "${OUT_DIR}/summary.txt"
echo "soak_seconds=${SOAK_SECONDS}" | tee -a "${OUT_DIR}/summary.txt"
echo "cpu_profile_seconds=${CPU_PROFILE_SECONDS}" | tee -a "${OUT_DIR}/summary.txt"

for sec in $(seq 10 10 "${SOAK_SECONDS}"); do
  sleep 10
  if ! kill -0 "${AGENT_PID}" >/dev/null 2>&1; then
    echo "agent exited early, see ${AGENT_LOG}" >&2
    exit 1
  fi
  if (( sec % 300 == 0 )); then
    echo "progress_seconds=${sec} ts=$(now_utc)" | tee -a "${OUT_DIR}/summary.txt"
  fi
done

curl -fsS "http://${PPROF_ADDR}/debug/pprof/heap" -o "${OUT_DIR}/heap.pb.gz"
curl -fsS "http://${PPROF_ADDR}/debug/pprof/goroutine?debug=1" -o "${OUT_DIR}/goroutine.txt"
go tool pprof -proto -seconds "${CPU_PROFILE_SECONDS}" "http://${PPROF_ADDR}/debug/pprof/profile" > "${OUT_DIR}/cpu.pb.gz"
go tool pprof -top "${AGENT_BIN}" "${OUT_DIR}/cpu.pb.gz" > "${OUT_DIR}/cpu-top.txt"
go tool pprof -top "${AGENT_BIN}" "${OUT_DIR}/heap.pb.gz" > "${OUT_DIR}/heap-top.txt"

cpu_rows="$(ch_count cpu)"
ram_rows="$(ch_count ram)"
swap_rows="$(ch_count swap)"
net_rows="$(ch_count net)"
disk_rows="$(ch_count disk)"
fs_rows="$(ch_count fs)"
process_rows="$(ch_count process)"
total_rows=$((cpu_rows + ram_rows + swap_rows + net_rows + disk_rows + fs_rows + process_rows))

bad_dt_cpu="$(ch_bad_dt cpu)"
bad_dt_ram="$(ch_bad_dt ram)"
bad_dt_swap="$(ch_bad_dt swap)"
bad_dt_net="$(ch_bad_dt net)"
bad_dt_disk="$(ch_bad_dt disk)"
bad_dt_fs="$(ch_bad_dt fs)"
bad_dt_process="$(ch_bad_dt process)"
total_bad_dt=$((bad_dt_cpu + bad_dt_ram + bad_dt_swap + bad_dt_net + bad_dt_disk + bad_dt_fs + bad_dt_process))

{
  echo "soak_end=$(now_utc)"
  echo "rows_cpu=${cpu_rows}"
  echo "rows_ram=${ram_rows}"
  echo "rows_swap=${swap_rows}"
  echo "rows_net=${net_rows}"
  echo "rows_disk=${disk_rows}"
  echo "rows_fs=${fs_rows}"
  echo "rows_process=${process_rows}"
  echo "rows_total=${total_rows}"
  echo "dt_ge_dts_total=${total_bad_dt}"
  echo "cpu_profile=${OUT_DIR}/cpu.pb.gz"
  echo "heap_profile=${OUT_DIR}/heap.pb.gz"
  echo "cpu_top=${OUT_DIR}/cpu-top.txt"
  echo "heap_top=${OUT_DIR}/heap-top.txt"
} | tee -a "${OUT_DIR}/summary.txt"

if [[ "${total_rows}" -le 0 ]]; then
  echo "FAIL: total rows is zero" >&2
  exit 1
fi
if [[ "${total_bad_dt}" -ne 0 ]]; then
  echo "FAIL: found dt >= dts violations" >&2
  exit 1
fi

echo "PASS soak+pprof"
