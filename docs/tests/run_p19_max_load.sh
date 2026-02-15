#!/usr/bin/env bash
set -euo pipefail

# Runs P#19 high-load local benchmark: agent -> Vector -> ClickHouse.
# Params:
#   $1 - database name (default: metrics)
#   $2 - active load duration in seconds (default: 45)
# Return:
#   0 on success; non-zero on setup/runtime/assertion failure.

DB_NAME="${1:-metrics}"
DURATION_SECONDS="${2:-45}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VECTOR_CONFIG="/tmp/vector-clickhouse-p19.toml"
VECTOR_LOG="/tmp/vector-p19.log"
AGENT_CONFIG="/tmp/magent-p19.toml"
AGENT_LOG="/tmp/magent-p19.log"
SCRIPT_METRIC_PATH="/tmp/magent-p19-script-db.sh"
VECTOR_PID=""

cleanup() {
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

truncate_tables() {
  local tables=("$@")
  for table in "${tables[@]}"; do
    clickhouse-client --query "TRUNCATE TABLE IF EXISTS ${DB_NAME}.${table}"
  done
}

cd "${ROOT_DIR}"

clickhouse-client --query "SELECT 1 FORMAT TSV" >/dev/null

bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "cpu,ram,swap,net,disk,fs,process"
bash "${ROOT_DIR}/deploy/clickhouse/create_builtin_tables.sh" "${DB_NAME}" "db"
truncate_tables cpu ram swap net disk fs process db

cat > "${SCRIPT_METRIC_PATH}" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
echo '{"key":"total","data":{"jobs":7,"lag":13,"conn":211,"slow":3,"locked":5,"dirty":17,"active":81,"idle":49,"tx":301,"rx":298}}'
SCRIPT
chmod +x "${SCRIPT_METRIC_PATH}"

cat > "${VECTOR_CONFIG}" <<VEC
[sources.agent_in]
type = "vector"
version = "2"
address = "0.0.0.0:6000"

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
database = "${DB_NAME}"
table = "{{ .metric }}"
compression = "none"
skip_unknown_fields = true
date_time_best_effort = true

[sinks.clickhouse.batch]
max_events = 5000
timeout_secs = 1
VEC

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
scrape = "250ms"
send = "1s"
percentiles = [50, 90, 99]

[[metrics.cpu]]
name = "cpu-p19-1"
[[metrics.cpu]]
name = "cpu-p19-2"
[[metrics.cpu]]
name = "cpu-p19-3"
[[metrics.cpu]]
name = "cpu-p19-4"

[[metrics.ram]]
name = "ram-p19-1"
[[metrics.ram]]
name = "ram-p19-2"
[[metrics.ram]]
name = "ram-p19-3"
[[metrics.ram]]
name = "ram-p19-4"

[[metrics.swap]]
name = "swap-p19-1"
[[metrics.swap]]
name = "swap-p19-2"
[[metrics.swap]]
name = "swap-p19-3"
[[metrics.swap]]
name = "swap-p19-4"

[[metrics.net]]
name = "net-p19-1"
[[metrics.net]]
name = "net-p19-2"
[[metrics.net]]
name = "net-p19-3"
[[metrics.net]]
name = "net-p19-4"

[[metrics.disk]]
name = "disk-p19-1"
[[metrics.disk]]
name = "disk-p19-2"
[[metrics.disk]]
name = "disk-p19-3"
[[metrics.disk]]
name = "disk-p19-4"

[[metrics.fs]]
name = "fs-p19-1"
[[metrics.fs]]
name = "fs-p19-2"
[[metrics.fs]]
name = "fs-p19-3"
[[metrics.fs]]
name = "fs-p19-4"

[[metrics.process]]
name = "process-p19-1"
cpu_util = 0

[[metrics.process]]
name = "process-p19-2"
cpu_util = 0

[[metrics.script.db]]
name = "script-db-p19-1"
path = "${SCRIPT_METRIC_PATH}"
timeout = "2s"

[[metrics.script.db]]
name = "script-db-p19-2"
path = "${SCRIPT_METRIC_PATH}"
timeout = "2s"

[[collector]]
name = "vector-local"
addr = ["127.0.0.1:6000"]
timeout = "3s"
retry_interval = "1s"

[collector.batch]
max_events = 2000
max_age = "1s"

[collector.queue]
enabled = false
dir = "/tmp/magent-p19-queue-ignore"
max_events = 1000
max_age = "1h"
CFG

vector --config "${VECTOR_CONFIG}" >"${VECTOR_LOG}" 2>&1 &
VECTOR_PID="$!"
sleep 2

echo "[p19] run duration=${DURATION_SECONDS}s"
timeout "$((DURATION_SECONDS + 5))s" go run ./cmd/magent -config "${AGENT_CONFIG}" >"${AGENT_LOG}" 2>&1 || true
sleep 3

cpu_rows="$(ch_count cpu)"
ram_rows="$(ch_count ram)"
swap_rows="$(ch_count swap)"
net_rows="$(ch_count net)"
disk_rows="$(ch_count disk)"
fs_rows="$(ch_count fs)"
process_rows="$(ch_count process)"
db_rows="$(ch_count db)"

total_rows=$((cpu_rows + ram_rows + swap_rows + net_rows + disk_rows + fs_rows + process_rows + db_rows))
rows_per_sec="$(awk -v rows="${total_rows}" -v dur="${DURATION_SECONDS}" 'BEGIN { if (dur <= 0) { print "0.00" } else { printf "%.2f", rows / dur } }')"

echo "[p19] rows cpu=${cpu_rows} ram=${ram_rows} swap=${swap_rows} net=${net_rows} disk=${disk_rows} fs=${fs_rows} process=${process_rows} db=${db_rows}"
echo "[p19] total_rows=${total_rows} approx_rows_per_sec=${rows_per_sec}"

if [[ "${total_rows}" -le 0 ]]; then
  echo "[p19] FAIL: total_rows is zero" >&2
  exit 1
fi

echo "[p19] OK"
