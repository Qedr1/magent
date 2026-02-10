# Vector Local E2E Configs

## intake-log.toml
- Purpose: receive agent events via Vector Protocol v2 and write raw received events to log file.
- Start:
  - `mkdir -p ./var/vector`
  - `vector --config ./deploy/vector/intake-log.toml`
- Output log: `./var/vector/intake.log`

## flatten-log.toml
- Purpose: receive agent events, flatten `data` into row-like records (`key,var,agg,value`) and write to log file.
- Start:
  - `mkdir -p ./var/vector`
  - `vector --config ./deploy/vector/flatten-log.toml`
- Output log: `./var/vector/flatten.log`

## clickhouse-e2e.toml
- Purpose: receive agent events, flatten rows via VRL (`remap` + `route`) and send to ClickHouse (`table = {{ .metric }}`), plus write flattened rows to local file.
- Start:
  - `mkdir -p ./var/vector`
  - `vector --config ./deploy/vector/clickhouse-e2e.toml`
- Output log: `./var/vector/flatten_clickhouse.log`
