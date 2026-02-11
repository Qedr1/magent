# Collector Failover Chaos Test

This folder stores the reproducible chaos test assets for one collector with two Vector addresses.

## Files
- `agent.toml` - agent config with one `[[collector]]` and two `addr` values.
- `vector-a.toml` - Vector receiver/sink on `127.0.0.1:6200`.
- `vector-b.toml` - Vector receiver/sink on `127.0.0.1:6201`.
- `metric-counter.sh` - deterministic script metric generator (`key=1..N`).
- `run.sh` - chaos runner: random stop/start of Vector A/B, then drain verification.

## Run
```bash
bash ./docs/tests/chaos_failover/run.sh 300 180
```

Params:
- `$1` chaos phase duration in seconds (default `300`)
- `$2` drain timeout in seconds (default `180`)

## Pass Criteria
Test is `PASS` when no packets are lost:
- `expected_numeric_keys == final_distinct_last`

Summary is written to `/tmp/magent-chaos-summary.txt`.
