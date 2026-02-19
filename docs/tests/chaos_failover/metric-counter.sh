#!/usr/bin/env bash
set -euo pipefail
state="/tmp/magent-chaos-seq.txt"
stop_flag="/tmp/magent-chaos-stop-generation"

if [[ -f "${stop_flag}" ]]; then
  echo '{"key":"stopped","data":{"value":0}}'
  exit 0
fi

last=0
if [[ -f "${state}" ]]; then
  last="$(cat "${state}" 2>/dev/null || echo 0)"
fi
next=$((last + 1))
printf '%s' "${next}" > "${state}"
echo "{\"key\":\"${next}\",\"data\":{\"value\":1}}"
