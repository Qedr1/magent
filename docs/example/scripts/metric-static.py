#!/usr/bin/env python3

import json

# Minimal script-metric payload example (multiple points).
#
# Stdout JSON contract:
#   - root: object or array of objects
#   - each object: {"key":"<string>","data":{"<var>":<number|bool|value_object>, ...}}
#
# This script emits 2 points (two different keys: "a" and "b") with identical var set.
#
# Example how it ends up in ClickHouse after:
#   script stdout -> agent window aggregation -> Vector VRL flatten -> ClickHouse insert
#
# Script stdout (this file):
#   [
#     {"key":"a","data":{"util":{"last":10},"jobs":{"last":1}}},
#     {"key":"b","data":{"util":{"last":90},"jobs":{"last":2}}}
#   ]
#
# Vector/ClickHouse result (table = <metric_name>, e.g. `demo_python`):
#   key=a var=util agg=last value=10
#   key=a var=jobs agg=last value=1
#   key=b var=util agg=last value=90
#   key=b var=jobs agg=last value=2
# Plus rows for configured percentiles (p50/p90/p99) when send window has >=4 samples.
payload = [
    {"key": "a", "data": {"util": {"last": 10}, "jobs": {"last": 1}}},
    {"key": "b", "data": {"util": {"last": 90}, "jobs": {"last": 2}}},
]

print(json.dumps(payload))
