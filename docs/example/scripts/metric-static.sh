#!/usr/bin/env bash
set -euo pipefail

# Minimal script-metric payload example (single point).
#
# Stdout JSON contract:
#   {"key":"<string>","data":{"<var>":<number|bool|value_object>, ...}}
#
# This script emits a single point with two variables: util and jobs.
#
# Example how it ends up in ClickHouse after:
#   script stdout -> agent window aggregation -> Vector VRL flatten -> ClickHouse insert
#
# Script stdout (this file):
#   {"key":"total","data":{"util":{"last":67},"jobs":{"last":7}}}
#
# Agent aggregates in send window (scrape runs multiple times per send):
#   data.util.last = 67; data.util.p50/p90/p99 computed from samples (>=4), here it stays 67
#   data.jobs.last = 7;  data.jobs.p50/p90/p99 computed from samples (>=4), here it stays 7
#
# Vector flattens `data[var][agg]` into row events and ClickHouse stores them as:
#   table = <metric_name> (e.g. `demo_bash`)
#   columns: key,var,agg,value (+ dt,dts,dc,host,project,role,host_ip,dtv)
#
# Example rows in ClickHouse table `demo_bash`:
#   key=total var=util  agg=last value=67
#   key=total var=util  agg=p50  value=67
#   key=total var=util  agg=p90  value=67
#   key=total var=util  agg=p99  value=67
#   key=total var=jobs  agg=last value=7
#   key=total var=jobs  agg=p50  value=7
#   key=total var=jobs  agg=p90  value=7
#   key=total var=jobs  agg=p99  value=7
#
# Note: if a send window has <4 samples, agent outputs pXX=0 (by design).
echo '{"key":"total","data":{"util":{"last":67},"jobs":{"last":7}}}'
