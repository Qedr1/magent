# magent — Linux Metrics Agent

## Purpose

Key properties, each tied to a mechanism described later in this document:

- traffic economy: a compact event structure without duplication and a binary protocol between agent and collector;
- network activity visibility: flow telemetry plus congestion-control state of the host network stack;
- agent-side event filtering before anything leaves the host;
- delivery resilience: local disk queues and collector failover;
- storage efficiency: a universal table schema whose read/write speed barely depends on data volume.


magent collects system metrics: network interface utilization, CPU, RAM, disk, filesystems, netflow pairs, network stack, kernel, processes, and more. It also collects arbitrary external metrics: it executes scripts, accepts data through `http_server`, or polls endpoints through `http_client`.

The solution monitors a combined infrastructure of 21k hosts with storage growth of ~2 TB per day.

## Table of Contents

- [Terms and Definitions](#terms-and-definitions)
- [Architecture](#architecture)
  - [Collection (owner: magent)](#collection-owner-magent)
  - [Delivery (owner: magent)](#delivery-owner-magent)
  - [Transform (owner: Vector)](#transform-owner-vector)
  - [Storage (owner: ClickHouse)](#storage-owner-clickhouse)
  - [Visualization (owner: Grafana)](#visualization-owner-grafana)
- [Stress Tests](#stress-tests)
- [Metrics](#metrics)
  - [Aggregation and Normalization](#aggregation-and-normalization)
  - [Event Structure](#event-structure)
  - [CPU](#cpu)
  - [RAM](#ram)
  - [SWAP](#swap)
  - [KERNEL](#kernel)
  - [DISK](#disk)
  - [FS](#fs)
  - [NET](#net)
  - [NETFLOW](#netflow)
  - [PROCESS](#process)
  - [MAGENT_INTERNAL](#magent_internal)
  - [SCRIPT](#script)
  - [HTTP-SERVER](#http-server)
  - [HTTP-CLIENT](#http-client)
  - [External Metrics Contract](#external-metrics-contract)
- [Configuration](#configuration)
  - [File Loading](#file-loading)
  - [Global Tags](#global-tags)
  - [Metric Defaults](#metric-defaults)
  - [Metric Workers](#metric-workers)
  - [Collectors](#collectors)
  - [pprof](#pprof)
  - [ClickHouse Tooling](#clickhouse-tooling)
  - [Hot Reload](#hot-reload)
- [Logging](#logging)
- [Vector Collector](#vector-collector)
- [ClickHouse Storage](#clickhouse-storage)
  - [Metric Tables](#metric-tables)
  - [netflow_pairs](#netflow_pairs)
- [Roadmap](#roadmap)
- [Changelog](#changelog)

## Terms and Definitions

This section defines every concept the architecture description uses; later sections reuse these terms without redefining them.

- **Event** — one metric payload object: `metric + key + data` plus tags and timestamps. `collector.batch.max_events` counts whole events, not individual `var/value` pairs.
- **key** — mandatory string identifier of the metric entity within one event (a CPU core, a block device, a mountpoint, `total`, etc.).
- **var** — one measured variable inside `data` (for example `util`, `rx_bytes`).
- **agg** — aggregation name of one var value: `last` or a percentile key `pXX`.
- **last** — the most recent sample of a var within one send window.
- **pXX** — nearest-rank percentile computed by the agent over the samples of one send window (for example `p90`).
- **scrape** — poll period of a pull metric worker.
- **send** — emit period: the aggregation window after which collected samples are flushed towards collectors.
- **window** — per-worker buffer that accumulates scrape samples between two sends.
- **batch** — a group of events delivered in one `PushEventsRequest` RPC.
- **collector** — a logical delivery target configured as one `[[collector]]` section and backed by one or more Vector instances.
- **active address** — the currently elected healthy `addr` entry of a collector; all batches go only to this address.
- **disk queue** — per-collector append-only on-disk FIFO that stores encoded batches when no collector address is reachable.
- **tag** — a global label (`dc`, `project`, `role`, `host`) attached to every event; defined once in `[global]`.
- **dt** — metric poll timestamp taken by the agent (uint64, milliseconds).
- **dts** — event send timestamp (seconds).
- **dtv** — event processing timestamp assigned by the collector (seconds).

## Architecture

One sequential process turns host state into dashboard rows. In **Collection** (owner: magent) each configured metric worker scrapes its source once per `scrape` period and aggregates samples in its window; once per `send` period the window becomes a set of events. In **Delivery** (owner: magent) events are batched per collector, encoded once into Vector Protocol v2, and pushed to the collector's active address; failures go through re-election, retry, and the disk queue. In **Transform** (owner: Vector) each incoming log event is flattened into `key × var × agg` rows and routed to the table named by `metric`. In **Storage** (owner: ClickHouse) rows are inserted in batches into per-metric MergeTree tables. In **Visualization** (owner: Grafana) dashboards read tables and materialized views.

The output of each iteration is the input of the next one: events → batches → rows → tables → dashboards. As an alternative consumer, an alerting branch leaves Storage into a NATS queue read by the malert component. The diagram shows the same order, the owner of every iteration, and every transition.

```
                  ┌─────────┐                 ┌─────────┐
4  # Grafana      │ VIEWER  │                 │ MALERT  │   # alerting
                  └────▲────┘                 └─────────┘┐
                       │ table + mat. view     └───▲─────┘
                       │                           │ 
                  ┌────┴────┐   alert         ┌────┴────┐
3  # ClickHouse   │ STORAGE │ ────────────▶   │  QUEUE  │   # NATS
                  └────▲────┘   nats engine   └─────────┘┐
                       │                       └─────────┘ 
                       │ insert (batch) 
                  ┌────┴──────┐
2                 │ COLL 1..N │    # vector.dev
                  └───────────┘┐
                   └───▲───────┘
                       │ events gRPC (batch, fan-out)
                  ┌────┴──┐
1                 │  HOST │        # magent
                  └───────┘
```

### Collection (owner: magent)

This iteration turns host state into events and is the entry point of the process.

- Preconditions: metric worker is present in the config; a metric absent from the config is not collected and its worker is not created.
- Input: host metric sources (`/proc`, `/sys`, block devices, sockets), external script stdout, HTTP payloads.
- Action: every metric is collected by its own asynchronous worker with its own `scrape` and `send` periods; samples accumulate in the worker window; once per `send` the agent computes `last` and configured `pXX`, normalizes values, and forms one event per entity key. A script worker executes an external command and parses its stdout in the standard format.
- Output: events with mandatory tags (`dc`, `project`, `role`, `host`, `host_ip`), `dt`, `dts`, `metric`, `key`, `data`.
- Result: one `send` period may contain `1..n` scrape periods; each metric is collected once per scrape and emitted once per send.
- Next transition: events are handed to Delivery (fan-out into every configured collector).

### Delivery (owner: magent)

This iteration guarantees that events leave the host exactly once per batch despite collector failures.

- Preconditions: at least one `[[collector]]` section exists; each collector has a non-empty `addr` list.
- Input: events from Collection.
- Action: per collector, events accumulate into batches by count or by age (whichever fires first); a batch is encoded into a protobuf `PushEventsRequest` once and pushed as one RPC to the active address; events travel as `EventWrapper.log` so the payload stays 1:1 and is not split into metric-events. With several `[[collector]]` sections, delivery fans out to all of them independently; queues and batches are kept per collector regardless of its address count. Re-sends to another address and queue drains reuse the already encoded bytes.
- Output: `PushEventsRequest` batches over Vector Protocol v2 (gRPC over HTTP/2, binary protobuf).
- Result: a batch leaves the agent when the event count or the batch age limit is reached; host-level delivery state is visible in the `magent_internal` metric.
- Next transition: batches are accepted by Transform on any alive collector.

#### Failover

Failover elects exactly one active address per collector and recovers without operator input; a single-address collector follows the same code path.

1. Every `health_interval` the agent probes all `addr` entries of a collector concurrently (Vector HealthCheck RPC; on `Unimplemented` it falls back to a TCP dial).
2. The first responding address becomes active and stays active while alive (no flapping between healthy addresses).
3. Every batch is sent only to the active address.
4. On a send error the address is cleared, all addresses are re-probed, and the batch is retried once on the re-elected address without loss.
5. When no address is alive, the batch goes to the collector's disk queue; the retry cycle drains the queue once a probe succeeds again.
6. While the disk queue is non-empty, fresh batches are appended to it and delivery proceeds strictly in write order (FIFO).
7. Elections are observable: `addr_switches`, `batches_sent`, `batches_failed`, `queue_pending` in `magent_internal`.

#### Disk Queue

The disk queue is the persistence mechanism behind step 5 of Failover.

- one append-only queue per collector; records are stored in the native Vector protobuf format (the same bytes that are sent);
- new records are appended only when all addresses of the collector are unavailable; draining retries indefinitely every `retry_interval`;
- queue limits (`max_events` and/or `max_age`, whichever fires first) reject new records and keep the old ones;
- reading is sequential with the current offset persisted in the queue itself and cached in memory; the offset is synced in batches or by timer (not per ack) and flushed on graceful stop;
- on send failure the offset stays in place and reading resumes from it after recovery;
- after the queue is fully drained the file is truncated and the offset resets to `0`;
- the queue directory is set by `collector.queue.dir` (required when `enabled=true`).

### Transform (owner: Vector)

This iteration converts each incoming event into flat table rows.

- Preconditions: a Vector instance with a `vector` source (protocol v2) and a remap transform as in `deploy/vector/`.
- Input: `PushEventsRequest` batches from Delivery.
- Action: each log event is flattened by a VRL remap into `key × var × agg` events (`var`, `agg`, `value` fields); routing selects the table by metric name (`table = {metric_name}`); the event timestamp becomes `dtv`.
- Output: row events per metric table.
- Result: one agent event becomes N rows, where N = keys × vars × aggs of that event.
- Next transition: rows are inserted by Storage.

### Storage (owner: ClickHouse)

This iteration persists rows so that read and write speed barely depend on data volume.

- Preconditions: per-metric tables created from the universal schema (`deploy/clickhouse/`).
- Input: row events from Transform.
- Action: batched inserts (1000 rows or 1 minute, whichever fires first) into the table named by the metric.
- Output: MergeTree tables partitioned by day.
- Result: one universal schema (`dt, dts, dtv, tags, key, var, agg, value`) covers every metric, including script metrics and `magent_internal`.
- Next transition: tables and materialized views are queried by Visualization.

### Visualization (owner: Grafana)

This iteration presents stored data to operators.

- Preconditions: Grafana connected to the ClickHouse database.
- Input: metric tables and materialized views (for example `netflow_pairs`).
- Action: dashboards query tables directly.
- Output: panels and alerts built on the universal columns.
- Result: the terminal state of the process — operators observe fleet state.
- Next transition: none for dashboards; the alerting alternative branches from Storage into the NATS queue consumed by malert.

## Stress Tests

These measurements bound the resource budget of the two resource-critical iterations (Collection/Delivery on the host, Transform on the collector).

magent:

```
- file size: 3.7 MB (upx)
- vCPU:1
- RES mem: 21MB
- RAM total: 128MB

~1 719 event/s
~18 801 metrics/s

~97% load average
```

collector (vector):

```
- file size: 136 MB
- vCPU:2
- RES mem: 82MB
- RAM total: 512MB

~25 785 event/s
~282 015 metrics/s

~182% load average
```

## Metrics

This section fixes the rules every metric follows and then documents each metric; it details what Collection and Transform produce. Built-in metrics are emitted in lower case (`cpu`, `ram`, `swap`, `net`, `netflow`, `disk`, `fs`, `process`, `magent_internal`) for direct mapping to lower-case ClickHouse tables.

### Aggregation and Normalization

These rules constrain every event the agent emits.

- every metric always records `last`; percentiles `pXX` are computed only when configured;
- metrics are collected per `scrape`, aggregated, and sent per `send`; when percentiles are not configured, the worker's `scrape` is forced to equal its `send`;
- percentiles are computed by the agent itself from the scrape samples of one send window;
- on output all numeric non-percent values in `data` are normalized to `uint64`, percent values to `uint8`; rounding is mathematical (0.5 rounds up);
- `0` is a valid sample value; when a window has no samples, `last=0` and every configured `pXX=0`;
- `dt` and `dts` are not subject to these rules and are passed as is.

### Event Structure

Every metric uses the same event structure, which the Transform iteration later flattens into rows.

```
dt        // poll timestamp of the event, once per scrape period, msec
dts       // send timestamp of the event, once per send period, sec
metric    // metric name (the ClickHouse table is created by this field)
dtv       // assigned in the collector: event processing time
dc        // server location. Mandatory global tag
host      // name of the host the metric was collected on. Mandatory global tag; OS hostname when not set
project   // project name. Mandatory global tag
role      // host role in the system architecture (frontdb/back/db ...). Mandatory global tag
host_ip   // set by the agent at batch encoding from the local route address towards the collector (route lookup, no network traffic)
key       // mandatory string key of the metric
data      // all metric vars and values, e.g. ram_total, ram_used, cpu_total
```

Key principles:

- `key` is always a string and always present;
- when a metric has no natural key, `key="total"` is used;
- when a metric has several entities, the agent forms a separate event per entity with its own key;
- after flattening, row count = keys × vars × aggs: for each key the table holds one row per (`var`, `agg`) pair with columns `key`, `var`, `agg`, `value`.

Universal structure example:

```json
{
  "metric": "cpu",
  "dt": 1231231232,
  "dts": 1231231254,
  "dc": "DC1",
  "host": "host1",
  "project": "Infra",
  "role": "DB_Postgres",
  "key": "core0",
  "data": {
    "util": { "last": 67, "p90": 97, "p99": 98 }
  }
}
```

CPU metric as ClickHouse rows:

```
|dc |host|project|role|host_ip  |key  |var |agg |value|
+---+----+-------+----+---------+-----+----+----+-----+
|dc1|dev |infra  |soak|127.0.0.1|core0|util|last|    1|
|dc1|dev |infra  |soak|127.0.0.1|core0|util|p50 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core0|util|p90 |    2|
|dc1|dev |infra  |soak|127.0.0.1|core0|util|p99 |    2|
|dc1|dev |infra  |soak|127.0.0.1|core1|util|last|    0|
|dc1|dev |infra  |soak|127.0.0.1|core1|util|p50 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core1|util|p90 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core1|util|p99 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core2|util|last|    0|
|dc1|dev |infra  |soak|127.0.0.1|core2|util|p50 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core2|util|p90 |    2|
|dc1|dev |infra  |soak|127.0.0.1|core2|util|p99 |    2|
|dc1|dev |infra  |soak|127.0.0.1|core3|util|last|    1|
|dc1|dev |infra  |soak|127.0.0.1|core3|util|p50 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core3|util|p90 |    1|
|dc1|dev |infra  |soak|127.0.0.1|core3|util|p99 |    1|
|dc1|dev |infra  |soak|127.0.0.1|total|util|last|    1|
|dc1|dev |infra  |soak|127.0.0.1|total|util|p50 |    1|
|dc1|dev |infra  |soak|127.0.0.1|total|util|p90 |    1|
|dc1|dev |infra  |soak|127.0.0.1|total|util|p99 |    1|
```

LowCardinality on the storage side removes the overhead of duplicated string fields.

### CPU

System metric of CPU load per core and aggregated per host.

- config section: `[[metrics.cpu]]`
- key: `coreN` for a single core, `total` for the all-core aggregate.
- util: utilization of all cores or of one core. uint8, %

### RAM

System metric of host memory usage.

- config section: `[[metrics.ram]]`
- key: always `total`.
- total: all physical memory. uint64, bytes
- used: used memory. uint64, bytes
- free: available memory. uint64, bytes
- util: utilization (used/total*100). uint8, %

### SWAP

System metric of swap usage.

- config section: `[[metrics.swap]]`
- key: always `total`.
- total: swap size. uint64, bytes
- used: used swap. uint64, bytes
- util: utilization. uint8, %

### KERNEL

System kernel metric from `/proc/loadavg` and `/proc/stat`.

- config section: `[[metrics.kernel]]`
- key: always `total`.
- load1: 1-minute load average. uint64
- load5: 5-minute load average. uint64
- load15: 15-minute load average. uint64
- procs_running: runnable tasks. uint64, count
- procs_blocked: blocked tasks. uint64, count
- ctxt_per_sec: context switches per poll interval. uint64, ops/sec
- intr_per_sec: hardware interrupts per poll interval. uint64, ops/sec
- softirq_per_sec: softirqs per poll interval. uint64, ops/sec
- forks_per_sec: created processes per poll interval. uint64, ops/sec

### DISK

System metric of block devices; only base disks are collected, partitions are ignored (`/dev/sda1`, `/dev/sda2`, `/dev/nvme0n1p1`, `/dev/mmcblk0p1`).

- config section: `[[metrics.disk]]`
- key: base block device path (`/dev/...`).
- rx_io: read operations per second. uint64, ops/sec
- tx_io: write operations per second. uint64, ops/sec
- rx_bytes: bytes read per poll interval. uint64, bytes
- tx_bytes: bytes written per poll interval. uint64, bytes
- rx_bytes_per_sec: read speed. uint64, bytes/sec
- tx_bytes_per_sec: write speed. uint64, bytes/sec
- rx_await: average read I/O wait time. uint64, ms
- tx_await: average write I/O wait time. uint64, ms
- await: average overall I/O wait time. uint64, ms
- qdepth: device request queue depth. uint64, count
- util: device utilization by IOPS. uint8, %
- inflight: requests in flight (submitted, not completed). uint64, count

### FS

System metric of filesystems by mountpoint.

- config section: `[[metrics.fs]]`
- key: mountpoint `<mpoint>`.
- total: filesystem size. uint64, bytes
- used: used bytes. uint64, bytes
- free: free bytes. uint64, bytes
- avail: available bytes (including reserved). uint64, bytes
- util: filesystem utilization. uint8, %
- inodes_total: total inodes. uint64, count
- inodes_used: used inodes. uint64, count
- inodes_free: free inodes. uint64, count
- inodes_util: inode utilization. uint8, %
- readonly: filesystem is read-only (0/1). uint8

### NET

System network metric: interface counters + host-level TCP/UDP stack + TCP congestion snapshot.

- config section: `[[metrics.net]]`
- key modes:
  - `<iface>` (for example `eth0`) for interface vars;
  - `total` for host-level TCP/UDP vars;
  - `cc:<algo>` (for example `cc:cubic`) for the TCP congestion snapshot.
- for key=`<iface>`:
  - tx_bytes: bytes sent per poll interval. uint64, bytes
  - rx_bytes: bytes received per poll interval. uint64, bytes
  - tx_bytes_per_sec: send speed. uint64, bytes/sec
  - rx_bytes_per_sec: receive speed. uint64, bytes/sec
  - tx_pkt: packets sent per second. uint64, packets/sec
  - rx_pkt: packets received per second. uint64, packets/sec
  - rx_err: receive errors. uint64, count
  - tx_err: send errors. uint64, count
  - rx_drop: dropped on receive. uint64, count
  - tx_drop: dropped on send. uint64, count
- for key=`total`:
  - tcp_active_opens: new active TCP opens per poll interval. uint64, count
  - tcp_passive_opens: new passive TCP opens per poll interval. uint64, count
  - tcp_retrans_segs: TCP retransmitted segments per poll interval. uint64, count
  - tcp_timeouts: TCP timeouts per poll interval. uint64, count
  - tcp_out_rsts: TCP reset packets per poll interval. uint64, count
  - udp_in_datagrams: incoming UDP datagrams per poll interval. uint64, count
  - udp_out_datagrams: outgoing UDP datagrams per poll interval. uint64, count
  - udp_in_errors: UDP receive errors per poll interval. uint64, count
  - udp_no_ports: UDP packets to closed ports per poll interval. uint64, count
  - udp_rcvbuf_errors: UDP recv-buffer errors per poll interval. uint64, count
  - udp_sndbuf_errors: UDP send-buffer errors per poll interval. uint64, count
- for key=`cc:<algo>`:
  - tcp_sockets: TCP socket samples (after top-N). uint64, count
  - tcp_tx_queue_bytes: sum of TCP tx-queue bytes over samples. uint64, bytes
  - tcp_rx_queue_bytes: sum of TCP rx-queue bytes over samples. uint64, bytes
  - tcp_retrans_pending: sum of pending retransmits over samples. uint64, count
  - tcp_cwnd_segs: average cwnd (segments) over samples. uint64, count
- `tcp_cc_top_n`: TCP socket sample limit for key=`cc:<algo>`; `0` disables `cc:*`

### NETFLOW

System pull metric of top-N network flow pairs per interface (no cgo, AF_PACKET capture).

- config section: `[[metrics.netflow]]`
- uses the same delivery path as other pull metrics: `scrape -> window aggregate -> collector batch/queue/failover -> Vector -> ClickHouse`; a materialized view expands the composite key into the separate `netflow_pairs` table
- key: composite `iface|proto|src_ip|src_port|dst_ip|dst_port`.
- vars: `bytes`, `packets`, `flows`
- all counters are computed per `scrape` window and reset after every poll (non-monotonic):
  - `bytes`: sum of packet bytes of the flow per window
  - `packets`: packet count of the flow per window
  - `flows`: new flows per window
- `flows` rule:
  - TCP: +1 only on a packet with flags `SYN && !ACK` (new connection)
  - UDP: +1 on the first packet of a tuple or after the tuple idles at least `flow_idle_timeout` (default `10s`)
- multiple interfaces via masks `ifaces = ["eth*","enp*","lo"]`
- `top_n` limits flow keys sent per window
- raw capture requires privileges: run the agent as `root` (or with `CAP_NET_RAW`)
- recommended mode: `percentiles = []` (last-only)

### PROCESS

System process metric filtered by `cpu_util`/`ram_util`/`iops` thresholds (OR logic: one threshold reached within the send window is enough).

- config section: `[[metrics.process]]`
- key: process name (`proc.Name`, without pid/cmdline).
- cpu_util: process CPU utilization. uint8, %
- ram_util: process RAM utilization = `rss_process/ram_total_host*100`. uint8, %
- iops: process block-device read+write operations. uint64, ops/sec

### MAGENT_INTERNAL

Built-in always-on metric of the agent's own state; created automatically, no config section required. Scrape/send period equals the global `metrics.send`; percentiles are disabled (`last` only).

- key: collector name (`[[collector]].name`) for delivery counters; worker instance name for scrape errors.
- for key=`<collector>`:
  - queue_pending: events currently in the collector's disk queue. uint64, count
  - batches_sent: successfully delivered batches since delivery start. uint64, count
  - batches_failed: failed batch delivery attempts (batch was queued, stayed queued, or dropped). uint64, count
  - overflow_dropped: events evicted by the `overflow=drop_oldest` policy. uint64, count
  - addr_switches: active address switches of the collector. uint64, count
- for key=`<worker instance>`:
  - scrape_errors: worker scrape errors since worker start. uint64, count
- ClickHouse table: `magent_internal` (included in the default list of `deploy/clickhouse/create_builtin_tables.sh`)

### SCRIPT

Custom pull metric from an external script `[[metrics.script.<name>]]`, whose stdout follows `format=json` (`{key,data}`) or `format=prometheus` (text exposition).

- config section: `[[metrics.script.<name>]]`
- key: for `json` taken from the `key` field; for `prometheus` fixed to `total` (labels are ignored).
- for `format=prometheus`: only `gauge/counter` are accepted, `var_mode=full|short`; var selection uses the standard `filter_var/drop_var/drop_event`.
- non-zero script exit code: data is not sent
- each script gets its own ClickHouse table (`<name>`)
- storage schema for script metrics matches all other metrics: `dt`, `dts`, `dtv`, tags, `key`, `var`, `agg`, `value`
- the user creates the standard ClickHouse table schema for the specific script, with the mandatory `key` field.

### HTTP-SERVER

Custom push metric: the agent runs an HTTP endpoint, accepts external data, and forwards it by the common rules.

- config section: `[[metrics.http_server.<name>]]`
- key: for `json` taken from the `key` field; for `prometheus` fixed to `total` (labels are ignored).
- HTTP: `POST http://<listen><path>`; request body in `json` or `prometheus` format (per `format` in config); successful intake: `204`
- for `format=prometheus`: only `gauge/counter` are accepted, `var_mode=full|short`; var selection uses the standard `filter_var/drop_var/drop_event`.
- no `scrape` period (data arrives from outside); only `send` (aggregation/emit period) applies
- `max_pending` bounds the buffer of accepted payloads; on overflow the policy is fixed: keep the old, drop the new (`503`)

### HTTP-CLIENT

Custom pull metric: the agent issues a `GET` to a URL, parses the response as `json` or `prometheus`, and forwards the data by the common rules.

- config section: `[[metrics.http_client.<name>]]`
- key: for `json` taken from the `key` field; for `prometheus` fixed to `total` (labels are ignored).
- supported response formats: `format=json` (`{key,data}` contract) and `format=prometheus` (text exposition, only gauge/counter)
- for `format=prometheus`: `var_mode=full|short`; var selection uses the standard `filter_var/drop_var/drop_event`
- HTTP: `GET` (only GET for now); the response format is set by the `format` field
- `url` supports path variables (path-escaped): `{dc},{host},{project},{role},{metric},{instance}`
- `instance` = worker name (`name` in config or auto-generated); used only for the URL and logs (never written to the event or the database)

### External Metrics Contract

Script, http_server, and http_client metrics share one input contract; this contract constrains what external producers may send.

- same delivery and aggregation rules as for built-in metrics
- common event fields (`dt`, `dts`, global tags, `metric`) are formed by the agent, not by the external source
- `format=json`: one JSON contract for script stdout, HTTP-SERVER body, and HTTP-CLIENT response
- root: an object or an array of objects; each object = 1 metric entity (1 `key`)
- minimal example:
```json
{"key":"total","data":{"util":{"last":67}}}
```
- `data.<var>`: a number, a bool (0/1), or an object `{last: <number>, kind?: "percent"|"number"}`
- `format=prometheus`: text exposition; only `gauge/counter`; `key` fixed to `total`
- `format=prometheus`: labels are ignored; when one `var` arrives in several series (different labels), the agent sums their values within one scrape
- `last/pXX` are computed by the agent and then stored/sent
- shared payload limit for `json` and `prometheus`: `16 MiB` (larger payloads are rejected)
- external metrics (`script/http_server/http_client`) with no data in the window are not emitted (no synthetic zero is generated)

## Configuration

This section defines the complete agent configuration surface referenced by the previous sections. The format is TOML; each entity has its own section: global parameters with tags, collectors, logs, metrics, and so on. A commented, load-checked example lives in `config.example.toml`.

### File Loading

These rules define how the agent finds and merges configuration.

- `-config` accepts a path to one TOML file or to a directory; for a directory the agent reads only `*.toml` files and merges them in lexicographical file-name order
- in a config directory, single tables (`[global]`, `[metrics]`, `[log.*]`, `[pprof]`, `[db.*]`) must be defined once; repeating them across files is a TOML error
- values may come from environment variables or a systemd service: `${VAR}` is expanded before TOML parsing

### Global Tags

Tags act as global variables present in every metric; they are defined in `[global]` and cannot be overridden in metric sections.

- mandatory tags: `dc` (datacenter), `project` (subproject within the product), `role` (host role in the product)
- when `host` is unset or empty, the OS hostname is used (`os.Hostname()`)
- a metric cannot create its own tags; tags are not affected by `drop_var`/`filter_var`/`drop_event`

### Metric Defaults

The `[metrics]` section sets defaults for all metrics; a metric is described by `[[metrics.<name>]]` (built-in), `[[metrics.script.<name>]]` (script), `[[metrics.http_server.<name>]]` (push HTTP), `[[metrics.http_client.<name>]]` (pull HTTP). A metric may be described any number of times; each section generates its own events.

- `scrape`/`send`: interval strings with time suffixes (for example `5s`, `1m`); set globally, overridable per metric
- `percentiles`: value aggregates computed once per send period over scrape periods; an array of integers (for example `[50,90,99]`) producing `pXX` keys
- when `percentiles` is set neither globally nor in the metric, there is no aggregation: only `last` is kept
- a metric inherits global `percentiles`; `percentiles = []` in a metric section disables percentiles for that metric (`last` only)
- default exception: `[[metrics.process]]` uses `scrape = "20s"` when no explicit `scrape` is set

### Metric Workers

Per-metric overrides and filters constrain what each worker collects and emits.

- `drop_var`/`filter_var` filter metric vars, not values; `drop_var` removes listed vars, `filter_var` keeps only listed vars; wildcard `*` is supported; both apply only to vars, never to tags
- `drop_event` drops the whole event when one or more conditions match; condition format: `<field><op><value>` with `op` in `=`, `!=`, `>`, `<`; wildcard `*` works with `=` and `!=` (for example `cmd=*postgres*`, `key!=core*`)
- example: `drop_event = ["iops>10000","key!=core*","var=rx_*"]`
- DISK key filtering example:
```toml
[[metrics.disk]]
name = "disk-main"
drop_event = ["key=/dev/loop*", "key=/dev/ram*"]

[[metrics.disk]]
name = "disk-nvme-only"
drop_event = ["key!=/dev/nvme*"]
filter_var = ["util", "*_bytes", "*_bytes_per_sec"]
```
- script metrics: `path` is required in the script section; `timeout` (default `5s`) bounds execution; `env` passes environment
- external metrics (`script`, `http_server`, `http_client`): `format` defaults to `json`; `prometheus` uses `var_mode=full|short` and `filter_var/drop_var/drop_event`
- `[[metrics.net]]`: `tcp_cc_top_n` (default `2000`); `0` disables key=`cc:<algo>`
- `[[metrics.process]]`: thresholds `cpu_util`, `ram_util`, `iops`; when set, events at or above at least one threshold are kept (OR logic); when all three are absent, the metric is not collected
- `[[metrics.netflow]]`: `ifaces` (wildcard masks), `top_n`, `flow_idle_timeout`

### Collectors

Collector sections define delivery targets: `addr` list, timing, overflow, compression, batching, and queueing.

- `addr`: array of `host:port` values; see [Failover](#failover) for the election scheme
- `timeout`: timeout per send attempt to one address; `retry_interval`: delay before retry/queue-drain cycles
- `health_interval`: healthcheck probe period over the `addr` list (default `3s`)
- `overflow`: input buffer overflow policy — `block` (default; writing waits for space or context cancellation, no immediate event drop) or `drop_oldest` (writing never blocks the metric worker; the oldest event is evicted and counted in `overflow_dropped`)
- `compression`: gRPC request compression — `none` (default) or `gzip`; `gzip` requires compression support on the Vector side (vector source/sink v2)
- `[[collector.batch]]`: `max_events` — events per batch, `max_age` — batch accumulation period; whichever fires first
- `[[collector.queue]]`: `enabled=false` by default; `dir` — queue directory, `max_events` — max queued events, `max_age` — max age of the oldest queued record; whichever fires first; with only one limit set, only it applies

### pprof

The `[pprof]` section enables runtime profiling of the agent.

- `enabled`: on/off
- `listen`: `host:port` for the pprof HTTP server (for example `127.0.0.1:6060`)

### ClickHouse Tooling

The `[db.clickhouse]` section configures the ClickHouse connection used by `docs/tests` tooling (the runtime sink is the collector, not this section).

- fields: `host`, `port`, `database`, `user`, `password`, `secure`, `dial_timeout`

### Hot Reload

Hot reload applies configuration changes without a process restart.

- triggered by `SIGHUP`: the agent re-reads and validates the `-config` path (file or directory), then recreates the runtime (workers/listeners); on apply failure it automatically restores the previous working runtime
- when the `[[collector]]` section is unchanged, delivery (gRPC connections, disk queues, unsent batches) is handed over to the new runtime without interruption
- after a successful reload, aggregation windows reset and new events accumulate under the new parameters

## Logging

This section defines agent log semantics and the corresponding configuration.

- custom package over the standard `slog`; console output is a colored single line
- two sinks: console and file; each has its own level; console lines are colored by level and text patterns (string/IP address/number)
- sinks: `[log.console]` and `[log.file]` with `enabled`, `level`, `format`; file adds `path`
- standard levels: `info/warn/error/panic/debug`; output as line or JSON per config; line format to console is colored by event level
- `info`: application-logic events (not system ones)
- `warn`: an error that does not break application logic; execution continues; data quality is guaranteed
- `error`: an error that breaks application or system logic; execution continues; data quality is not guaranteed
- `panic`: an error that breaks both application and system logic; execution is impossible
- `debug`: debug mode; every iteration in the program is logged

## Vector Collector

This section details the Transform iteration configuration.

- Vector processes the incoming Vector Protocol v2 log event
- it flattens one event into N rows: row count = sum over all vars in `data`; for example `disk` is the table name, `rx_io` is `var`, `last`/`p90` are `agg`, and `value` holds the values
- for keyed fields (for example cores): every key × every agg = one row
- routing to a table by metric name: `table = {metric_name}`
- the transformation is written in VRL (Vector Remap Language): a remap that expands into an array of events; no separate route is used
- results of `[[metrics.script.<name>]]` go to table `<name>`
- the standard event timestamp field (`.timestamp`) is renamed to `dtv`
- `host_ip` is added by the agent to every event at batch encoding (see [Event Structure](#event-structure))

## ClickHouse Storage

This section details the Storage iteration schemas referenced by Transform and Visualization.

### Metric Tables

One table per metric name; for script metrics the table name equals `<name>`. Table name = metric name:

```
dt: DateTime64(3) CODEC(DoubleDelta)              // event poll timestamp, once per scrape period, msec
dts: DateTime CODEC(DoubleDelta)                  // event send timestamp, once per send period, sec
dtv: DateTime DEFAULT now() CODEC(DoubleDelta)    // event processing time in Vector, sec
dc: LowCardinality(String)                        // hoster or site of the source server. mandatory tag
host: LowCardinality(String)                      // source host name. mandatory tag
project: LowCardinality(String)                   // project name or part of the system architecture (frontdb/back)
role: LowCardinality(String)                      // host role in the system architecture (frontdb/back/db ...)
host_ip: IPv6                                     // external IP of the source host
key: LowCardinality(String)                       // mandatory metric key
var: LowCardinality(String)                       // concrete metric variable: total, used, util, etc.
agg: LowCardinality(String)                       // aggregate name of the variable: last, p90, p99, etc.
value: UInt64                                     // concrete value of the agg aggregate
ORDER BY: (dt, host, key)
PARTITION BY: toYYYYMMDD(dt)
TTL: dt + INTERVAL 4 MONTH
```

For netflow analytics a chain is used:

- raw table `netflow` (universal schema `dt,dts,dtv,tags,key,var,agg,value`; stores input flow aggregates)
- materialized view `mv_netflow_pairs` (parses the composite `key` of the raw event and forms analytical pair fields)
- result table `netflow_pairs` (pairs `iface/proto/src_ip/src_port/dst_ip/dst_port` + `bytes/packets/flows`)

### netflow_pairs

The result analytical table for netflow:

```
dt: DateTime64(3) CODEC(DoubleDelta)               // aggregation window time in the agent
dts: DateTime CODEC(DoubleDelta)                   // send-to-collector time
dtv: DateTime CODEC(DoubleDelta)                   // insert time into the result table
dc: LowCardinality(String)                         // global tag
host: LowCardinality(String)                       // global tag
project: LowCardinality(String)                    // global tag
role: LowCardinality(String)                       // global tag
host_ip: IPv6                                      // IP of the event sender
iface: LowCardinality(String)                      // interface
proto: LowCardinality(String)                      // protocol (tcp/udp/...)
src_ip: IPv6                                       // source IP
src_port: UInt16                                   // source port
dst_ip: IPv6                                       // destination IP
dst_port: UInt16                                   // destination port
bytes: UInt64                                      // bytes per window
packets: UInt64                                    // packets per window
flows: UInt64                                      // flow records per window
ORDER BY: (dt, host, iface)
PARTITION BY: toYYYYMMDD(dt)
TTL: dt + INTERVAL 4 MONTH
```

## Roadmap

Planned extensions not yet implemented:

- snmp trap
- alerting

## Changelog

This section records functional changes in reverse chronological order, one entry per release date.

### 2026-09-04

Collector delivery rework and agent self-metrics:

- failover replaced by health-first election: all `addr` entries of a collector are probed concurrently every `health_interval` (Vector HealthCheck RPC, TCP dial fallback); the first alive address becomes active and receives every batch until it fails; a send failure triggers re-election and one batch retry without loss (previously: sequential failover with a full timeout per address)
- strict FIFO: when a collector's disk queue is non-empty, fresh batches are appended to the queue and delivery proceeds in write order (previously a fresh batch could overtake queued data)
- encode-once: a batch is marshaled to protobuf once; re-sends to other addresses and queue drains reuse the encoded bytes (previously re-encoded per address and re-marshaled on drain)
- non-blocking connection setup: lazy dial with keepalive replaces blocking dial in the send path
- new collector options: `overflow` (input buffer policy `block`|`drop_oldest`, default `block`; `drop_oldest` never blocks metric workers and counts evictions), `compression` (`none`|`gzip`, default `none`; gzip is verified against the vector source), `health_interval` (default `3s`)
- hot reload: when `[[collector]]` is unchanged, delivery (gRPC connections, disk queues, unsent batches) is handed over to the new runtime without interruption
- new built-in always-on metric `magent_internal`: per-collector `queue_pending`, `batches_sent`, `batches_failed`, `overflow_dropped`, `addr_switches` and per-worker `scrape_errors`, delivered through the common pipeline into the `magent_internal` table
- `host_ip` is resolved by route lookup (no network traffic) and baked into events at batch encoding, so it is present even for batches encoded during a collector outage
- deploy: `magent_internal` added to the default table list of `deploy/clickhouse/create_builtin_tables.sh`
- docs: this README rewritten in English with a normalized structure
