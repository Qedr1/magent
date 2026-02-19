CREATE TABLE IF NOT EXISTS __DB__.__PAIRS_TABLE__
(
    dt DateTime64(3) CODEC(DoubleDelta),
    dts DateTime CODEC(DoubleDelta),
    dtv DateTime DEFAULT now() CODEC(DoubleDelta),
    dc LowCardinality(String),
    host LowCardinality(String),
    project LowCardinality(String),
    role LowCardinality(String),
    host_ip IPv6 DEFAULT toIPv6('::'),
    iface LowCardinality(String),
    proto LowCardinality(String),
    src_ip IPv6,
    src_port UInt16,
    dst_ip IPv6,
    dst_port UInt16,
    bytes UInt64,
    packets UInt64,
    flows UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY (dt, host, iface)
TTL dt + INTERVAL 4 MONTH;

CREATE MATERIALIZED VIEW IF NOT EXISTS __DB__.__MV_TABLE__
TO __DB__.__PAIRS_TABLE__
AS
SELECT
    dt,
    dts,
    dc,
    host,
    project,
    role,
    host_ip,
    arrayElement(parts, 1) AS iface,
    arrayElement(parts, 2) AS proto,
    toIPv6OrDefault(arrayElement(parts, 3)) AS src_ip,
    toUInt16OrZero(arrayElement(parts, 4)) AS src_port,
    toIPv6OrDefault(arrayElement(parts, 5)) AS dst_ip,
    toUInt16OrZero(arrayElement(parts, 6)) AS dst_port,
    sumIf(value, var = 'bytes') AS bytes,
    sumIf(value, var = 'packets') AS packets,
    sumIf(value, var = 'flows') AS flows
FROM
(
    SELECT
        dt,
        dts,
        dc,
        host,
        project,
        role,
        host_ip,
        var,
        value,
        splitByChar('|', key) AS parts
    FROM __DB__.__RAW_TABLE__
    WHERE agg = 'last'
)
WHERE length(parts) = 6
GROUP BY
    dt,
    dts,
    dc,
    host,
    project,
    role,
    host_ip,
    iface,
    proto,
    src_ip,
    src_port,
    dst_ip,
    dst_port;
