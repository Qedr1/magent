CREATE TABLE IF NOT EXISTS __DB__.__TABLE__
(
    dt DateTime64(3) CODEC(DoubleDelta),
    dts DateTime CODEC(DoubleDelta),
    dtv DateTime DEFAULT now() CODEC(DoubleDelta),
    dc LowCardinality(String),
    host LowCardinality(String),
    project LowCardinality(String),
    role LowCardinality(String),
    host_ip IPv6 DEFAULT toIPv6('::'),
    key LowCardinality(String),
    var LowCardinality(String),
    agg LowCardinality(String),
    value UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY (dt, host, key)
TTL dt + INTERVAL 4 MONTH;
