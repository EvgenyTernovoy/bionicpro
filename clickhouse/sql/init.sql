CREATE TABLE crm_customers_kafka
(
    before String,
    after String,
    op String,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'crm.public.customers',
    kafka_group_name = 'clickhouse_crm_customers',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE crm_customers_staging
(
    id UInt64,
    name String,
    email String,
    op String,
    ts_ms UInt64
)
ENGINE = MergeTree()
ORDER BY ts_ms;

CREATE MATERIALIZED VIEW mv_crm_customers
TO crm_customers_staging
AS
SELECT
    JSONExtractUInt(after, 'id') AS id,
    JSONExtractString(after, 'name') AS name,
    JSONExtractString(after, 'email') AS email,
    op,
    ts_ms
FROM crm_customers_kafka;

-- Telemetry config

CREATE TABLE telemetry_events_kafka
(
    before String,
    after String,
    op String,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'telemetry.public.telemetry',
    kafka_group_name = 'clickhouse_telemetry_events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE telemetry_events_staging
(
    id String,
    client_id String,
    metric_type String,
    metric_value String,
    event_time DateTime64(6),
    payload String
)
ENGINE = MergeTree()
ORDER BY (client_id, event_time);



CREATE MATERIALIZED VIEW mv_telemetry_events
TO telemetry_events_staging
AS
SELECT
    JSONExtractUInt(after, 'id') AS id,
    JSONExtractString(after, 'client_id') AS client_id,
    -- используем JSONExtractUInt и приводим к Float64 для деления
    toDateTime64(JSONExtractUInt(after, 'event_time') / 1000000.0, 6) AS event_time,
    JSONExtractString(after, 'metric_type') AS metric_type,
    JSONExtractString(JSONExtractRaw(after, 'metric_value'), 'value') AS metric_value,
    JSONExtractString(after, 'payload') AS payload
FROM telemetry_events_kafka;


-- Итоговая таблица телеметрии
CREATE TABLE telemetry_metrics
(
    email String,
    metric_type String,
    metric_value String,
    event_time DateTime64(6),
    payload String
)
ENGINE = MergeTree()
ORDER BY (email, event_time);

-- Materialized View для автоматической вставки данных
CREATE MATERIALIZED VIEW mv_telemetry_metrics
TO telemetry_metrics
AS
SELECT
    c.email AS email,
    t.metric_type,
    t.metric_value,
    t.event_time,
    t.payload
FROM telemetry_events_staging t
LEFT JOIN crm_customers_staging c
    ON t.client_id = c.email;

