-- Flink SQL pipeline without Iceberg
-- This reads batch data from a local filesystem JSON file and streaming data from Kafka,
-- then writes the combined result to a filesystem sink.

CREATE TABLE IF NOT EXISTS raw_orders (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/data/batch/sample_orders.json',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS orders_stream (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS output_orders (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/orders_no_iceberg',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

INSERT INTO output_orders
SELECT * FROM raw_orders
UNION ALL
SELECT * FROM orders_stream;
