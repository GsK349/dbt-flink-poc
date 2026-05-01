-- Flink SQL job: read JSON events from Kafka and write to Iceberg

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Step 1: Create Kafka source in Flink's in-memory catalog (not Iceberg)
USE CATALOG default_catalog;
USE default_database;

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

-- Step 2: Set up Iceberg catalog and create sink table
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = '${ICEBERG_WAREHOUSE_PATH}',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write.format.default' = 'parquet'
);

-- Step 3: Stream Kafka → Iceberg using fully-qualified table names
INSERT INTO iceberg_catalog.`default`.iceberg_orders
SELECT id, customer_id, order_ts, amount, status
FROM default_catalog.default_database.orders_stream;
