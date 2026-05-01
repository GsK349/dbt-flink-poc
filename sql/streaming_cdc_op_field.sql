-- Streaming CDC with Operation Field in Message Body
--
-- Operation field (INSERT, UPDATE, DELETE) is included in the JSON message body.
-- This avoids the Flink 1.20.x MAP<VARBINARY,VARBINARY> header lookup bug.
--
-- Kafka event format:
-- Body: { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"created",
--         "operation":"INSERT", "source":"order-service", "event_ts":"2026-04-28T08:00:00Z" }
--
-- Body: { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"completed",
--         "operation":"UPDATE", "source":"order-service", "event_ts":"2026-04-28T10:30:00Z" }
--
-- Body: { "id":"1002", "customer_id":"cust_1002", "amount":42.00, "status":"cancelled",
--         "operation":"DELETE", "source":"order-service", "event_ts":"2026-04-28T12:00:00Z" }

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source: operation field is part of the JSON body
CREATE TABLE IF NOT EXISTS orders_cdc_with_op (
  id STRING NOT NULL,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  operation STRING,               -- CDC operation type: INSERT, UPDATE, DELETE
  source STRING,                  -- Source system identifier
  event_ts STRING,                -- Event timestamp (ISO-8601 string)
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_cdc_op_body',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Raw CDC log table (append-only, stores all operations)
DROP TABLE IF EXISTS iceberg_catalog.`default`.iceberg_orders_cdc_log;
CREATE TABLE iceberg_catalog.`default`.iceberg_orders_cdc_log (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  operation STRING,
  source STRING,
  event_timestamp STRING,
  ingested_at TIMESTAMP(3)
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream all CDC events to the raw log
INSERT INTO iceberg_catalog.`default`.iceberg_orders_cdc_log
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  operation,
  source,
  event_ts AS event_timestamp,
  CURRENT_TIMESTAMP AS ingested_at
FROM default_catalog.default_database.orders_cdc_with_op;
