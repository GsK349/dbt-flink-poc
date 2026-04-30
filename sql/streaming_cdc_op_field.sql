-- Streaming CDC with Operation Field
--
-- Events from Kafka include 'op' field (INSERT, UPDATE, DELETE)
-- Flink stores all events in a raw log table
-- dbt materializes the final state (deduplicating by latest timestamp)
--
-- Kafka event format:
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"created","op":"INSERT","ts":"2026-04-28T08:00:00"}
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"completed","op":"UPDATE","ts":"2026-04-28T10:30:00"}
-- {"id":"1002","customer_id":"cust_1002","amount":42.00,"status":"cancelled","op":"DELETE","ts":"2026-04-28T12:00:00"}

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Step 1: Create Kafka source with operation field
USE CATALOG default_catalog;
USE default_database;

CREATE TABLE IF NOT EXISTS orders_cdc (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  op STRING,                           -- INSERT, UPDATE, DELETE
  ts TIMESTAMP(3),                     -- event timestamp
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_cdc_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Step 2: Set up Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Step 3: Create raw CDC log table (append-only)
-- This preserves the complete change history
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_cdc_log (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  op STRING,
  event_ts TIMESTAMP(3)
) WITH (
  'write.format.default' = 'parquet'
);

-- Step 4: Stream all CDC events to the raw log
-- This captures every INSERT/UPDATE/DELETE operation
INSERT INTO iceberg_catalog.`default`.iceberg_orders_cdc_log
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  op,
  ts as event_ts
FROM default_catalog.default_database.orders_cdc;

-- Note: dbt will create the final materialized view that:
-- 1. Deduplicates by id (keeping latest timestamp)
-- 2. Filters out deleted records (WHERE op != 'DELETE')
-- 3. Shows only the current state (latest version per id)
--
-- See: dbt/models/mart_orders_current.sql for the final state view
