-- Streaming CDC with Kafka Headers (RECOMMENDED APPROACH)
--
-- Operation field (INSERT, UPDATE, DELETE) comes from Kafka message HEADERS
-- NOT from the message payload, keeping the data schema clean.
--
-- Kafka event format:
-- Headers: { "operation": "INSERT", "source": "order-service", "timestamp": "2026-04-28T08:00:00" }
-- Body:    { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"created" }
--
-- Headers: { "operation": "UPDATE", "source": "order-service", "timestamp": "2026-04-28T10:30:00" }
-- Body:    { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"completed" }
--
-- Headers: { "operation": "DELETE", "source": "order-service", "timestamp": "2026-04-28T12:00:00" }
-- Body:    { "id":"1002", "customer_id":"cust_1002", "amount":42.00, "status":"cancelled" }

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Step 1: Create Kafka source with HEADERS as METADATA
-- Headers are read using METADATA FROM 'value.headers.HEADER_NAME'
USE CATALOG default_catalog;
USE default_database;

CREATE TABLE IF NOT EXISTS orders_cdc_with_headers (
  -- Data columns (from message body/payload)
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,

  -- Metadata columns (from Kafka message headers)
  -- These are read-only virtual columns
  operation STRING METADATA FROM 'value.headers.operation',        -- INSERT, UPDATE, DELETE
  source STRING METADATA FROM 'value.headers.source',              -- order-service, admin-api, etc.
  event_timestamp STRING METADATA FROM 'value.headers.timestamp',  -- ISO-8601 timestamp

  -- Watermark for ordering
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
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
-- This preserves the complete change history with operations
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_cdc_log (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  operation STRING,                -- From header
  source STRING,                   -- From header
  event_timestamp STRING,          -- From header
  ingested_at TIMESTAMP(3)
) WITH (
  'write.format.default' = 'parquet'
);

-- Step 4: Stream all CDC events to the raw log
-- Extract headers and store with data
INSERT INTO iceberg_catalog.`default`.iceberg_orders_cdc_log
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  operation,                       -- From Kafka header
  source,                          -- From Kafka header
  event_timestamp,                 -- From Kafka header
  CURRENT_TIMESTAMP as ingested_at
FROM default_catalog.default_database.orders_cdc_with_headers
WHERE operation IS NOT NULL;       -- Only process events with operation header

-- Note: dbt will create the final materialized view that:
-- 1. Deduplicates by id (keeping latest event_timestamp)
-- 2. Filters out deleted records (WHERE operation != 'DELETE')
-- 3. Shows only the current state (latest version per id)
--
-- See: dbt/models/mart_orders_cdc_current.sql for the final state view
--
-- Advantages of using headers:
-- ✅ Data schema is clean (no 'operation' column in payload)
-- ✅ Headers not stored in Iceberg (more efficient)
-- ✅ Consumers can ignore headers if needed
-- ✅ Industry standard (used by Debezium, Confluent)
