-- Streaming with Soft Deletes
--
-- Instead of actually deleting rows, add an 'is_deleted' flag.
-- Combines PRIMARY KEY upsert with reversible deletion.
-- Operation comes from Kafka header, is_deleted flag is in payload.
--
-- Kafka event format:
-- Headers: { "operation": "INSERT" }
-- Body:    { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"created", "is_deleted":false }
--
-- Headers: { "operation": "DELETE" }
-- Body:    { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"cancelled", "is_deleted":true }
--
-- → Result: Row marked as deleted (is_deleted=true) but data preserved for audit/compliance

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source with is_deleted flag AND operation header
CREATE TABLE IF NOT EXISTS orders_soft_delete (
  -- Data columns (from message body)
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN DEFAULT FALSE,  -- Soft delete flag (in payload)

  -- Metadata columns (from Kafka headers)
  operation STRING METADATA FROM 'value.headers.operation',  -- INSERT, UPDATE, DELETE
  source STRING METADATA FROM 'value.headers.source',        -- origin of event
  event_timestamp STRING METADATA FROM 'value.headers.timestamp',

  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Set up Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Create Iceberg table with is_deleted column
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_soft_delete (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN DEFAULT FALSE,  -- Soft delete flag
  operation STRING,                  -- From header
  source STRING,                     -- From header
  event_timestamp STRING
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream orders with soft delete flag
-- Operation and source come from Kafka headers, not payload
INSERT INTO iceberg_catalog.`default`.iceberg_orders_soft_delete
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  is_deleted,                    -- From payload
  operation,                     -- From Kafka header
  source,                        -- From Kafka header
  event_timestamp                -- From Kafka header
FROM default_catalog.default_database.orders_soft_delete
WHERE operation IS NOT NULL;     -- Only process events with operation header

-- Note: dbt will create a view that filters out soft-deleted rows
-- See: dbt/models/mart_orders_active.sql
--
-- Advantages:
-- ✅ Preserves deletion history (data not destroyed)
-- ✅ Can undelete by setting is_deleted=false
-- ✅ Audit trail of why deleted (operation=DELETE in header)
-- ✅ GDPR/compliance friendly (can prove deletion)
-- ✅ Schema clean (operation not in data payload)
