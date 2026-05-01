-- Streaming with Soft Deletes
--
-- Instead of actually deleting rows, add an 'is_deleted' flag.
-- Combines PRIMARY KEY upsert with reversible deletion.
-- Operation field is included in the JSON message body.
-- This avoids the Flink 1.20.x MAP<VARBINARY,VARBINARY> header lookup bug.
--
-- Kafka event format:
-- Body: { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"created",
--         "is_deleted":false, "operation":"INSERT", "source":"order-service", "event_ts":"2026-04-28T10:00:00Z" }
--
-- Body: { "id":"1001", "customer_id":"cust_1001", "amount":15.50, "status":"cancelled",
--         "is_deleted":true, "operation":"DELETE", "source":"order-service", "event_ts":"2026-04-28T12:00:00Z" }
--
-- → Result: Row marked as deleted (is_deleted=true) but data preserved for audit/compliance

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source: operation and is_deleted are both in the JSON body
CREATE TABLE IF NOT EXISTS orders_soft_delete_body (
  id STRING NOT NULL,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN,             -- Soft delete flag (in payload)
  operation STRING,               -- CDC operation type: INSERT, UPDATE, DELETE (in payload)
  source STRING,                  -- Source system identifier (in payload)
  event_ts STRING                 -- Event timestamp ISO-8601 string (in payload)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_soft_delete_body',
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

-- Iceberg table with soft delete flag and upsert enabled
DROP TABLE IF EXISTS iceberg_catalog.`default`.iceberg_orders_soft_delete_v3;
CREATE TABLE iceberg_catalog.`default`.iceberg_orders_soft_delete_v3 (
  id STRING NOT NULL,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN,
  operation STRING,
  source STRING,
  event_timestamp STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'write.format.default' = 'parquet',
  'write.upsert.enabled' = 'true'
);

-- Stream orders with soft delete flag
INSERT INTO iceberg_catalog.`default`.iceberg_orders_soft_delete_v3
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  is_deleted,
  operation,
  source,
  event_ts AS event_timestamp
FROM default_catalog.default_database.orders_soft_delete_body;
