-- Streaming with Soft Deletes
--
-- Instead of actually deleting rows, add an 'is_deleted' flag.
-- Combines upsert deduplication with reversible deletion.
--
-- Kafka event format:
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"created","is_deleted":false}
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"cancelled","is_deleted":true}
-- → Result: Row marked as deleted (but data preserved for audit)

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source with is_deleted flag
CREATE TABLE IF NOT EXISTS orders_soft_delete (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  is_deleted BOOLEAN,               -- Soft delete flag
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
  is_deleted BOOLEAN DEFAULT FALSE
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream orders with soft delete flag
INSERT INTO iceberg_catalog.`default`.iceberg_orders_soft_delete
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  is_deleted
FROM default_catalog.default_database.orders_soft_delete;

-- Note: dbt will create a view that filters out soft-deleted rows
-- See: dbt/models/mart_orders_active.sql
