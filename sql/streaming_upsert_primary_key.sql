-- Streaming Upsert with Primary Key
--
-- Instead of tracking operations, rely on primary keys.
-- If the same 'id' appears multiple times, Flink deduplicates based on latest event.
-- The Iceberg connector automatically handles UPDATE when key already exists.
--
-- This is the simplest approach for deduplication.
--
-- Kafka event format (no 'op' field needed):
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"created"}
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"in_progress"}
-- {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"completed"}
-- → Result: Only the latest (completed) is kept

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source with primary key
-- Flink will use 'id' to deduplicate
CREATE TABLE IF NOT EXISTS orders_upsert (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED  -- Tell Flink: 'id' uniquely identifies a row
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

-- Create Iceberg table with same primary key
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_upsert (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream orders with upsert
-- Flink handles deduplication automatically:
-- - If id exists in Iceberg: UPDATE the row
-- - If id doesn't exist: INSERT new row
INSERT INTO iceberg_catalog.`default`.iceberg_orders_upsert
SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status
FROM default_catalog.default_database.orders_upsert;

-- Result in Iceberg:
-- Each unique 'id' appears exactly once (latest state)
-- No duplicates, no audit trail (original values are overwritten)
