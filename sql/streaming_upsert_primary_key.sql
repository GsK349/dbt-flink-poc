-- Streaming Upsert with Primary Key
--
-- Instead of tracking operations via headers, rely on PRIMARY KEY.
-- If the same 'id' appears multiple times, Flink deduplicates based on latest event.
-- The Iceberg connector automatically handles UPDATE when key already exists.
--
-- This is the simplest approach - no need for operation tracking.
--
-- Kafka event format (no 'operation' header needed):
-- Headers: {} (can be empty)
-- Body:    {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"created"}
--
-- Headers: {} (empty)
-- Body:    {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"in_progress"}
--
-- Headers: {} (empty)
-- Body:    {"id":"1001","customer_id":"cust_1001","amount":15.50,"status":"completed"}
--
-- → Result in Iceberg: Only the latest (completed) is kept
-- Flink automatically deduplicates based on PRIMARY KEY (id)

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

USE CATALOG default_catalog;
USE default_database;

-- Kafka source with PRIMARY KEY
-- Flink will use 'id' to deduplicate and handle upserts
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
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Set up Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = '${ICEBERG_WAREHOUSE_PATH}',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Create Iceberg table with same PRIMARY KEY
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_upsert (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write.format.default' = 'parquet'
);

-- Stream orders with PRIMARY KEY based upsert
-- Flink handles deduplication automatically:
-- - If id exists in Iceberg: UPDATE the row (Iceberg commits new snapshot)
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
-- Each unique 'id' appears exactly once with latest state
-- No duplicates
-- No audit trail (original values are overwritten)
-- Simple and efficient
--
-- Use this when:
-- ✅ You don't need to track old values
-- ✅ Latest state is all you care about
-- ✅ DELETE operations are not important
