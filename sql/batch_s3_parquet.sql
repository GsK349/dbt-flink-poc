-- Flink SQL job: Batch processing from S3 Parquet files
-- Read unpartitioned parquet files from S3 and write to Iceberg

SET 'execution.runtime-mode' = 'batch';

-- Step 1: Create a source table that reads from S3 parquet files
USE CATALOG default_catalog;
USE default_database;

CREATE TABLE IF NOT EXISTS orders_from_s3 (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://your-bucket-name/path/to/parquet/files/',
  'format' = 'parquet'
);

-- Step 2: Set up Iceberg catalog
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

-- Step 3: Create Iceberg sink table
CREATE TABLE IF NOT EXISTS iceberg_catalog.`default`.iceberg_orders_batch (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write.format.default' = 'parquet'
);

-- Step 4: Batch INSERT - read S3 parquet and write to Iceberg
INSERT INTO iceberg_catalog.`default`.iceberg_orders_batch
SELECT id, customer_id, order_ts, amount, status
FROM default_catalog.default_database.orders_from_s3;
