-- Flink SQL job: Batch processing from partitioned S3 Parquet files
-- Read Hive-style partitioned parquet files from S3 and write to Iceberg

SET 'execution.runtime-mode' = 'batch';

-- Step 1: Create a source table that reads from PARTITIONED S3 parquet files
-- Partition structure example:
--   s3://your-bucket/orders/year=2026/month=04/day=28/file1.parquet
--   s3://your-bucket/orders/year=2026/month=04/day=29/file2.parquet
--   s3://your-bucket/orders/year=2026/month=05/day=01/file3.parquet

USE CATALOG default_catalog;
USE default_database;

CREATE TABLE IF NOT EXISTS orders_from_s3_partitioned (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING,
  -- Partition columns (MUST appear after data columns)
  year INT,
  month INT,
  day INT
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://your-bucket-name/path/to/partitioned/orders/',
  'format' = 'parquet',
  'partition.fields' = 'year,month,day'
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

-- Step 4: Batch INSERT with optional WHERE clause to filter partitions
-- Load only specific date range to save time/cost
INSERT INTO iceberg_catalog.`default`.iceberg_orders_batch
SELECT id, customer_id, order_ts, amount, status
FROM default_catalog.default_database.orders_from_s3_partitioned
WHERE year = 2026 AND month = 4;  -- Optional: filter specific partitions

-- Alternative: Load all partitions (no WHERE clause)
-- INSERT INTO iceberg_catalog.`default`.iceberg_orders_batch
-- SELECT id, customer_id, order_ts, amount, status
-- FROM default_catalog.default_database.orders_from_s3_partitioned;
