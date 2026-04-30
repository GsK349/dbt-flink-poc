-- Flink SQL for batch + streaming into Iceberg using AWS Glue catalog

CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'warehouse'='s3://flink-iceberg-warehouse/',
  'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS `default`;
USE `default`;

CREATE TABLE IF NOT EXISTS raw_orders (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/data/batch/sample_orders.csv',
  'format' = 'csv'
);

CREATE TABLE IF NOT EXISTS orders_stream (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE IF NOT EXISTS iceberg_orders (
  id STRING,
  customer_id STRING,
  order_ts TIMESTAMP(3),
  amount DECIMAL(10,2),
  status STRING
) WITH (
  'write-format' = 'parquet'
);

INSERT INTO iceberg_orders
SELECT * FROM raw_orders
UNION ALL
SELECT * FROM orders_stream;
