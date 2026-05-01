-- Materialized view: Current state of orders
--
-- Deduplicates the CDC log by keeping only the latest event per order
-- Filters out deleted records
--
-- Input: iceberg_catalog.default.iceberg_orders_cdc_log (raw append-only log)
-- Output: Current state (one row per active order with latest values)

{{ config(materialized='view') }}

SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status,
  latest_op as operation,
  latest_event_ts as last_modified_at
FROM (
  SELECT
    id,
    customer_id,
    order_ts,
    amount,
    status,
    operation as latest_op,
    event_timestamp as latest_event_ts,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_timestamp DESC) as rn
  FROM iceberg_catalog.`default`.iceberg_orders_cdc_log
  WHERE operation <> 'DELETE'  -- Exclude deleted records
)
WHERE rn = 1  -- Keep only latest version per order
