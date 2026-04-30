-- Active Orders (Soft Delete View)
--
-- Filters out soft-deleted records
-- Always used by downstream models
--
-- Input: iceberg_catalog.default.iceberg_orders_soft_delete (with is_deleted flag)
-- Output: Only active (not deleted) orders

{{ config(materialized='view') }}

SELECT
  id,
  customer_id,
  order_ts,
  amount,
  status
FROM iceberg_catalog.`default`.iceberg_orders_soft_delete
WHERE is_deleted = FALSE  -- Show only active records
