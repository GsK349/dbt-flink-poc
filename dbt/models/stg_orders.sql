{{ config(materialized='view') }}

select
  id,
  customer_id,
  order_ts,
  amount,
  status
from iceberg_catalog.`default`.iceberg_orders
