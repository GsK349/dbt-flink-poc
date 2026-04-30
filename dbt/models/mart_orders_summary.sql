{{ config(materialized='view') }}

select
  customer_id,
  FLOOR(order_ts TO DAY) as order_date,
  count(*) as order_count,
  sum(amount) as total_amount
from {{ ref('stg_orders') }}
group by customer_id, FLOOR(order_ts TO DAY)
