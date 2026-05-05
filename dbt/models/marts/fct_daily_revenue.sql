-- ─────────────────────────────────────────────────────────
-- DEMO: INCREMENTAL MATERIALIZATION
--   - first run: full build
--   - subsequent runs: only process rows newer than last run
--   - is_incremental() + {{ this }} are dbt Jinja helpers
--   - merge strategy upserts on the unique_key
-- ─────────────────────────────────────────────────────────
{{ config(
    materialized = 'incremental',
    unique_key   = ['order_date', 'customer_id'],
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns'
) }}

select
  cast(order_ts as date)      as order_date,
  customer_id,
  count(*)                    as order_count,
  sum(amount)                 as revenue,
  {{ cents_to_dollars('sum(amount * 100)') }} as revenue_dollars_demo,
  current_timestamp           as dbt_loaded_at
from {{ source('iceberg', 'iceberg_orders') }}

{% if is_incremental() %}
  -- only process rows arrived since the latest day we already loaded
  where order_ts >= (
    select coalesce(max(order_date), date '1900-01-01') from {{ this }}
  )
{% endif %}

group by cast(order_ts as date), customer_id
