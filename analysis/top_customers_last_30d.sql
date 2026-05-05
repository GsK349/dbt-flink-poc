-- ─────────────────────────────────────────────────────────
-- DEMO: ANALYSIS
--   - .sql files under analysis/ are NOT materialized
--   - `dbt compile --select top_customers_last_30d`
--     produces ready-to-paste SQL with all refs resolved
--   - perfect for ad-hoc investigations that should still
--     respect lineage (and show up in dbt docs)
-- ─────────────────────────────────────────────────────────

with revenue as (
  select
    customer_id,
    sum(revenue)      as revenue_30d,
    sum(order_count)  as orders_30d
  from {{ ref('fct_daily_revenue') }}
  where order_date >= current_date - interval '30' day
  group by customer_id
),
labelled as (
  select
    r.customer_id,
    r.revenue_30d,
    r.orders_30d,
    s.status_label    as latest_status_label
  from revenue r
  left join {{ ref('stg_orders') }}        o on o.customer_id = r.customer_id
  left join {{ ref('order_status_codes') }} s on s.status_code = o.status
)
select *
from labelled
order by revenue_30d desc
limit 50
