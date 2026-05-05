-- ─────────────────────────────────────────────────────────
-- DEMO: SNAPSHOT (SCD Type 2)
--   - dbt tracks history automatically
--   - adds dbt_valid_from / dbt_valid_to columns
--   - 'check' strategy compares listed columns; row is
--     versioned only when one of them changes
--   - run with: dbt snapshot
--
--   NOTE: Snapshot support varies by adapter. With dbt-flink
--   you may need batch mode + Iceberg v2 tables. For Spark/
--   Glue this works out of the box.
-- ─────────────────────────────────────────────────────────
{% snapshot customers_snapshot %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'customer_id',
    strategy      = 'check',
    check_cols    = ['email', 'tier', 'country', 'is_active']
  )
}}

select
  customer_id,
  email,
  tier,
  country,
  is_active,
  current_timestamp as observed_at
from {{ source('iceberg', 'iceberg_orders') }}        -- replace with real customers source
-- ↑ wired to orders for demo only; in prod point at the customers table

{% endsnapshot %}
