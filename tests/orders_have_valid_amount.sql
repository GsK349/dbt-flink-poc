-- ─────────────────────────────────────────────────────────
-- DEMO: SINGULAR TEST
--   - any .sql file under tests/ that returns rows = FAIL
--   - run via `dbt test` (also via `dbt build`)
--   - this one catches negative or null amounts that
--     somehow slipped past the streaming validators
-- ─────────────────────────────────────────────────────────

select
  id,
  customer_id,
  amount,
  status,
  order_ts
from {{ ref('stg_orders') }}
where amount is null
   or amount < 0
   or amount > 1000000
