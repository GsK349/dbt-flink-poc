-- ─────────────────────────────────────────────────────────
-- DEMO: SINGULAR TEST (referential integrity in CDC log)
--   - every UPDATE event must be preceded by an INSERT
--   - failing rows are streaming bugs worth investigating
-- ─────────────────────────────────────────────────────────

with updates as (
  select id, min(event_ts) as first_update_ts
  from {{ source('iceberg', 'iceberg_orders_cdc_log') }}
  where op in ('UPDATE', 'U')
  group by id
),
inserts as (
  select id, min(event_ts) as first_insert_ts
  from {{ source('iceberg', 'iceberg_orders_cdc_log') }}
  where op in ('INSERT', 'I')
  group by id
)
select
  u.id,
  u.first_update_ts,
  i.first_insert_ts
from updates u
left join inserts i using (id)
where i.id is null
   or u.first_update_ts < i.first_insert_ts
