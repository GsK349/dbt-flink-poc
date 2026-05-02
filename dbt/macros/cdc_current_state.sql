{# Project an append-only CDC log into "current state" per key.

   Args:
     source_table   Fully qualified source, e.g. "iceberg_catalog.`default`.iceberg_orders_cdc_log"
     key_columns    List of PK columns, e.g. ['id']  (composite keys supported)
     order_column   Column used to pick latest event per key (default: 'event_ts')
     op_column      CDC op column (default: 'op')
     delete_value   Value of op_column meaning "deleted" (default: 'DELETE')
     extra_columns  Other columns to carry through, e.g. ['customer_id','amount','status','order_ts']
#}
{% macro cdc_current_state(
    source_table,
    key_columns,
    extra_columns,
    order_column='event_ts',
    op_column='op',
    delete_value='DELETE'
) %}
SELECT
  {{ key_columns | join(', ') }},
  {{ extra_columns | join(', ') }},
  {{ op_column }} AS operation,
  {{ order_column }} AS last_modified_at
FROM (
  SELECT
    {{ key_columns | join(', ') }},
    {{ extra_columns | join(', ') }},
    {{ op_column }},
    {{ order_column }},
    ROW_NUMBER() OVER (
      PARTITION BY {{ key_columns | join(', ') }}
      ORDER BY {{ order_column }} DESC
    ) AS rn
  FROM {{ source_table }}
  WHERE {{ op_column }} <> '{{ delete_value }}'
)
WHERE rn = 1
{% endmacro %}
