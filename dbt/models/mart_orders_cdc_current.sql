{{ config(materialized='view') }}

{{ cdc_current_state(
    source_table="iceberg_catalog.`default`.iceberg_orders_cdc_log",
    key_columns=['id'],
    extra_columns=['customer_id', 'order_ts', 'amount', 'status']
) }}
