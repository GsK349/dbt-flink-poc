{{ config(materialized='view') }}

{{ soft_delete_active(
    source_table="iceberg_catalog.`default`.iceberg_orders_soft_delete",
    columns=['id', 'customer_id', 'order_ts', 'amount', 'status']
) }}
