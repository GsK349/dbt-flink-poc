{% macro setup_iceberg_catalog() %}
{% if execute %}
{% set sql %}
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type' = 'iceberg',
  'warehouse' = 's3://flink-iceberg-warehouse/',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
)
{% endset %}
{% set result = run_query(sql) %}
{{ log("Iceberg catalog registered", info=True) }}
{% endif %}
{% endmacro %}
