{# Project a soft-delete table into "active rows only".

   Args:
     source_table     Fully qualified source table
     columns          Columns to select (list)
     deleted_column   Boolean flag column (default: 'is_deleted')
#}
{% macro soft_delete_active(source_table, columns, deleted_column='is_deleted') %}
SELECT
  {{ columns | join(', ') }}
FROM {{ source_table }}
WHERE {{ deleted_column }} = FALSE
{% endmacro %}
