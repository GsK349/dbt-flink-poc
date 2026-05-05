{# ──────────────────────────────────────────────────────────
   DEMO: ADMIN MACRO (run via `dbt run-operation`)
     - wraps Iceberg's rewrite_data_files procedure
     - schedule it to compact small files from streaming writes
     - usage:
        dbt run-operation compact_iceberg_table \
          --args '{table_name: iceberg_orders, target_size_mb: 128}'
   ────────────────────────────────────────────────────────── #}
{% macro compact_iceberg_table(table_name, target_size_mb=128) %}
  {% set sql %}
    call iceberg_catalog.system.rewrite_data_files(
      table         => 'default.{{ table_name }}',
      options       => map['target-file-size-bytes', '{{ target_size_mb * 1024 * 1024 }}']
    )
  {% endset %}

  {{ log("▸ Compacting " ~ table_name ~ " (target " ~ target_size_mb ~ "MB)", info=True) }}
  {% if execute %}
    {% do run_query(sql) %}
    {{ log("✓ Compaction complete for " ~ table_name, info=True) }}
  {% endif %}
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   DEMO: COMPANION MACRO — expire old snapshots
     - GDPR + cost hygiene
     - usage:
        dbt run-operation expire_iceberg_snapshots \
          --args '{table_name: iceberg_orders, retention_days: 7}'
   ────────────────────────────────────────────────────────── #}
{% macro expire_iceberg_snapshots(table_name, retention_days=7) %}
  {% set sql %}
    call iceberg_catalog.system.expire_snapshots(
      table      => 'default.{{ table_name }}',
      older_than => current_timestamp - interval '{{ retention_days }}' day
    )
  {% endset %}
  {{ log("▸ Expiring snapshots older than " ~ retention_days ~ "d on " ~ table_name, info=True) }}
  {% if execute %}{% do run_query(sql) %}{% endif %}
{% endmacro %}
