{# ──────────────────────────────────────────────────────────
   DEMO: CUSTOM GENERIC TEST
     - reusable test attached via YAML:
         columns:
           - name: amount
             tests: [positive_value]
     - any rows returned = FAIL
   ────────────────────────────────────────────────────────── #}
{% test positive_value(model, column_name, allow_zero=false) %}

  select *
  from {{ model }}
  where {{ column_name }} is not null
    and {{ column_name }} {% if allow_zero %}<{% else %}<={% endif %} 0

{% endtest %}
