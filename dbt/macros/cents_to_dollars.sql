{# ──────────────────────────────────────────────────────────
   DEMO: REUSABLE MACRO
     - parameterised SQL snippet, callable from any model
     - replaces copy-pasted formulas with one source of truth
     - default precision can be overridden per call site
   ────────────────────────────────────────────────────────── #}
{% macro cents_to_dollars(column_expr, precision=2) %}
  cast(({{ column_expr }}) / 100.0 as decimal(18, {{ precision }}))
{% endmacro %}
