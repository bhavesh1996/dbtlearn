{% macro convert_to_amount(column_name, scale=2) %}
    ({{ column_name }})::numeric(16, {{ scale }})
{% endmacro %}
