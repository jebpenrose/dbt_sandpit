{% macro clean_numeric(column_name) %}
    (REPLACE(REPLACE({{column_name}}, '$',''),',',''))::NUMERIC(10,2) AS {{column_name}}
{% endmacro %}