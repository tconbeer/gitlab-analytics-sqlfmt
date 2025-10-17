{% macro get_keyed_nulls(columns) %} coalesce({{ columns }}, md5(-1)) {% endmacro %}
