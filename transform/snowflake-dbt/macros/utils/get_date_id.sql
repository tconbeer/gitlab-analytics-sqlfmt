{% macro get_date_id(column) %}

    to_number(to_char({{ column }}::date, 'YYYYMMDD'), '99999999')

{% endmacro %}
