{% macro get_date_pt_id(column) %}

    to_number(
        to_char(
            convert_timezone('America/Los_Angeles', {{ column }})::date, 'YYYYMMDD'
        ),
        '99999999'
    )

{% endmacro %}
