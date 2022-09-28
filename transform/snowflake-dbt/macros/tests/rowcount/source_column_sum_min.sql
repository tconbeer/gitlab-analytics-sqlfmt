{% macro source_column_sum_min(
    source_name, table, column, min_value, where_clause=None
) %}

with
    source as (select * from {{ source(source_name, table) }}),
    counts as (

        select sum({{ column }}) as sum_value
        from source
        {% if where_clause != None %} where {{ where_clause }} {% endif %}

    )

select sum_value
from counts
where sum_value < {{ min_value }}

{% endmacro %}
