{% macro model_column_sum_min(model_name, column, min_value, where_clause=None) %}

    with
        source as (select * from {{ ref(model_name) }}),
        counts as (

            select sum({{ column }}) as sum_value
            from source
            {% if where_clause != None %} where {{ where_clause }} {% endif %}

        )

    select sum_value
    from counts
    where sum_value < {{ min_value }}

{% endmacro %}
