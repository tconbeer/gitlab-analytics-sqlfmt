{% macro model_rowcount(model_name, count, where_clause=None) %}

with
    source as (select * from {{ ref(model_name) }}),
    counts as (

        select count(*) as row_count
        from source
        {% if where_clause != None %} where {{ where_clause }} {% endif %}

    )

select row_count
from counts
where row_count < {{ count }}

{% endmacro %}
