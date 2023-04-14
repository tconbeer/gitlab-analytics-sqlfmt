{% macro model_union_limit(model_name, column_name, day_limit=30) %}

    with base as (select * from {{ ref(model_name) }})

    select *
    from base
    where {{ column_name }} >= dateadd('day', -{{ day_limit }}, current_date())

{% endmacro %}
