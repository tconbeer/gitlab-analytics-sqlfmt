{% macro schema_union_limit(
    schema_part, table_name, column_name, day_limit=30, database_name=none
) %}

with
    base_union as (

        {{ schema_union_all(schema_part, table_name, database_name=database_name) }}

    )

select *
from base_union
where {{ column_name }} >= dateadd('day', -{{ day_limit }}, current_date())

{% endmacro %}
