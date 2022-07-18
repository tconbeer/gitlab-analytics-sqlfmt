{%- macro scd_latest_state(source="base", max_column="_task_instance") -%}

,
max_task_instance as (
    select max({{ max_column }}) as max_column_value from {{ source }}

),
filtered as (

    select *
    from {{ source }}
    where {{ max_column }} = (select max_column_value from max_task_instance)

)

select *
from filtered

{%- endmacro -%}
