{%- macro distinct_source(source) -%}

distinct_source as (

    select
        {{ dbt_utils.star(from=source, except=["_UPLOADED_AT", "_TASK_INSTANCE"]) }},
        min(dateadd('sec', _uploaded_at, '1970-01-01'))::timestamp as valid_from,
        max(dateadd('sec', _uploaded_at, '1970-01-01'))::timestamp as max_uploaded_at,
        max(_task_instance)::varchar as max_task_instance
    from {{ source }}
    group by
        {{ dbt_utils.star(from=source, except=["_UPLOADED_AT", "_TASK_INSTANCE"]) }}

)

{%- endmacro -%}
