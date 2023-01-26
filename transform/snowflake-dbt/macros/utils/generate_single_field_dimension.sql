{% macro generate_single_field_dimension(
    model_name,
    id_column,
    id_column_name,
    dimension_column,
    dimension_column_name,
    where_clause=None
) %}

with
    source_data as (

        select *
        from {{ ref(model_name) }}
        where
            {{ dimension_column }} is not null
            {% if where_clause != None %} and {{ where_clause }} {% endif %}

    ),
    unioned as (

        select distinct
            {{ dbt_utils.surrogate_key([id_column]) }} as {{ id_column_name }},
            {{ dimension_column }} as {{ dimension_column_name }}
        from source_data
        union all
        select
            md5('-1') as {{ id_column_name }},
            'Missing {{dimension_column_name}}' as {{ dimension_column_name }}

    )

{%- endmacro -%}
