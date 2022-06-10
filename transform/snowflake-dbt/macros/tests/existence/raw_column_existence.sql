{% macro raw_column_existence(schema, table, column_list) %}

with
    source as (

        select *
        from "{{ env_var('SNOWFLAKE_LOAD_DATABASE') }}".information_schema.columns

    ),
    counts as (

        select count(1) as row_count
        from source
        where
            lower(table_schema) = '{{schema|lower}}' and lower(
                table_name
            ) = '{{table|lower}}' and lower(column_name) in (
                {%- for column in column_list -%}

                '{{column|lower}}'{% if not loop.last %},{%- endif -%}

                {%- endfor -%}
            )

    )

select row_count
from counts
where
    row_count < array_size(
        array_construct(
            {%- for column in column_list -%}

            '{{column|lower}}'{% if not loop.last %},{%- endif -%}

            {%- endfor -%}
        )
    )

{% endmacro %}
