{% macro raw_table_existence(schema, table_list) %}

with
    source as (

        select *
        from "{{ env_var('SNOWFLAKE_LOAD_DATABASE') }}".information_schema.tables

    ),
    counts as (

        select count(1) as row_count
        from source
        where
            lower(table_schema) = '{{schema|lower}}' and lower(table_name) in (
                {%- for table in table_list -%}

                '{{table|lower}}'{% if not loop.last %},{%- endif -%}

                {%- endfor -%}
            )

    )

select row_count
from counts
where
    row_count < array_size(
        array_construct(
            {%- for table in table_list -%}

            '{{table|lower}}'{% if not loop.last %},{%- endif -%}

            {%- endfor -%}
        )
    )

{% endmacro %}
