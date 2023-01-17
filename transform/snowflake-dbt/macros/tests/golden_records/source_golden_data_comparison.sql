{% macro source_golden_data_comparison(source_name, table_name) %}

{% set golden_data_model = source_name + "_" + table_name + "_raw_golden_data" %}
{% set gr_columns = adapter.get_columns_in_relation(ref(golden_data_model)) %}
{% set gr_column_names = gr_columns | map(attribute="name") | list %}

with
    check_data as (

        select
            sum(
                {%- for column in gr_column_names %}
                case
                    when
                        golden_data.{{ column }}::varchar
                        = source_table.{{ column }}::varchar
                    then 0
                    else 1
                end
                {%- if not loop.last %} + {% endif %}
                {% endfor %}
            ) as is_incorrect
        from {{ ref(golden_data_model) }} golden_data
        left join
            {{ source(source_name, table_name) }} source_table
            on {%- for column in gr_column_names %}
            source_table.{{ column }}::varchar = golden_data.{{ column }}::varchar
            {% if not loop.last %} and {% endif %}
            {% endfor %}
    )

select *
from check_data
where is_incorrect > 1

{% endmacro %}
