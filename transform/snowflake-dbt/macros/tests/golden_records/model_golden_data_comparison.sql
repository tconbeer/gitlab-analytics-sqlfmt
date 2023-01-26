{% macro model_golden_data_comparison(dbt_model) %}

{% set golden_data_model = dbt_model + "_golden_data" %}
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
                        = dbt_model.{{ column }}::varchar
                    then 0
                    else 1
                end
                {%- if not loop.last %} + {% endif %}
                {% endfor %}
            ) as is_incorrect
        from {{ ref(golden_data_model) }} golden_data
        left join
            {{ ref(dbt_model) }} dbt_model
            on {%- for column in gr_column_names %}
            dbt_model.{{ column }}::varchar = golden_data.{{ column }}::varchar
            {% if not loop.last %} and {% endif %}
            {% endfor %}
    )

select *
from check_data
where is_incorrect > 1

{% endmacro %}
