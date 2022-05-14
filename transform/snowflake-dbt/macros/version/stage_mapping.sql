{% macro stage_mapping(stage) %}

{%- call statement('get_mappings', fetch_result=True) %}

select stats_used_key_name
from {{ ref("version_usage_stats_to_stage_mappings") }}
where
    stage = '{{ stage }}'

    {%- endcall -%}

    {%- set value_list = load_result("get_mappings") -%}

    {%- if value_list and value_list["data"] -%}

    {%- set values = value_list["data"] | map(attribute=0) | list %}

    coalesce(
        sum(
            case when
            {% for feature in values %}

                    change.{{ feature }}_change > 0

                    {%- if not loop.last %} or
            {% else %} then change.user_count end
            {% endif -%}

            {% endfor -%}
        ),
        0
    )
    {%- else -%} {{ return(1) }}
    {%- endif %}

    as {{ stage }}_sum

{% endmacro %}
