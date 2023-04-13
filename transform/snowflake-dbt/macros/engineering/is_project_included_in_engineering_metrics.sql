{% macro is_project_included_in_engineering_metrics() %}

    {%- call statement("get_project_ids", fetch_result=True) %}

        select distinct project_id
        from {{ ref("engineering_productivity_metrics_projects_to_include") }}
        where project_id is not null

    {%- endcall -%}

    {%- set value_list = load_result("get_project_ids") -%}

    {%- if value_list and value_list["data"] -%}
        {%- set values = value_list["data"] | map(attribute=0) | join(", ") %}
    {%- endif -%}

    {{ return(values) }}

{% endmacro %}
