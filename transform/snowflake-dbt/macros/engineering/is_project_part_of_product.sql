{% macro is_project_part_of_product() %}

{%- call statement("get_project_ids", fetch_result=True) %}

select distinct project_id
from {{ ref("projects_part_of_product") }}
where project_id is not null

{%- endcall -%}

{%- set value_list = load_result("get_project_ids") -%}

{%- if value_list and value_list["data"] -%}
{%- set values = value_list["data"] | map(attribute=0) | join(", ") %}
{%- endif -%}

{{ return(values) }}

{% endmacro %}
