{%- macro schema_union_all(
    schema_part, table_name, exclude_part="scratch", database_name=none
) -%}

    {% if database_name is not none %} {% set database = database_name %}

    {% else %} {% set database = target.database %}

    {% endif %}

    {% call statement("get_schemata", fetch_result=True) %}

        select distinct '"' || table_schema || '"."' || table_name || '"'
        from "{{ database }}".information_schema.tables
        where
            table_schema ilike '%{{ schema_part }}%'
            and table_schema not ilike '%{{ exclude_part }}%'
            and table_name ilike '{{ table_name }}'
        order by 1

    {%- endcall -%}

    {%- set value_list = load_result("get_schemata") -%}

    {%- if value_list and value_list["data"] -%}

        {%- set values = value_list["data"] | map(attribute=0) | list %}

        {% for schematable in values %}
            select *
            from "{{ database }}".{{ schematable }}

            {%- if not loop.last %}
                union all
            {% endif -%}

        {% endfor -%}

    {%- else -%} {{ return(1) }}

    {%- endif %}

{%- endmacro -%}
