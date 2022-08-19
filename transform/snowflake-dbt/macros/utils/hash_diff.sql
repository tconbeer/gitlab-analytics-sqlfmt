{% macro hash_diff(cte_ref, return_cte, columns) %}

,
hashing as (

    select *, {{ dbt_utils.surrogate_key(columns) }} as prev_hash from {{ cte_ref }}

),
{{ return_cte }} as (

    {%- set columns = adapter.get_columns_in_relation(this) -%}

    {%- set column_names = [] -%}

    {%- for column in columns -%}

    {%- set _ = column_names.append(column.name) -%}

    {% endfor %}

    {% if "LAST_CHANGED" in column_names %}

    select
        hashing.*,
        case
            when hashing.prev_hash = t.prev_hash
            then last_changed
            else current_timestamp()
        end as last_changed
    from hashing
    left join {{ this }} as t on t.prev_hash = hashing.prev_hash

    {% else %}select *, current_timestamp() as last_changed from hashing

    {% endif %}

)
{% endmacro %}
