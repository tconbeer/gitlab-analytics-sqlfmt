{%- macro create_masking_policy_hide_float_column_values(database, schema) -%}

create masking policy if
not exists "{{database}}".{{ schema }}.hide_float_column_values as (val float)
returns float ->
case when current_role() in ('DATA_OBSERVABILITY') then 0 else val
end
;

{%- endmacro -%}
