{%- macro create_masking_policy_hide_boolean_column_values(database, schema) -%}

create masking policy if
not
exists "{{database}}".{{ schema }}.hide_boolean_column_values
as (val boolean)
returns boolean -> case
    when current_role() in ('DATA_OBSERVABILITY') then null else val
end
;

{%- endmacro -%}
