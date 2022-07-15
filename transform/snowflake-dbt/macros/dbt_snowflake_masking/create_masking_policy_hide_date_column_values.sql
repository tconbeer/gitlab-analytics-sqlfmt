{%- macro create_masking_policy_hide_date_column_values(database, schema) -%}

create masking policy if
not exists "{{database}}".{{ schema }}.hide_date_column_values as (val date)
returns date -> case
    when current_role() in ('DATA_OBSERVABILITY') then null else val
end
;

{%- endmacro -%}
