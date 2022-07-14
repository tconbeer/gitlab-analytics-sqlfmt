{%- macro create_masking_policy_hide_string_column_values(database, schema) -%}

create masking policy if
not exists "{{database}}".{{ schema }}.hide_string_column_values as (val string)
returns string ->
case when current_role() in ('DATA_OBSERVABILITY') then '**********' else val
end
;

{%- endmacro -%}
