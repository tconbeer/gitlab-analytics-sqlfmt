{%- macro create_masking_policy_hide_number_column_values(database, schema) -%}

create masking policy if
not exists "{{database}}".{{ schema }}.hide_number_column_values as (val number(38, 0))
returns number(38, 0) ->
case when current_role() in ('DATA_OBSERVABILITY') then 0 else val
end
;

{%- endmacro -%}
