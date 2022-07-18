{%- macro null_or_missing(column, new_column_name) -%}

iff(
    {{ column }} is null or {{ column }} like 'Missing%',
    'Missing {{new_column_name}}',
    {{ column }}
) as {{ new_column_name }}

{%- endmacro -%}
