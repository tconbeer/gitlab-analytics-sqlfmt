{%- macro convert_variant_to_boolean_field(value) -%}

try_to_boolean({{ value }}::varchar)

{%- endmacro -%}
