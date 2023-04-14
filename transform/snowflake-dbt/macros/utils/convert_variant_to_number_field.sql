{%- macro convert_variant_to_number_field(value) -%}

    try_to_number({{ value }}::varchar)

{%- endmacro -%}
