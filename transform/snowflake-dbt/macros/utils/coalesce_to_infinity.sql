{%- macro coalesce_to_infinity(value) -%}

    coalesce({{ value }}, '9999-12-31'::timestamp)

{%- endmacro -%}
