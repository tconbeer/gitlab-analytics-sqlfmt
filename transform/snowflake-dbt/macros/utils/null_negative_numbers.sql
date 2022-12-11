{%- macro null_negative_numbers(value) -%}

iff({{ value }}::number < 0, null, {{ value }}::number)

{%- endmacro -%}
