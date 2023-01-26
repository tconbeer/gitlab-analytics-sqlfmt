{%- macro case_when_boolean_int(value) -%}

case when {{ value }} > 0 then 1 end

{%- endmacro -%}
