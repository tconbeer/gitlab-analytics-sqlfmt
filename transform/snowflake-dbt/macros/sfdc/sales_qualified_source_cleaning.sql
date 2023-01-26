{%- macro sales_qualified_source_cleaning(column_1) -%}

iff({{ column_1 }} = 'BDR Generated', 'SDR Generated', {{ column_1 }})

{%- endmacro -%}
