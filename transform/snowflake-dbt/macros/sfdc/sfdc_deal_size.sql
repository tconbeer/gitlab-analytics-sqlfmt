{%- macro sfdc_deal_size(iacv_value, column_name) -%}

case
    when {{ iacv_value }} < 5000
    then '1 - Small (<5k)'
    when {{ iacv_value }} < 25000
    then '2 - Medium (5k - 25k)'
    when {{ iacv_value }} < 100000
    then '3 - Big (25k - 100k)'
    when {{ iacv_value }} >= 100000
    then '4 - Jumbo (>100k)'
    else '5 - Unknown'
end as {{ column_name }}

{%- endmacro -%}
