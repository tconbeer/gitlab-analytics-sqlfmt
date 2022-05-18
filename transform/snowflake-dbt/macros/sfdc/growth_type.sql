{%- macro growth_type(order_type, arr_basis) -%}

case
    when {{ order_type }} != '1. New - First Order' and {{ arr_basis }} = 0
    then 'Add-On Growth'
    when
        {{ order_type }} not in (
            '1. New - First Order',
            '4. Contraction',
            '5. Churn - Partial',
            '6. Churn - Final'
        ) and {{ arr_basis }} != 0
    then 'Growth on Renewal'
    when
        {{ order_type }} in (
            '4. Contraction', '5. Churn - Partial'
        ) and {{ arr_basis }} != 0
    then 'Contraction on Renewal'
    when {{ order_type }} in ('6. Churn - Final') and {{ arr_basis }} != 0
    then 'Lost on Renewal'
    else 'Missing growth_type'
end

{%- endmacro -%}
