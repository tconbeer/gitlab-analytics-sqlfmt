{%- macro reason_for_arr_change_tier_change(
    product_ranking,
    previous_product_ranking,
    quantity,
    previous_quantity,
    arr,
    previous_arr
) -%}

case
    when {{ previous_product_ranking }} != {{ product_ranking }}
    then
        zeroifnull(
            {{ quantity }}
            * (
                {{ arr }}
                / nullif({{ quantity }}, 0)
                - {{ previous_arr }}
                / nullif({{ previous_quantity }}, 0)
            )
        )
    else 0
end as tier_change_arr

{%- endmacro -%}
