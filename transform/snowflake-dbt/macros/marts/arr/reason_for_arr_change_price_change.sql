{%- macro reason_for_arr_change_price_change(
    product_category,
    previous_product_category,
    quantity,
    previous_quantity,
    arr,
    previous_arr,
    product_ranking,
    previous_product_ranking
) -%}

case
    when {{ previous_product_category }} = {{ product_category }}
    then
        {{ quantity }} * (
            {{ arr }}/ nullif(
                {{ quantity }}, 0
            )
            - {{ previous_arr }}/ nullif(
                {{ previous_quantity }}, 0
            )
        )
    when
        {{ previous_product_category }} != {{ product_category }}
        and {{ previous_product_ranking }} = {{ product_ranking }}
    then
        {{ quantity }} * (
            {{ arr }}/ nullif(
                {{ quantity }}, 0
            )
            - {{ previous_arr }}/ nullif(
                {{ previous_quantity }}, 0
            )
        )
    else 0
end as price_change_arr

{%- endmacro -%}
