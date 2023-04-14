{%- macro plan_change(
    original_product_ranking, original_mrr, new_product_ranking, new_mrr
) -%}

    case
        when {{ new_mrr }} = 0 and {{ original_mrr }} > 0
        then 'Cancelled'
        when {{ new_product_ranking }} = 0 or {{ original_product_ranking }} = 0
        then 'Not Valid'
        when {{ original_product_ranking }} = {{ new_product_ranking }}
        then 'Maintained'
        when {{ original_product_ranking }} > {{ new_product_ranking }}
        then 'Downgraded'
        when {{ original_product_ranking }} < {{ new_product_ranking }}
        then 'Upgraded'
    end as plan_change

{%- endmacro -%}
