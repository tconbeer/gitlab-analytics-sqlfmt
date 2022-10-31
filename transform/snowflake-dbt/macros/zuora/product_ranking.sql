{%- macro product_ranking(product_category) -%}

max(  -- Need to account for the 'other' categories
    decode(
        {{ product_category }},
        'Bronze',
        1,
        'Silver',
        2,
        'Gold',
        3,

        'Starter',
        1,
        'Premium',
        2,
        'Ultimate',
        3,
        0
    )
)

{%- endmacro -%}
