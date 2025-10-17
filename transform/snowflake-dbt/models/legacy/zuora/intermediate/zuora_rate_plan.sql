with
    source as (select * from {{ ref("zuora_rate_plan_source") }}),
    with_product_category as (

        select
            *,
            {{ product_category("rate_plan_name") }},
            {{ delivery("product_category") }}
        from source

    )

select *
from with_product_category
