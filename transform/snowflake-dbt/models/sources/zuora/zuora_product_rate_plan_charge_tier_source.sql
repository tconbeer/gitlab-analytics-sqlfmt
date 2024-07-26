with
    source as (select * from {{ source("zuora", "product_rate_plan_charge_tier") }}),
    renamed as (

        select
            productrateplanchargeid as product_rate_plan_charge_id,
            currency as currency,
            price as price
        from source

    )

select *
from renamed
