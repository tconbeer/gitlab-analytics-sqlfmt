with
    source as (

        select *
        from {{ source("zuora_central_sandbox", "product_rate_plan_charge_tier") }}

    ),
    renamed as (

        select
            product_rate_plan_charge_id as product_rate_plan_charge_id,
            currency as currency,
            price as price
        from source

    )

select *
from renamed
