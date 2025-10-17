with
    source as (

        select * from {{ source("zuora_central_sandbox", "rate_plan_charge_tier") }}

    ),
    renamed as (

        select
            rate_plan_charge_id as rate_plan_charge_id,
            product_rate_plan_charge_id as product_rate_plan_charge_id,
            price,
            currency
        from source

    )

select *
from renamed
