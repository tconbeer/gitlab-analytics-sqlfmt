with
    source as (

        select * from {{ source("zuora_api_sandbox", "rate_plan_charge_tier") }}

    ),
    renamed as (

        select
            rateplanchargeid as rate_plan_charge_id,
            productrateplanchargeid as product_rate_plan_charge_id,
            price,
            currency
        from source

    )

select *
from renamed
