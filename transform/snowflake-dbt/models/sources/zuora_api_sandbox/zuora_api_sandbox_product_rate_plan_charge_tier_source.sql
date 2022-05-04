with
    source as (

        select * from {{ source("zuora_api_sandbox", "product_rate_plan_charge_tier") }}

    ),
    renamed as (

        select
            productrateplanchargeid as product_rate_plan_charge_id,
            currency as currency,
            price as price
        from source

    )

select *
from renamed
