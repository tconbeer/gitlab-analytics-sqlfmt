with
    source as (

        select *
        from {{ source("snapshots", "zuora_product_rate_plan_charge_tier_snapshots") }}

    ),
    renamed as (

        select
            productrateplanchargeid as product_rate_plan_charge_id,
            currency as currency,
            price as price,


            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed
