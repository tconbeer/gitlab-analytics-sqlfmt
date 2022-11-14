with
    source as (

        select *
        from {{ source("snapshots", "zuora_product_rate_plan_charge_snapshots") }}

    ),
    renamed as (

        select
            id as product_rate_plan_charge_id,
            productrateplanid as product_rate_plan_id,
            name as product_rate_plan_charge_name,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed
