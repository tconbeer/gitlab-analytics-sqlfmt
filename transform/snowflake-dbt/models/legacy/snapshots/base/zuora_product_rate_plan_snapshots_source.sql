with
    source as (

        select * from {{ source("snapshots", "zuora_product_rate_plan_snapshots") }}

    ),
    renamed as (

        select
            -- Primary Keys
            id::varchar as product_rate_plan_id,

            -- Info
            productid::varchar as product_id,
            description::varchar as product_rate_plan_description,
            effectiveenddate::timestamp_tz as effective_end_date,
            effectivestartdate::timestamp_tz as effective_start_date,
            name::varchar as product_rate_plan_name,
            createdbyid::varchar as created_by_id,
            createddate::timestamp_tz as created_date,
            updatedbyid::varchar as updated_by_id,
            updateddate::timestamp_tz as updated_date,
            deleted as is_deleted,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed
