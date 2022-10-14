with
    source as (select * from {{ source("snapshots", "zuora_product_snapshots") }}),
    renamed as (

        select
            -- Primary Keys
            id::varchar as product_id,

            -- Info
            name::varchar as product_name,
            sku::varchar as sku,
            description::varchar as product_description,
            category::varchar as category,
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
