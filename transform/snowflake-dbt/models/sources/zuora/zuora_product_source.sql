with
    source as (select * from {{ source("zuora", "product") }}),
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
            effectivestartdate as effective_start_date,
            effectiveenddate as effective_end_date

        from source

    )

select *
from renamed
