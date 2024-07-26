with
    source as (select * from {{ source("zuora_api_sandbox", "product_rate_plan") }}),
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
            deleted as is_deleted

        from source

    )

select *
from renamed
