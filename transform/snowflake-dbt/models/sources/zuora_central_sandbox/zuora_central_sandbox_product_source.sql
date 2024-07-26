with
    source as (select * from {{ source("zuora_central_sandbox", "product") }}),
    renamed as (

        select
            -- Primary Keys
            id::varchar as product_id,

            -- Info
            name::varchar as product_name,
            sku::varchar as sku,
            description::varchar as product_description,
            category::varchar as category,
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date,
            _fivetran_deleted as is_deleted,
            effective_start_date as effective_start_date,
            effective_end_date as effective_end_date

        from source

    )

select *
from renamed
