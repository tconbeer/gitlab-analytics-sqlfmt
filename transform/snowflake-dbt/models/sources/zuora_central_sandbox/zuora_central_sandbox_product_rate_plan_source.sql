with
    source as (

        select * from {{ source("zuora_central_sandbox", "product_rate_plan") }}

    ),
    renamed as (

        select
            -- Primary Keys
            id::varchar as product_rate_plan_id,

            -- Info
            product_id::varchar as product_id,
            description::varchar as product_rate_plan_description,
            effective_end_date::timestamp_tz as effective_end_date,
            effective_start_date::timestamp_tz as effective_start_date,
            name::varchar as product_rate_plan_name,
            created_by_id::varchar as created_by_id,
            created_date::timestamp_tz as created_date,
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date,
            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed
