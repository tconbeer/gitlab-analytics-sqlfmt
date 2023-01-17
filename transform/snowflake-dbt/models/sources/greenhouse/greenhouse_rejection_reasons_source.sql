with
    source as (select * from {{ source("greenhouse", "rejection_reasons") }}),
    renamed as (

        select
            -- keys
            id::number as rejection_reason_id,
            organization_id::number as organization_id,

            -- info
            name::varchar as rejection_reason_name,
            type::varchar as rejection_reason_type,
            created_at::timestamp as rejection_reason_created_at,
            updated_at::timestamp as rejection_reason_updated_at

        from source

    )

select *
from renamed
