with
    source as (select * from {{ source("greenhouse", "users") }}),
    renamed as (

        select
            -- keys
            id::number as user_id,
            organization_id::number as organization_id,
            employee_id::varchar as employee_id,

            -- info
            status::varchar as user_status,
            created_at::timestamp as user_created_at,
            updated_at::timestamp as user_updated_at

        from source

    )

select *
from renamed
