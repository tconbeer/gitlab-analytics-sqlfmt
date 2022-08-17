with
    source as (select * from {{ source("greenhouse", "offices") }}),
    renamed as (

        select

            -- keys
            id::number as office_id,
            organization_id::number as organization_id,
            parent_id::number as office_parent_id,

            -- info
            name::varchar as office_name,
            created_at::timestamp as office_created_at,
            updated_at::timestamp as office_updated_at

        from source

    )

select *
from renamed
