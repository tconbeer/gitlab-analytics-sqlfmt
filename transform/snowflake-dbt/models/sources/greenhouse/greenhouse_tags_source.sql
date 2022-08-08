with
    source as (select * from {{ source("greenhouse", "tags") }}),
    renamed as (

        select

            -- keys
            id::number as tag_id,
            organization_id::number as organization_id,

            -- info
            name::varchar as tag_name,
            created_at::timestamp as tag_created_at,
            updated_at::timestamp as tag_updated_at

        from source

    )

select *
from renamed
