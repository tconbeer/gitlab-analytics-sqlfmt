with
    source as (select * from {{ source("greenhouse", "interviews") }}),
    renamed as (

        select

            -- keys
            id::number as interview_id,
            organization_id::number as organization_id,

            -- info
            name::varchar as interview_name,
            created_at::varchar::timestamp as interview_created_at,
            updated_at::varchar::timestamp as interview_updated_at

        from source

    )

select *
from renamed
