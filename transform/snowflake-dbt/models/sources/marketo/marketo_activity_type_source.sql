with
    source as (select * from {{ source("marketo", "activity_type") }}),
    renamed as (

        select

            id::number as marketo_activity_type_id,
            name::text as name,
            description::text as description

        from source

    )

select *
from renamed
