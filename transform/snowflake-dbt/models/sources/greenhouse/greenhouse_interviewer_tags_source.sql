with
    source as (select * from {{ source("greenhouse", "interviewer_tags") }}),
    renamed as (

        select

            -- key
            user_id::number as user_id,

            -- info
            tag::varchar as interviewer_tag,
            created_at::timestamp as interviewer_tag_created_at,
            updated_at::timestamp as interviewer_tag_upated_at

        from source

    )

select *
from renamed
