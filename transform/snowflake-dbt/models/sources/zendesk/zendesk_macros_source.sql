with
    source as (select * from {{ source("zendesk", "macros") }}),

    renamed as (

        select

            -- ids
            id as macro_id,

            -- field
            active as is_active,
            url as macro_url,
            description as macro_description,
            position as macro_position,
            title as macro_title,

            -- dates
            created_at,
            updated_at

        from source

    )

select *
from renamed
