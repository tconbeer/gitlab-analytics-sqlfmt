with
    source as (select * from {{ source("greenhouse", "events") }}),
    renamed as (

        select

            -- key
            id::number as greenhouse_event_id,

            -- info
            name::varchar as greenhouse_event_name

        from source

    )

select *
from renamed
