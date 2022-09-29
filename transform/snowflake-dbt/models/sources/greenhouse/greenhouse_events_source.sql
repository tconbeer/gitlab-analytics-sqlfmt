with
    source as (select * from {{ source("greenhouse", "events") }}),
    renamed as (

        -- key
        -- info
        select id::number as greenhouse_event_id, name::varchar as greenhouse_event_name

        from source

    )

select *
from renamed
