with
    source as (select * from {{ source("greenhouse", "user_actions") }}),
    renamed as (

        select

            -- keys
            id::number as user_action_id,
            job_id::number as job_id,
            user_id::number as user_id,

            -- info
            type::varchar as user_action_type


        from source

    )

select *
from renamed
