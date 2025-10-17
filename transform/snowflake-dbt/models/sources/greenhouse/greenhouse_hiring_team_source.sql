with
    source as (select * from {{ source("greenhouse", "hiring_team") }}),
    renamed as (

        select

            -- keys
            job_id::number as job_id,
            user_id::number as user_id,

            -- info
            role::varchar as hiring_team_role,
            responsible::boolean as is_responsible,
            created_at::timestamp as hiring_team_created_at,
            updated_at::timestamp as hiring_team_updated_at

        from source

    )

select *
from renamed
