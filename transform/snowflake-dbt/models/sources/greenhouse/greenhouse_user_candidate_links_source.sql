with
    source as (select * from {{ source("greenhouse", "user_candidate_links") }}),
    renamed as (

        select
            -- keys
            user_id::number as user_id,
            candidate_id::number as candidate_id,

            -- info
            created_at::timestamp as user_candidate_link_created_at,
            updated_at::timestamp as user_candidate_link_updated_at

        from source

    )

select *
from renamed
