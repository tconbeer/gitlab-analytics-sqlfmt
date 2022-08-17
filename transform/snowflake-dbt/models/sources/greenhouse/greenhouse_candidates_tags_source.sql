with
    source as (select * from {{ source("greenhouse", "candidates_tags") }}),
    renamed as (

        select
            -- keys
            id::number as candidate_tag_id,
            tag_id::number as tag_id,
            candidate_id::number as candidate_id,

            -- info
            created_at::timestamp as candidate_tag_created_at,
            updated_at::timestamp as candidate_tag_updated_at


        from source

    )

select *
from renamed
