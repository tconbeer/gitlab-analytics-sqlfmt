with
    source as (select * from {{ source("greenhouse", "job_posts") }}),
    renamed as (

        select

            -- keys
            id::number as job_post_id,
            job_id::number as job_id,

            -- info
            title::varchar as job_post_title,
            live::boolean as is_job_live,
            job_board_name::varchar as job_board_name,
            language::varchar as job_post_language,
            location::varchar as job_post_location,
            created_at::timestamp as job_post_created_at,
            updated_at::timestamp as job_post_updated_at

        from source

    )

select *
from renamed
