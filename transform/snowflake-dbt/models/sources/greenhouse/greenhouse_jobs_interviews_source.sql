with
    source as (select * from {{ source("greenhouse", "jobs_interviews") }}),
    renamed as (

        select

            -- keys
            id::number as job_interview_id,
            job_id::number as job_id,
            stage_id::number as interview_stage_id,
            interview_id::number as interview_id,

            -- info
            "order"::number as interview_order,
            estimated_duration::number as estimated_duration,
            created_at::timestamp as interview_created_at,
            updated_at::timestamp as interview_updated_at

        from source

    )

select *
from renamed
