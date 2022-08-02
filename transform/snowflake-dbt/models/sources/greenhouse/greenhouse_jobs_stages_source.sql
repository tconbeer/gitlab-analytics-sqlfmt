with
    source as (select * from {{ source("greenhouse", "jobs_stages") }}),
    renamed as (

        select
            -- keys
            job_id::number as job_id,
            stage_id::number as job_stage_id,

            -- info
            "order"::number as job_stage_order,
            name::varchar as job_stage_name,
            stage_alert_setting::varchar as job_stage_alert_setting,
            created_at::timestamp as job_stage_created_at,
            updated_at::timestamp as job_stage_updated_at,
            milestones::varchar as job_stage_milestone

        from source

    )

select *
from renamed
