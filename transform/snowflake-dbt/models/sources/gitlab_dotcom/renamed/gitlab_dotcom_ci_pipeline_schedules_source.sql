
with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_pipeline_schedules_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as ci_pipeline_schedule_id,
            description as ci_pipeline_schedule_description,
            ref as ref,
            cron as cron,
            cron_timezone as cron_timezone,
            next_run_at::timestamp as next_run_at,
            project_id::number as project_id,
            owner_id::number as owner_id,
            active as active,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
