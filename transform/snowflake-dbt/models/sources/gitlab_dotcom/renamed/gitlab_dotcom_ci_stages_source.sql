with
    source as (

        select *
        from {{ ref("gitlab_dotcom_ci_stages_dedupe_source") }}
        where created_at is not null

    ),
    renamed as (

        select
            id::number as ci_stage_id,
            project_id::number as project_id,
            pipeline_id::number as pipeline_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            name::varchar as ci_stage_name,
            status::number as ci_stage_status,
            lock_version::number as lock_version,
            position::number as position
        from source

    )

select *
from renamed
order by updated_at
