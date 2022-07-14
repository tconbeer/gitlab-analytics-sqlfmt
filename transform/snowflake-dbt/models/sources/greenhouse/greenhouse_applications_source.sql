with
    source as (select * from {{ source("greenhouse", "applications") }}),
    stages_source as (select * from {{ source("greenhouse", "application_stages") }}),
    stages as (

        select *
        from stages_source
        where entered_on is not null
        qualify
            row_number() OVER (partition by application_id order by entered_on desc) = 1

    ),
    renamed as (

        select
            id as application_id,

            -- keys
            candidate_id,
            stages.stage_id,
            source_id,
            referrer_id,
            rejected_by_id,
            job_post_id,
            event_id,
            rejection_reason_id,
            converted_prospect_application_id,

            -- info
            status as application_status,
            prospect,

            pipeline_percent,
            migrated,
            rejected_by,
            stages.stage_name,
            prospect_pool,
            prospect_pool_stage,

            applied_at::timestamp as applied_at,
            rejected_at::timestamp as rejected_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as last_updated_at
        from source
        left join stages on stages.application_id = source.id
    )

select *
from renamed
