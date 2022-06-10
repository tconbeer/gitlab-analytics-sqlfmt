with
    source as (select * from {{ source("greenhouse", "application_stages") }}),
    stage_dim as (select * from {{ ref("greenhouse_stages_source") }}),
    renamed as (

        select
            -- keys
            application_id::number as application_id,
            stage_id::number as stage_id,

            -- info
            entered_on::timestamp as stage_entered_on,
            exited_on::timestamp as stage_exited_on,
            stage_name::varchar as application_stage_name

        from source

    ),
    intermediate as (

        select
            renamed.*,
            is_milestone_stage,
            stage_name_modified,
            iff(
                stage_name_modified = 'Team Interview - Face to Face',
                'team_interview',
                lower(replace (stage_name_modified, ' ', '_'))
            ) as stage_name_modified_with_underscores
        from renamed
        left join stage_dim on renamed.stage_id = stage_dim.stage_id

    )

select *
from intermediate
