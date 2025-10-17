with
    source as (select * from {{ source("greenhouse", "scorecards") }}),
    renamed as (

        select

            -- keys
            id::number as scorecard_id,
            application_id::number as application_id,
            stage_id::number as stage_id,
            interview_id::number as interview_id,
            interviewer_id::number as interviewer_id,
            submitter_id::number as submitter_id,

            -- info
            overall_recommendation::varchar as scorecard_overall_recommendation,
            submitted_at::timestamp as scorecard_submitted_at,
            scheduled_interview_ended_at::timestamp
            as scorecard_scheduled_interview_ended_at,
            total_focus_attributes::number as scorecard_total_focus_attributes,
            completed_focus_attributes::number as scorecard_completed_focus_attributes,
            stage_name::varchar as scorecard_stage_name,
            created_at::timestamp as scorecard_created_at,
            updated_at::timestamp as scorecard_updated_at,
            interview_name::varchar as interview_name,
            interviewer::varchar as interviewer,
            submitter::varchar as scorecard_submitter

        from source

    )

select *
from renamed
