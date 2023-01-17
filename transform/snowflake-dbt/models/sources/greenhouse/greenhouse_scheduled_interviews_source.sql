with
    source as (select * from {{ source("greenhouse", "scheduled_interviews") }}),
    renamed as (

        select

            -- keys
            id::number as scheduled_interview_id,
            application_id::number as application_id,
            interview_id::number as interview_id,
            scheduled_by_id::number as interview_scheduled_by_id,

            -- info
            status::varchar as scheduled_interview_status,
            scheduled_at::timestamp as interview_scheduled_at,
            starts_at::timestamp as interview_starts_at,
            ends_at::timestamp as interview_ends_at,
            all_day_start_date::varchar::date as all_day_start_date,
            all_day_end_date::varchar::date as all_day_end_date,
            stage_name::varchar as scheduled_interview_stage_name,
            interview_name::varchar as scheduled_interview_name

        from source

    )

select *
from renamed
