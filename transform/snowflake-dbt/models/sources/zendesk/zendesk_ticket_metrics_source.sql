with
    source as (select * from {{ source("zendesk", "ticket_metrics") }}),

    renamed as (

        select

            -- ids
            id as ticket_metrics_id,
            ticket_id,

            -- fields
            agent_wait_time_in_minutes__business::float
            as agent_wait_time_in_minutes_business_hours,
            agent_wait_time_in_minutes__calendar::float
            as agent_wait_time_in_minutes_calendar_hours,
            first_resolution_time_in_minutes__business::float
            as first_resolution_time_in_minutes_during_business_hours,
            first_resolution_time_in_minutes__calendar::float
            as first_resolution_time_in_minutes_during_calendar_hours,
            full_resolution_time_in_minutes__business::float
            as full_resolution_time_in_minutes_during_business_hours,
            full_resolution_time_in_minutes__calendar::float
            as full_resolution_time_in_minutes_during_calendar_hours,
            on_hold_time_in_minutes__business::float
            as on_hold_time_in_minutes_during_business_hours,
            on_hold_time_in_minutes__calendar::float
            as on_hold_time_in_minutes_during_calendar_hours,
            reopens,
            replies as total_replies,
            reply_time_in_minutes__business::float
            as reply_time_in_minutes_during_business_hours,
            reply_time_in_minutes__calendar::float
            as reply_time_in_minutes_during_calendar_hours,
            requester_wait_time_in_minutes__business::float
            as requester_wait_time_in_minutes_during_business_hours,
            requester_wait_time_in_minutes__calendar::float
            as requester_wait_time_in_minutes_during_calendar_hours,
            assignee_stations as assignee_station_number,
            group_stations as group_station_number,

            -- dates
            created_at,
            assigned_at,
            initially_assigned_at,
            latest_comment_added_at,
            solved_at,
            updated_at,
            assignee_updated_at

        from source

    )

select *
from renamed
