WITH source AS (

    SELECT *
    FROM {{ source('zendesk_community_relations', 'ticket_metrics') }}

),

renamed AS (

    SELECT

      --ids
      id                                                  AS ticket_metrics_id,
      ticket_id,

      --fields
      agent_wait_time_in_minutes__business::FLOAT         AS agent_wait_time_in_minutes_business_hours,
      agent_wait_time_in_minutes__calendar::FLOAT         AS agent_wait_time_in_minutes_calendar_hours,
      first_resolution_time_in_minutes__business::FLOAT   AS first_resolution_time_in_minutes_during_business_hours,
      first_resolution_time_in_minutes__calendar::FLOAT   AS first_resolution_time_in_minutes_during_calendar_hours,
      full_resolution_time_in_minutes__business::FLOAT    AS full_resolution_time_in_minutes_during_business_hours,
      full_resolution_time_in_minutes__calendar::FLOAT    AS full_resolution_time_in_minutes_during_calendar_hours,
      on_hold_time_in_minutes__business::FLOAT            AS on_hold_time_in_minutes_during_business_hours,
      on_hold_time_in_minutes__calendar::FLOAT            AS on_hold_time_in_minutes_during_calendar_hours,
      reopens,
      replies                                             AS total_replies,
      reply_time_in_minutes__business::FLOAT              AS reply_time_in_minutes_during_business_hours,
      reply_time_in_minutes__calendar::FLOAT              AS reply_time_in_minutes_during_calendar_hours,
      requester_wait_time_in_minutes__business::FLOAT     AS requester_wait_time_in_minutes_during_business_hours,
      requester_wait_time_in_minutes__calendar::FLOAT     AS requester_wait_time_in_minutes_during_calendar_hours,
      assignee_stations                                   AS assignee_station_number,
      group_stations                                      AS group_station_number,

      --dates
      created_at,
      assigned_at,
      initially_assigned_at,
      latest_comment_added_at,
      solved_at,
      updated_at,
      assignee_updated_at

    FROM source

)

SELECT *
FROM renamed
