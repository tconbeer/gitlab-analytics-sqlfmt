with
    source as (

        select * from {{ ref("zendesk_community_relations_ticket_metrics_source") }}

    ),

    renamed as (

        select
            *,
            reply_time_in_minutes_during_calendar_hours
            as sla_reply_time_calendar_hours,
            reply_time_in_minutes_during_business_hours as sla_reply_time_business_hours

        from source

    )

select *
from renamed
