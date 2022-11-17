{{ config({"schema": "legacy"}) }}

with
    source as (select * from {{ ref("zendesk_ticket_metrics_source") }}),

    renamed as (

        select
            *,
            /* The following is a stopgap solution to set the minimum value between first resolution,
           full resolution and reply time for calendar and business hours respectively.
           In Snowflake, LEAST will always return NULL if any input to the function is NULL.
           In the event all three metrics are NULL, NULL should be returned */
            iff(
                first_resolution_time_in_minutes_during_calendar_hours is null
                and full_resolution_time_in_minutes_during_calendar_hours is null
                and reply_time_in_minutes_during_calendar_hours is null,
                null,
                least(
                    coalesce(
                        first_resolution_time_in_minutes_during_calendar_hours, 50000000
                    ),
                    -- 50,000,000 is roughly 100 years, sufficient to not be the LEAST
                    -- value
                    coalesce(
                        full_resolution_time_in_minutes_during_calendar_hours, 50000000
                    ),
                    coalesce(reply_time_in_minutes_during_calendar_hours, 50000000)
                )
            ) as sla_reply_time_calendar_hours,
            iff(
                first_resolution_time_in_minutes_during_business_hours is null
                and full_resolution_time_in_minutes_during_business_hours is null
                and reply_time_in_minutes_during_business_hours is null,
                null,
                least(
                    coalesce(
                        first_resolution_time_in_minutes_during_business_hours, 50000000
                    ),
                    coalesce(
                        full_resolution_time_in_minutes_during_business_hours, 50000000
                    ),
                    coalesce(reply_time_in_minutes_during_business_hours, 50000000)
                )
            ) as sla_reply_time_business_hours


        from source

    )

select *
from renamed
