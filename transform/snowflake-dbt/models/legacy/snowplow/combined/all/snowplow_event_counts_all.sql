with
    good_events as (select * from {{ ref("snowplow_unnested_events_all") }}),

    bad_events as (select * from {{ ref("snowplow_unnested_errors_all") }}),

    good_count as (

        select
            date_trunc('day', derived_tstamp::timestamp)::date as event_day,
            count(*) as good_event_count
        from good_events
        group by 1

    ),

    bad_count as (

        select
            date_trunc('day', failure_timestamp::timestamp)::date as event_day,
            count(*) as bad_event_count
        from bad_events
        group by 1

    ),

    bad_unstruct_count as (

        select
            date_trunc('day', derived_tstamp::timestamp)::date as event_day,
            count(*) as bad_unstruct_event_count
        from good_events
        where is_bad_unstruct_event = true
        group by 1

    )

select
    good_count.event_day,
    good_count.good_event_count,
    bad_count.bad_event_count,
    bad_unstruct_count.bad_unstruct_event_count
from good_count
left join bad_count on good_count.event_day = bad_count.event_day
left join bad_unstruct_count on good_count.event_day = bad_unstruct_count.event_day
order by event_day
