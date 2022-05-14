{{
    config(
        {
            "materialized": "table",
            "unique_key": "event_id",
        }
    )
}}

with
    fishtown as (

        select nullif(jsontext['event_id']::varchar, '') as event_id
        from {{ ref("snowplow_fishtown_good_events_source") }}

    ),
    gitlab as (select event_id from {{ ref("snowplow_gitlab_good_events_source") }}),
    unioned as (select event_id from fishtown union all select event_id from gitlab),
    counts as (

        select event_id, count(event_id) as event_count
        from unioned
        group by 1
        having event_count > 1

    )

select *
from counts
