{{ config({"unique_key": "event_id"}) }}

with
    gitlab as (select * from {{ ref("snowplow_gitlab_events") }}),
    events_to_ignore as (select event_id from {{ ref("snowplow_duplicate_events") }})

select *
from gitlab
where event_id not in (select event_id from events_to_ignore)
