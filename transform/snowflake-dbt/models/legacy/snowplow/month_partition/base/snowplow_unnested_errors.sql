{{ config({"unique_key": "bad_event_surrogate"}) }}

with gitlab as (select * from {{ ref("snowplow_gitlab_bad_events") }})

select *
from gitlab
