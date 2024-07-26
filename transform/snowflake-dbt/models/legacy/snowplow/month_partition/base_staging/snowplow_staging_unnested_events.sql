{{ config({"unique_key": "event_id"}) }}

with gitlab as (select * from {{ ref("snowplow_gitlab_staging_events") }})

select *
from gitlab
