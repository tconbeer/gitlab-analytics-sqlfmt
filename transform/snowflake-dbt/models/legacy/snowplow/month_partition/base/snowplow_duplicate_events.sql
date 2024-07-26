{{ config({"materialized": "view", "unique_key": "event_id"}) }}

with source as (select * from {{ ref("snowplow_duplicate_events_source") }})

select *
from source
