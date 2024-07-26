with events as (select * from {{ ref("snowplow_unnested_events") }})

select event_id as root_id, web_page_id as id
from events
