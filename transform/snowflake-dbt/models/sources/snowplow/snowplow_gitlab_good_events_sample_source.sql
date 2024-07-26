with source as (select * from {{ source("gitlab_snowplow", "events_sample") }})

select *
from source
