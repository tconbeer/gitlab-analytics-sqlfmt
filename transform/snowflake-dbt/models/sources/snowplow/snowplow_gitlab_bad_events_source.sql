with source as (select * from {{ source("gitlab_snowplow", "bad_events") }})

select *
from source
