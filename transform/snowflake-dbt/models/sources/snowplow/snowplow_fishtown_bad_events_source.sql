with source as (select * from {{ source("fishtown_snowplow", "bad_events") }})

select *
from source
