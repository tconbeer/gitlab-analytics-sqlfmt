with source as (select * from {{ source("fishtown_snowplow", "events_sample") }})

select *
from source
