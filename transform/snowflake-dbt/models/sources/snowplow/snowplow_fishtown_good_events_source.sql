with source as (select * from {{ source("fishtown_snowplow", "events") }})

select *
from source
