with source as (select * from {{ ref("twitter_impressions_source") }})

select *
from source
