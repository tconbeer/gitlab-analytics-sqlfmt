with source as (select * from {{ ref("marketo_activity_request_campaign_source") }})

select *
from source
