with source as (select * from {{ ref("marketo_activity_execute_campaign_source") }})

select *
from source
