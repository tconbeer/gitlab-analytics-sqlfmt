with source as (select * from {{ ref("marketo_activity_add_to_sfdc_campaign_source") }})

select *
from source
