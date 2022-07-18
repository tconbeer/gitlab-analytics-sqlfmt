with source as (select * from {{ ref("marketo_activity_sync_lead_to_sfdc_source") }})

select *
from source
