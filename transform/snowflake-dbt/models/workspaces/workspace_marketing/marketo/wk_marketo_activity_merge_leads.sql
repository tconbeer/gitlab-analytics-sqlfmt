with source as (select * from {{ ref("marketo_activity_merge_leads_source") }})

select *
from source
