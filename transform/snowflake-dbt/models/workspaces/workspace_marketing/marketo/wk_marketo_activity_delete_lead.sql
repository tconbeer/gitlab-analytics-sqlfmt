with source as (select * from {{ ref("marketo_activity_delete_lead_source") }})

select *
from source
