with source as (select * from {{ ref("marketo_activity_new_lead_source") }})

select *
from source
