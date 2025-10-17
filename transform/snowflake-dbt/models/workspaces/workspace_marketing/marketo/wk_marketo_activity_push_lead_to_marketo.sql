with source as (select * from {{ ref("marketo_activity_push_lead_to_marketo_source") }})

select *
from source
