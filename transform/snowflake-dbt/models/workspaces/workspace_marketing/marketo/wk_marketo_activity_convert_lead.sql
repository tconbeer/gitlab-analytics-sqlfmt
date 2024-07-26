with source as (select * from {{ ref("marketo_activity_convert_lead_source") }})

select *
from source
