with source as (select * from {{ ref("marketo_activity_update_opportunity_source") }})

select *
from source
