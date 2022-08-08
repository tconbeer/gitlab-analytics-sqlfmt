with source as (select * from {{ ref("marketo_activity_add_to_opportunity_source") }})

select *
from source
