with source as (select * from {{ ref("marketo_activity_change_owner_source") }})

select *
from source
