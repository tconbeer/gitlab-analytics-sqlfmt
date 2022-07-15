with source as (select * from {{ ref("marketo_activity_click_link_source") }})

select *
from source
