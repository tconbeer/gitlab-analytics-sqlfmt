with source as (select * from {{ ref("marketo_activity_add_to_list_source") }})

select *
from source
